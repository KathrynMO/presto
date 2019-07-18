/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.aresdb;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.AresDbOutputInfo;
import com.facebook.presto.aresdb.query.AugmentedAQL;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNEXPECTED_ERROR;
import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_OUTPUT_TYPE;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;

public class AresDbPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(AresDbPageSource.class);

    private final AresDbSplit aresDbSplit;
    private final List<AresDbColumnHandle> columns;
    private final AresDbConnection aresDbConnection;
    private final ConnectorSession session;
    private final Cache<AugmentedAQL, Page> cache;
    private final List<Page> cachedPages;
    private final List<Future<AqlResponse>> aqlResponses;

    // state information
    private int pagesConsumed;
    private long readTimeNanos;
    private long completedBytes;

    private static class AqlResponse
    {
        private final AugmentedAQL request;
        private final String response;
        private final boolean cacheable;

        public AqlResponse(AugmentedAQL request, String response, boolean cacheable)
        {
            this.request = request;
            this.response = response;
            this.cacheable = cacheable;
        }
    }

    public AresDbPageSource(AresDbSplit aresDbSplit, List<AresDbColumnHandle> columns, AresDbConnection aresDbConnection, ConnectorSession session, Cache<AugmentedAQL, Page> cache)
    {
        this.aresDbSplit = aresDbSplit;
        this.columns = columns;
        this.aresDbConnection = aresDbConnection;
        this.session = session;
        this.cache = cache;
        ImmutableList.Builder<Page> cachedPages = ImmutableList.builder();
        ImmutableList.Builder<Future<AqlResponse>> aqlResponses = ImmutableList.builder();
        fetch(cachedPages, aqlResponses);
        this.cachedPages = cachedPages.build();
        this.aqlResponses = aqlResponses.build();
        session.getSessionLogger().log(() -> String.format("Ares start split", aresDbSplit.getIndex()));
    }

    private Page getPageFromAqlResponse(AqlResponse response)
    {
        Page page = buildPage(response.request.getAql(), response.response);
        if (response.cacheable) {
            cache.put(response.request, page);
        }
        return page;
    }

    private void fetch(ImmutableList.Builder<Page> cachedPagesBuilder, ImmutableList.Builder<Future<AqlResponse>> aqlResponses)
    {
        for (int i = 0; i < aresDbSplit.getAqls().size(); ++i) {
            AresDbSplit.AresQL aresQL = aresDbSplit.getAqls().get(i);
            String aql = aresQL.getAql();
            boolean miss = true;
            AugmentedAQL augmentedAQL = new AugmentedAQL(aql, aresDbSplit.getExpressions());
            if (aresQL.isCacheable()) {
                Page cachedPage = cache.getIfPresent(augmentedAQL);
                if (cachedPage != null) {
                    cachedPagesBuilder.add(cachedPage);
                    miss = false;
                }
            }
            if (miss) {
                aqlResponses.add(Futures.transform(aresDbConnection.queryAndGetResultsAsync(aql, i, aresDbSplit.getIndex(), session), response -> new AqlResponse(augmentedAQL, response, aresQL.isCacheable())));
            }
        }
    }

    private void setValue(Type type, BlockBuilder blockBuilder, Object value, AresDbOutputInfo outputInfo)
    {
        if (value == null || "NULL".equals(value)) {
            blockBuilder.appendNull();
            return;
        }

        if (type instanceof BigintType) {
            long parsedValue;
            if (value instanceof Number) {
                parsedValue = ((Number) value).longValue();
            }
            else if (value instanceof String) {
                parsedValue = Double.valueOf((String) value).longValue();
            }
            else {
                throw new AresDbException(ARESDB_UNSUPPORTED_OUTPUT_TYPE, "For type '" + type + "' received unsupported output type: " + value.getClass());
            }

            type.writeLong(blockBuilder, parsedValue);
            completedBytes += ((FixedWidthType) type).getFixedSize();
        }
        else if (type instanceof TimestampType) {
            // output is always seconds since timeUnit is seconds
            long parsedValue = Long.parseUnsignedLong((String) value, 10) * 1000;
            type.writeLong(blockBuilder, parsedValue);
            completedBytes += ((FixedWidthType) type).getFixedSize();
        }
        else if (type instanceof TimestampWithTimeZoneType) {
            // output is always seconds since timeUnit is seconds
            long parsedValue = Long.parseUnsignedLong((String) value, 10) * 1000;
            TimeZoneKey tzKey = outputInfo.timeBucketizer.orElseThrow(() -> new IllegalStateException("Expected to find a     time bucketizer when handling a TimeStampWithTimeZone")).getTimeZoneKey();
            type.writeLong(blockBuilder, packDateTimeWithZone(parsedValue, tzKey));
            completedBytes += ((FixedWidthType) type).getFixedSize();
        }
        else if (type instanceof IntegerType) {
            int parsedValue;
            if (value instanceof Number) {
                parsedValue = ((Number) value).intValue();
            }
            else if (value instanceof String) {
                parsedValue = Double.valueOf((String) value).intValue();
            }
            else {
                throw new AresDbException(ARESDB_UNSUPPORTED_OUTPUT_TYPE, "For type '" + type + "' received unsupported output type: " + value.getClass());
            }
            blockBuilder.writeInt(parsedValue);
            completedBytes += ((FixedWidthType) type).getFixedSize();
        }
        else if (type instanceof TinyintType) {
            byte parsedValue;
            if (value instanceof Number) {
                parsedValue = ((Number) value).byteValue();
            }
            else if (value instanceof String) {
                parsedValue = Double.valueOf((String) value).byteValue();
            }
            else {
                throw new AresDbException(ARESDB_UNSUPPORTED_OUTPUT_TYPE, "For type '" + type + "' received unsupported output type: " + value.getClass());
            }
            blockBuilder.writeByte(parsedValue);
            completedBytes += ((FixedWidthType) type).getFixedSize();
        }
        else if (type instanceof SmallintType) {
            short parsedValue;
            if (value instanceof Number) {
                parsedValue = ((Number) value).shortValue();
            }
            else if (value instanceof String) {
                parsedValue = Double.valueOf((String) value).shortValue();
            }
            else {
                throw new AresDbException(ARESDB_UNSUPPORTED_OUTPUT_TYPE, "For type '" + type + "' received unsupported output type: " + value.getClass());
            }
            blockBuilder.writeShort(parsedValue);
            completedBytes += ((FixedWidthType) type).getFixedSize();
        }
        else if (type instanceof BooleanType) {
            if (value instanceof String) {
                type.writeBoolean(blockBuilder, Boolean.valueOf((String) value));
                completedBytes += ((FixedWidthType) type).getFixedSize();
            }
            else {
                throw new AresDbException(ARESDB_UNSUPPORTED_OUTPUT_TYPE, "For type '" + type + "' received unsupported output type: " + value.getClass());
            }
        }
        else if (type instanceof DecimalType || type instanceof DoubleType) {
            double parsedValue;
            if (value instanceof Number) {
                parsedValue = ((Number) value).doubleValue();
            }
            else if (value instanceof String) {
                parsedValue = Double.valueOf((String) value);
            }
            else {
                throw new AresDbException(ARESDB_UNSUPPORTED_OUTPUT_TYPE, "For type '" + type + "' received unsupported output type: " + value.getClass());
            }

            type.writeDouble(blockBuilder, parsedValue);
            completedBytes += ((FixedWidthType) type).getFixedSize();
        }
        else if (type instanceof VarcharType) {
            if (value instanceof String) {
                Slice slice = Slices.utf8Slice((String) value);
                blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
                completedBytes += slice.length();
            }
            else {
                throw new AresDbException(ARESDB_UNSUPPORTED_OUTPUT_TYPE, "For type '" + type + "' received unsupported output type: " + value.getClass());
            }
        }
        else {
            throw new AresDbException(ARESDB_UNSUPPORTED_OUTPUT_TYPE, "type '" + type + "' not supported");
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return pagesConsumed >= aresDbSplit.getAqls().size();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }

        long start = System.nanoTime();
        session.getSessionLogger().log(() -> String.format("Ares getNextPage start %d of split %d", pagesConsumed, aresDbSplit.getIndex()));
        try {
            if (pagesConsumed < cachedPages.size()) {
                return cachedPages.get(pagesConsumed);
            }

            return getPageFromAqlResponse(aqlResponses.get(pagesConsumed - cachedPages.size()).get());
        }
        catch (InterruptedException e) {
            throw new AresDbException(ARESDB_UNEXPECTED_ERROR, "Interrupted", e);
        }
        catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause());
            throw new AresDbException(ARESDB_UNEXPECTED_ERROR, "Unexpected error", e.getCause());
        }
        finally {
            session.getSessionLogger().log(() -> String.format("Ares getNextPage end %d of split %d", pagesConsumed, aresDbSplit.getIndex()));
            readTimeNanos += System.nanoTime() - start;
            ++pagesConsumed;
        }
    }

    private int populatePage(String aql, String response, List<BlockBuilder> blockBuilders, List<Type> types, List<AresDbOutputInfo> outputInfos)
    {
        JSONObject responseJson = JSONObject.parseObject(response);
        if (Optional.ofNullable(responseJson.getJSONArray("errors")).map(x -> x.size()).orElse(0) > 0) {
            throw new AresDbException(ARESDB_UNEXPECTED_ERROR, "Error in response " + response, aql);
        }
        if (!responseJson.containsKey("results")) {
            return 0;
        }

        JSONArray resultsJson = responseJson.getJSONArray("results");
        if (resultsJson.isEmpty()) {
            return 0;
        }

        if (resultsJson.getJSONObject(0).containsKey("matrixData") || resultsJson.getJSONObject(0).containsKey("headers")) {
            JSONArray rows = resultsJson.getJSONObject(0).getJSONArray("matrixData");
            int numRows = rows == null ? 0 : rows.size();
            final int numCols = blockBuilders.size();
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                JSONArray row = rows.getJSONArray(rowIdx);
                for (int columnIdx = 0; columnIdx < numCols; columnIdx++) {
                    AresDbOutputInfo outputInfo = outputInfos.get(columnIdx);
                    int outputIdx = outputInfo.index;
                    setValue(types.get(outputIdx), blockBuilders.get(outputIdx), row.get(columnIdx), outputInfo);
                }
            }
            return numRows;
        }
        else {
            // parse group by results:
            // Example output:
            // {"1556668800":{"uber/production":5576974, "uber/staging":5576234}}, {"1556668800":{"uber/production":5576974, "uber/staging":5576234}} ->
            // {groupByKey1: {groupByKey2: measure, groupByKey2: measure}}, {groupByKey1: {groupByKey2: measure, groupByKey2: measure}}
            int rowIndex = 0;
            List<Object> currentRow = new ArrayList<>();
            for (int entryIdx = 0; entryIdx < resultsJson.size(); entryIdx++) {
                JSONObject groupByResult = resultsJson.getJSONObject(entryIdx);
                rowIndex = parserGroupByObject(groupByResult, currentRow, outputInfos, blockBuilders, types, 0, rowIndex);
            }
            return rowIndex;
        }
    }

    private Page buildPage(String aql, String response)
    {
        session.getSessionLogger().log(() -> "Building page from AQL");
        List<Type> expectedTypes = columns.stream().map(AresDbColumnHandle::getDataType).collect(Collectors.toList());
        PageBuilder pageBuilder = new PageBuilder(expectedTypes);
        List<AresDbOutputInfo> outputInfos = AresDbQueryGeneratorContext.getIndicesMappingFromAresDbSchemaToPrestoSchema(aresDbSplit.getExpressions(), columns);
        ImmutableList.Builder<BlockBuilder> columnBlockBuilders = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        for (AresDbOutputInfo outputInfo : outputInfos) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputInfo.index);
            columnBlockBuilders.add(blockBuilder);
            columnTypesBuilder.add(expectedTypes.get(outputInfo.index));
        }

        int rowCount = populatePage(aql, response, columnBlockBuilders.build(), columnTypesBuilder.build(), outputInfos);
        pageBuilder.declarePositions(rowCount);
        Page page = pageBuilder.build();
        session.getSessionLogger().log(() -> "End building page from AQL");
        return page;
    }

    private int parserGroupByObject(Object output, List<Object> valuesSoFar, List<AresDbOutputInfo> outputInfos, List<BlockBuilder> blockBuilders, List<Type> types, int startingColumnIndex, int currentRowNumber)
    {
        if (output instanceof JSONObject) {
            JSONObject groupByResult = (JSONObject) output;
            for (Map.Entry<String, Object> entry : groupByResult.entrySet()) {
                addColumnToCurrentRow(valuesSoFar, entry.getKey());
                currentRowNumber = parserGroupByObject(entry.getValue(), valuesSoFar, outputInfos, blockBuilders, types, startingColumnIndex + 1, currentRowNumber);
                removeLastColumnFromCurrentRow(valuesSoFar);
            }
        }
        else {
            addColumnToCurrentRow(valuesSoFar, output);
            // we have come to the measure, that means it is the end of the row
            for (int columnIdx = 0; columnIdx <= startingColumnIndex; columnIdx++) {
                AresDbOutputInfo outputInfo = outputInfos.get(columnIdx);
                int outputIdx = outputInfo.index;
                setValue(types.get(outputIdx), blockBuilders.get(outputIdx), valuesSoFar.get(columnIdx), outputInfo);
            }

            removeLastColumnFromCurrentRow(valuesSoFar);
            currentRowNumber += 1;
        }

        return currentRowNumber;
    }

    private void addColumnToCurrentRow(List<Object> valuesSoFar, Object value)
    {
        valuesSoFar.add(value);
    }

    private void removeLastColumnFromCurrentRow(List<Object> valuesSoFar)
    {
        valuesSoFar.remove(valuesSoFar.size() - 1);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        pagesConsumed = aresDbSplit.getAqls().size();
        session.getSessionLogger().log(() -> String.format("Ares end split", aresDbSplit.getIndex()));
    }
}
