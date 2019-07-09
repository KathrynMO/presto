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
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNEXPECTED_ERROR;
import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_OUTPUT_TYPE;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static java.util.Objects.requireNonNull;

public class AresDbPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(AresDbPageSource.class);

    private final AresDbSplit aresDbSplit;
    private final List<AresDbColumnHandle> columns;
    private final AresDbConnection aresDbConnection;
    private final ConnectorSession session;
    private final Cache<AugmentedAQL, Page> cache;
    private final LinkedBlockingQueue<PageOrError> pages;
    private AtomicBoolean fetched = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    // state information
    private int pagesConsumed;
    private long readTimeNanos;
    private long completedBytes;

    public AresDbPageSource(AresDbSplit aresDbSplit, List<AresDbColumnHandle> columns, AresDbConnection aresDbConnection, ConnectorSession session, Cache<AugmentedAQL, Page> cache)
    {
        this.aresDbSplit = aresDbSplit;
        this.columns = columns;
        this.aresDbConnection = aresDbConnection;
        this.session = session;
        this.cache = cache;
        this.pages = new LinkedBlockingQueue<>(this.aresDbSplit.getAqls().size());
    }

    private static class PageOrError
    {
        private final Page page;
        private final String aql;
        private final Throwable error;

        public PageOrError(Page page, String aql, Throwable error)
        {
            this.page = page;
            this.aql = requireNonNull(aql);
            this.error = error;
            Preconditions.checkState(page != null ^ error != null);
        }
    }

    public void fetchIfNeeded()
    {
        if (fetched.compareAndSet(false, true)) {
            for (int i = 0; i < aresDbSplit.getAqls().size(); ++i) {
                AresDbSplit.AresQL aresQL = aresDbSplit.getAqls().get(i);
                String aql = aresQL.getAql();
                boolean miss = true;
                AugmentedAQL augmentedAQL = new AugmentedAQL(aql, aresDbSplit.getExpressions());
                if (aresQL.isCacheable()) {
                    Page cachedPage = this.cache.getIfPresent(augmentedAQL);
                    if (cachedPage != null) {
                        log.debug("Got hit for %s", i);
                        this.pages.offer(new PageOrError(cachedPage, aql, null));
                        miss = false;
                    }
                }
                if (miss) {
                    log.debug("Fetching %s %s", aql, i);
                    Futures.addCallback(
                            this.aresDbConnection.queryAndGetResultsAsync(aql, i, session),
                            new FutureCallback<String>()
                            {
                                @Override
                                public void onSuccess(String response)
                                {
                                    Page page = buildPage(aql, response);
                                    if (aresQL.isCacheable()) {
                                        cache.put(augmentedAQL, page);
                                    }
                                    pages.offer(new PageOrError(page, aql, null));
                                }

                                @Override
                                public void onFailure(Throwable error)
                                {
                                    pages.offer(new PageOrError(null, aql, error));
                                }
                            },
                            executor);
                }
            }
            pagesConsumed = 0;
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
        return pagesConsumed >= this.aresDbSplit.getAqls().size();
    }

    @Override
    public Page getNextPage()
    {
        fetchIfNeeded();
        if (isFinished()) {
            return null;
        }

        long start = System.nanoTime();
        session.getSessionLogger().log(() -> "Ares getNextPage start " + pagesConsumed);
        try {
            PageOrError pageOrError = pages.take();
            if (pageOrError.error != null) {
                throw new AresDbException(ARESDB_UNEXPECTED_ERROR, "Encountered error on aql " + pageOrError.aql, pageOrError.error);
            }
            log.debug("Took page %s", pagesConsumed);
            return pageOrError.page;
        }
        catch (InterruptedException e) {
            throw new AresDbException(ARESDB_UNEXPECTED_ERROR, "Error when polling for page", e);
        }
        finally {
            session.getSessionLogger().log(() -> "Ares getNextPage end " + pagesConsumed);
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
            session.getSessionLogger().log(() -> "Aql JSON Parsed");
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
            session.getSessionLogger().log(() -> "Aql JSON Parsed");
            return rowIndex;
        }
    }

    private Page buildPage(String aql, String response)
    {
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
        return pageBuilder.build();
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
        pagesConsumed = this.aresDbSplit.getAqls().size();
        executor.shutdownNow();
    }
}
