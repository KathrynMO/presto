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
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNEXPECTED_ERROR;
import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_OUTPUT_TYPE;

public class AresDbPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(AresDbPageSource.class);

    private final AresDbSplit aresDbSplit;
    private final List<AresDbColumnHandle> columns;
    private final AresDbConnection aresDbConnection;
    private final AresDbConfig aresDbConfig;
    private final ConnectorSession session;

    // state information
    private boolean finished;
    private long readTimeNanos;

    public AresDbPageSource(AresDbSplit aresDbSplit, List<AresDbColumnHandle> columns, AresDbConnection aresDbConnection, AresDbConfig aresDbConfig, ConnectorSession session)
    {
        this.aresDbSplit = aresDbSplit;
        this.columns = columns;
        this.aresDbConnection = aresDbConnection;
        this.aresDbConfig = aresDbConfig;
        this.session = session;
    }

    private static void setValue(Type type, BlockBuilder blockBuilder, Object value)
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
        }
        else if (type instanceof TimestampType) {
            // output is always seconds since timeUnit is seconds
            long parsedValue = Long.parseUnsignedLong((String) value, 10) * 1000;
            type.writeLong(blockBuilder, parsedValue);
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
        }
        else if (type instanceof BooleanType) {
            if (value instanceof String) {
                type.writeBoolean(blockBuilder, Boolean.valueOf((String) value));
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
        }
        else if (type instanceof VarcharType) {
            if (value instanceof String) {
                Slice slice = Slices.utf8Slice((String) value);
                blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
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
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }

        long start = System.nanoTime();
        try {
            AugmentedAQL aql = aresDbSplit.getAugmentedAql();
            List<Type> expectedTypes = columns.stream().map(AresDbColumnHandle::getDataType).collect(Collectors.toList());
            PageBuilder pageBuilder = new PageBuilder(expectedTypes);
            List<AresDbOutputInfo> indicesMappingFromAresDbSchemaToPrestoSchema = AresDbQueryGeneratorContext.getIndicesMappingFromAresDbSchemaToPrestoSchema(aql.getExpressions(), columns);
            ImmutableList.Builder<BlockBuilder> columnBlockBuilders = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            for (AresDbOutputInfo outputInfo : indicesMappingFromAresDbSchemaToPrestoSchema) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputInfo.index);
                columnBlockBuilders.add(blockBuilder);
                columnTypes.add(expectedTypes.get(outputInfo.index));
            }

            int counter = issueAqlAndPopulate(aql, columnBlockBuilders.build(), columnTypes.build(), indicesMappingFromAresDbSchemaToPrestoSchema);
            pageBuilder.declarePositions(counter);
            return pageBuilder.build();
        }
        finally {
            finished = true;
            readTimeNanos += System.nanoTime() - start;
        }
    }

    private int issueAqlAndPopulate(AugmentedAQL aresQL, List<BlockBuilder> blockBuilders, List<Type> types, List<AresDbOutputInfo> outputInfos)
    {
        String response = aresDbConnection.queryAndGetResults(aresQL.getAql());

        JSONObject responseJson = JSONObject.parseObject(response);
        if (Optional.ofNullable(responseJson.getJSONArray("errors")).map(x -> x.size()).orElse(0) > 0) {
            throw new AresDbException(ARESDB_UNEXPECTED_ERROR, "Error in response " + response, aresQL.getAql());
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
                    setValue(types.get(outputIdx), blockBuilders.get(outputIdx), row.get(columnIdx));
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
                setValue(types.get(outputIdx), blockBuilders.get(outputIdx), valuesSoFar.get(columnIdx));
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
        finished = true;
    }
}
