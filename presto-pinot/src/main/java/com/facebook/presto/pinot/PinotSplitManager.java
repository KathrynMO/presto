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
package com.facebook.presto.pinot;

import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.ScanParallelismFinder;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.pipeline.TableScanPipelineVisitor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.Iterables;
import com.linkedin.pinot.client.PinotClientException;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotSplit.createBrokerSplit;
import static com.facebook.presto.pinot.PinotSplit.createSegmentSplit;
import static com.facebook.presto.pinot.PinotUtils.checkType;
import static com.facebook.presto.spi.pipeline.PushDownUtils.combineExpressions;
import static com.facebook.presto.spi.pipeline.PushDownUtils.getColumnPredicate;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class PinotSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PinotSplitManager.class);
    private final String connectorId;
    private final PinotConnection pinotPrestoConnection;
    private final PinotConfig pinotConfig;

    @Inject
    public PinotSplitManager(PinotConnectorId connectorId, PinotConnection pinotPrestoConnection, PinotConfig pinotConfig)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotPrestoConnection = requireNonNull(pinotPrestoConnection, "pinotPrestoConnection is null");
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
    }

    static PushDownExpression getPredicate(TupleDomain<ColumnHandle> constraint, Map<ColumnHandle, String> columnAliasMap)
    {
        List<PushDownExpression> expressions = new ArrayList<>();
        Map<ColumnHandle, Domain> columnHandleDomainMap = constraint.getDomains().get();
        for (ColumnHandle k : columnHandleDomainMap.keySet()) {
            Domain domain = columnHandleDomainMap.get(k);
            Optional<PushDownExpression> columnPredicate = getColumnPredicate(domain, ((PinotColumnHandle) k).getDataType(), columnAliasMap.get(k));
            if (columnPredicate.isPresent()) {
                expressions.add(columnPredicate.get());
            }
        }

        Optional<PushDownExpression> predicate = combineExpressions(expressions, "AND");
        if (!predicate.isPresent()) {
            throw new IllegalStateException("TupleDomain resolved to empty predicate: " + constraint);
        }

        return predicate.get();
    }

    private static TableScanPipeline getScanPipeline(PinotTableLayoutHandle pinotTable)
    {
        if (!pinotTable.getScanPipeline().isPresent()) {
            throw new IllegalArgumentException("Scan pipeline is missing from the Pinot table layout handle");
        }

        return pinotTable.getScanPipeline().get();
    }

    protected static TableScanPipeline addTupleDomainToScanPipelineIfNeeded(Optional<TupleDomain<ColumnHandle>> constraint, TableScanPipeline existingPipeline)
    {
        // if there is no constraint or the constraint selects all then just return the existing pipeline
        if (!constraint.isPresent() || constraint.get().isAll()) {
            return existingPipeline;
        }

        // Check if the pipeline already contains the filter. If it is, then the TupleDomain part is already accommodated into the pipeline
        if (FilterFinder.hasFilter(existingPipeline)) {
            return existingPipeline;
        }

        checkArgument(existingPipeline.getPipelineNodes().size() == 1, "expected to contain just the scan node in pipeline");
        final PipelineNode baseNode = existingPipeline.getPipelineNodes().get(0);
        checkArgument(baseNode instanceof TablePipelineNode, "expected ");
        final TablePipelineNode tablePipelineNode = (TablePipelineNode) baseNode;

        // Create map of underlying column to column alias that TablePipelineNode node exposes as. We are constructing filter on top of the TablePipelineNode and
        // the filter should refer the column names in terms of what is exposed by the TablePipelineNode. TablePipelineNode can map the underlying ColumnHandle to a different
        // name than the actual column name.
        Map<ColumnHandle, String> columnAliasMap = new HashMap<>();
        List<ColumnHandle> inputColumns = tablePipelineNode.getInputColumns();
        List<String> outputColumnAliases = tablePipelineNode.getOutputColumns();
        for (int i = 0; i < inputColumns.size(); i++) {
            columnAliasMap.put(inputColumns.get(i), outputColumnAliases.get(i));
        }

        // convert TupleDomain into FilterPipelineNode
        PushDownExpression predicate = getPredicate(constraint.get(), columnAliasMap);

        final FilterPipelineNode filterNode = new FilterPipelineNode(predicate, tablePipelineNode.getOutputColumns(), tablePipelineNode.getRowType(), Optional.empty(), Optional.empty());

        TableScanPipeline newScanPipeline = new TableScanPipeline();
        newScanPipeline.addPipeline(tablePipelineNode, existingPipeline.getOutputColumnHandles());
        newScanPipeline.addPipeline(filterNode, existingPipeline.getOutputColumnHandles());

        return newScanPipeline;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        PinotTableLayoutHandle pinotLayoutHandle = checkType(layout, PinotTableLayoutHandle.class, "expected a Pinot table layout handle");

        TableScanPipeline scanPipeline = getScanPipeline(pinotLayoutHandle);

        if (ScanParallelismFinder.canParallelize(PinotSessionProperties.isScanParallelismEnabled(session), scanPipeline)) {
            scanPipeline = addTupleDomainToScanPipelineIfNeeded(pinotLayoutHandle.getConstraint(), scanPipeline);

            return generateSplitsForSegmentBasedScan(pinotLayoutHandle, scanPipeline, PinotSessionProperties.getNumSegmentsPerSplit(session));
        }
        else {
            return generateSplitForBrokerBasedScan(scanPipeline);
        }
    }

    protected ConnectorSplitSource generateSplitForBrokerBasedScan(TableScanPipeline scanPipeline)
    {
        return new FixedSplitSource(singletonList(createBrokerSplit(connectorId, scanPipeline)));
    }

    protected ConnectorSplitSource generateSplitsForSegmentBasedScan(PinotTableLayoutHandle pinotLayoutHandle, TableScanPipeline scanPipeline, int segmentsPerSplit)
    {
        PinotTableHandle tableHandle = pinotLayoutHandle.getTable();
        String tableName = tableHandle.getTableName();
        Map<String, Map<String, List<String>>> routingTable;
        Map<String, String> timeBoundary;

        try {
            routingTable = pinotPrestoConnection.getRoutingTable(tableName);
            timeBoundary = pinotPrestoConnection.getTimeBoundary(tableName);
        }
        catch (Exception e) {
            log.error("Failed to fetch table status for Pinot table: %s, Exceptions: %s", tableName, e);
            throw new PinotClientException("Failed to fetch table status for Pinot table: " + tableName, e);
        }

        Optional<String> offlineTimePredicate = Optional.empty();
        Optional<String> onlineTimePredicate = Optional.empty();

        if (timeBoundary.containsKey("timeColumnName") && timeBoundary.containsKey("timeColumnValue")) {
            String timeColumnName = timeBoundary.get("timeColumnName");
            String timeColumnValue = timeBoundary.get("timeColumnValue");

            offlineTimePredicate = Optional.of(format("%s < %s", timeColumnName, timeColumnValue));
            onlineTimePredicate = Optional.of(format("%s >= %s", timeColumnName, timeColumnValue));
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        if (!routingTable.isEmpty()) {
            generateSegmentSplits(splits, routingTable, onlineTimePredicate, tableName, "_REALTIME", scanPipeline, segmentsPerSplit);
            generateSegmentSplits(splits, routingTable, offlineTimePredicate, tableName, "_OFFLINE", scanPipeline, segmentsPerSplit);
        }

        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }

    protected void generateSegmentSplits(List<ConnectorSplit> splits, Map<String, Map<String, List<String>>> routingTable, Optional<String> timePredicate,
            String tableName, String tableNameSuffix, TableScanPipeline scanPipeline, int segmentsPerSplit)
    {
        final String finalTableName = tableName + tableNameSuffix;
        for (String routingTableName : routingTable.keySet()) {
            if (!routingTableName.equalsIgnoreCase(finalTableName)) {
                continue;
            }

            String pql = PinotQueryGenerator.generateForSegmentSplits(scanPipeline, Optional.of(tableNameSuffix), timePredicate, Optional.of(pinotConfig)).getPql();

            Map<String, List<String>> hostToSegmentsMap = routingTable.get(routingTableName);
            hostToSegmentsMap.forEach((host, segments) -> {
                // segments is already shuffled
                Iterables.partition(segments, Math.min(segments.size(), segmentsPerSplit)).forEach(segmentsForThisSplit -> splits.add(createSegmentSplit(connectorId, pql, segmentsForThisSplit, host)));
            });
        }
    }

    static class FilterFinder
            extends TableScanPipelineVisitor<Boolean, Boolean>
    {
        static boolean hasFilter(TableScanPipeline scanPipeline)
        {
            FilterFinder filterFinder = new FilterFinder();
            for (PipelineNode pipelineNode : scanPipeline.getPipelineNodes()) {
                if (pipelineNode.accept(filterFinder, null)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitNode(PipelineNode node, Boolean context)
        {
            return false;
        }

        @Override
        public Boolean visitFilterNode(FilterPipelineNode filter, Boolean context)
        {
            return true;
        }
    }
}
