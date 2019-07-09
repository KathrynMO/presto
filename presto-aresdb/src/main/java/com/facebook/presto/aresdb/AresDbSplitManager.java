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

import com.facebook.presto.aresdb.query.AresDbQueryGenerator;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext;
import com.facebook.presto.aresdb.query.AugmentedAQL;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.pipeline.ScanParallelismFinder;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class AresDbSplitManager
        implements ConnectorSplitManager
{
    private final AresDbConfig aresDbConfig;

    @Inject
    public AresDbSplitManager(AresDbConfig aresDbConfig)
    {
        this.aresDbConfig = aresDbConfig;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        AresDbTableLayoutHandle aresDbTableLayoutHandle = (AresDbTableLayoutHandle) layout;
        TableScanPipeline tableScanPipeline = aresDbTableLayoutHandle.getScanPipeline().get();
        AresDbQueryGeneratorContext aresDbQueryGeneratorContext = AresDbQueryGenerator.generateContext(tableScanPipeline, Optional.of(aresDbConfig), Optional.of(session));
        aresDbQueryGeneratorContext.validate(session);
        Optional<AresDbQueryGeneratorContext.TimeFilterParsed> parsedTimeFilter = aresDbQueryGeneratorContext.getParsedTimeFilter(Optional.of(session));
        ImmutableList.Builder<AresDbSplit> splitBuilder = ImmutableList.builder();
        Optional<Type> timeStampType = ((AresDbTableLayoutHandle) layout).getTable().getTimeStampType();

        if (timeStampType.isPresent() && parsedTimeFilter.isPresent() && aresDbQueryGeneratorContext.isInputTooBig(Optional.of(session)) && ScanParallelismFinder.canParallelize(true, tableScanPipeline)) {
            long low = parsedTimeFilter.get().getFrom();
            long high = parsedTimeFilter.get().getTo();
            Duration singleSplitLimit = AresDbSessionProperties.getSingleSplitLimit(session);
            long limit = singleSplitLimit.roundTo(TimeUnit.SECONDS);
            do {
                long nextLow = Math.min(low + limit, high);
                AresDbQueryGeneratorContext withNewTimeFilter = aresDbQueryGeneratorContext.withNewTimeBound(Domain.create(ValueSet.ofRanges(new Range(Marker.above(timeStampType.get(), low), Marker.below(timeStampType.get(), nextLow))), false));
                splitBuilder.add(new AresDbSplit(aresDbTableLayoutHandle.getTable().getConnectorId(), withNewTimeFilter.toAresDbRequest(Optional.of(session))));
                low = nextLow;
            }
            while (low < high);
        }
        else {
            AugmentedAQL augmentedAQL = aresDbQueryGeneratorContext.toAresDbRequest(Optional.of(session));
            splitBuilder.add(new AresDbSplit(aresDbTableLayoutHandle.getTable().getConnectorId(), augmentedAQL));
        }
        return new FixedSplitSource(splitBuilder.build());
    }
}
