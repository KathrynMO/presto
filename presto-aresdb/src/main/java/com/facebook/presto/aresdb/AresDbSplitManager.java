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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
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
        Optional<Type> timeStampType = ((AresDbTableLayoutHandle) layout).getTable().getTimeStampType();
        AugmentedAQL fullRequest = aresDbQueryGeneratorContext.toAresDbRequest(Optional.of(session));
        List<AresDbSplit.AresQL> aqls = new ArrayList<>();

        if (timeStampType.isPresent() && parsedTimeFilter.isPresent() && aresDbQueryGeneratorContext.isInputTooBig(Optional.of(session)) && ScanParallelismFinder.canParallelize(true, tableScanPipeline)) {
            long low = parsedTimeFilter.get().getFrom();
            long high = parsedTimeFilter.get().getTo();
            Duration singleSplitLimit = AresDbSessionProperties.getSingleSplitLimit(session).get();
            long limit = singleSplitLimit.roundTo(TimeUnit.SECONDS);
            long unsafeToCacheLowMark = AresDbSessionProperties.getUnsafeToCacheInterval(session).map(interval -> AresDbQueryGeneratorContext.getSessionStartTimeInMs(session) / 1000L - interval.roundTo(TimeUnit.SECONDS)).orElse(0L);

            do {
                long thisSplitHigh = low % limit == 0 ? low + limit : ((low + limit - 1) / limit) * limit;
                thisSplitHigh = Math.min(thisSplitHigh, high);
                AresDbQueryGeneratorContext withNewTimeFilter = aresDbQueryGeneratorContext.withNewTimeBound(Domain.create(ValueSet.ofRanges(new Range(Marker.above(timeStampType.get(), low), Marker.below(timeStampType.get(), thisSplitHigh))), false));
                // Only cache non partial splits that are not too fresh
                boolean cacheable = thisSplitHigh < unsafeToCacheLowMark && thisSplitHigh - low == limit;
                aqls.add(new AresDbSplit.AresQL(withNewTimeFilter.toAresDbRequest(Optional.of(session)).getAql(), cacheable));
                low = thisSplitHigh;
            }
            while (low < high);

            Collections.shuffle(aqls, ThreadLocalRandom.current());
        }
        else {
            aqls.add(new AresDbSplit.AresQL(fullRequest.getAql(), false));
        }
        ImmutableList.Builder<AresDbSplit> splitBuilder = ImmutableList.builder();

        int maxNumSplits = AresDbSessionProperties.getMaxNumOfSplits(session);
        int numAqlsPerSplit = aqls.size() > maxNumSplits ? aqls.size() / maxNumSplits : 1;
        Preconditions.checkState(numAqlsPerSplit >= 1);
        int aqlIdx = 0;
        while (aqlIdx < aqls.size()) {
            int newAqlIdx = Math.min(aqlIdx + numAqlsPerSplit, aqls.size());
            splitBuilder.add(new AresDbSplit(aresDbTableLayoutHandle.getTable().getConnectorId(), fullRequest.getExpressions(), ImmutableList.copyOf(aqls.subList(aqlIdx, newAqlIdx))));
            aqlIdx = newAqlIdx;
        }
        return new FixedSplitSource(splitBuilder.build());
    }
}
