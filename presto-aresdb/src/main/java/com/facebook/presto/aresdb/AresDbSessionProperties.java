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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class AresDbSessionProperties
{
    private static final String SINGLE_SPLIT_LIMIT = "single_split_limit";
    private static final String MAX_LIMIT_WITHOUT_AGGREGATES = "max_limit_without_aggregates";

    private final List<PropertyMetadata<?>> sessionProperties;

    public static Duration getSingleSplitLimit(ConnectorSession session)
    {
        return session.getProperty(SINGLE_SPLIT_LIMIT, Duration.class);
    }

    public static long getMaxLimitWithoutAggregates(ConnectorSession session)
    {
        return session.getProperty(MAX_LIMIT_WITHOUT_AGGREGATES, Long.class);
    }

    @Inject
    public AresDbSessionProperties(AresDbConfig aresDbConfig)
    {
        sessionProperties = ImmutableList.of(
                longProperty(
                        MAX_LIMIT_WITHOUT_AGGREGATES,
                        "Max limit without aggregates",
                        aresDbConfig.getMaxLimitWithoutAggregates(),
                        false),
                new PropertyMetadata<>(
                        SINGLE_SPLIT_LIMIT,
                        "Single split limit duration",
                        createUnboundedVarcharType(),
                        Duration.class,
                        aresDbConfig.getSingleSplitLimit(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
