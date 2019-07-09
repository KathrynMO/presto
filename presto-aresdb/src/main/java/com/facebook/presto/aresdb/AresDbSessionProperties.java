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
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class AresDbSessionProperties
{
    private static final String SINGLE_SPLIT_LIMIT = "single_split_limit";
    private static final String UNSAFE_TO_CACHE_LIMIT = "unsafe_to_cache_interval";
    private static final String MAX_LIMIT_WITHOUT_AGGREGATES = "max_limit_without_aggregates";
    private static final String MAX_NUM_OF_SPLITS = "max_number_of_splits";

    private final List<PropertyMetadata<?>> sessionProperties;

    public static Optional<Duration> getSingleSplitLimit(ConnectorSession session)
    {
        return session.getProperty(SINGLE_SPLIT_LIMIT, Optional.class);
    }

    public static Optional<Duration> getUnsafeToCacheInterval(ConnectorSession session)
    {
        return session.getProperty(UNSAFE_TO_CACHE_LIMIT, Optional.class);
    }

    public static int getMaxNumOfSplits(ConnectorSession session)
    {
        return session.getProperty(MAX_NUM_OF_SPLITS, Integer.class);
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
                integerProperty(MAX_NUM_OF_SPLITS, "Max number of splits", aresDbConfig.getMaxNumOfSplits(), false),
                createOptionalDurationProperty(aresDbConfig.getSingleSplitLimit(), SINGLE_SPLIT_LIMIT, "Single split limit duration"),
                createOptionalDurationProperty(aresDbConfig.getUnsafeToCacheInterval(), UNSAFE_TO_CACHE_LIMIT, "Unsafe to cache limit"));
    }

    private static PropertyMetadata<Optional> createOptionalDurationProperty(Optional<Duration> duration, String name, String description)
    {
        return new PropertyMetadata<>(
                name,
                description,
                createUnboundedVarcharType(),
                Optional.class,
                duration,
                false,
                value -> Optional.of(Duration.valueOf((String) value)),
                Object::toString);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
