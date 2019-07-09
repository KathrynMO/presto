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

import com.facebook.presto.aresdb.query.AugmentedAQL;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class AresDbPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final AresDbConnection aresDbConnection;
    private final AresDbConfig aresDbConfig;
    private final Cache<AugmentedAQL, Page> cache;

    @Inject
    public AresDbPageSourceProvider(AresDbConnection aresDbConnection, AresDbConfig aresDbConfig)
    {
        this.aresDbConnection = requireNonNull(aresDbConnection, "aresDbConnection is null");
        this.aresDbConfig = requireNonNull(aresDbConfig, "aresDb config is null");
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite((long) aresDbConfig.getCacheDuration().convertTo(TimeUnit.SECONDS).getValue(), TimeUnit.SECONDS)
                .maximumWeight(aresDbConfig.getMaxCacheSize().toBytes())
                .weigher((Weigher<AugmentedAQL, Page>) (key, value) -> (int) value.getRetainedSizeInBytes())
                .recordStats()
                .build();
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        AresDbSplit aresDbSplit = (AresDbSplit) split;
        List<AresDbColumnHandle> aresDbColumns = columns.stream().map(c -> (AresDbColumnHandle) c).collect(Collectors.toList());

        return new AresDbPageSource(aresDbSplit, aresDbColumns, aresDbConnection, session, cache);
    }

    @Managed
    public double getHitRate()
    {
        return cache.stats().hitRate();
    }

    @Managed
    public double getMissRate()
    {
        return cache.stats().missRate();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @Managed
    public long getCacheSize()
    {
        return cache.size();
    }
}
