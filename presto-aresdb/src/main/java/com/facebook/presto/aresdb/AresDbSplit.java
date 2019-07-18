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

import com.facebook.presto.aresdb.query.AQLExpression;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AresDbSplit
        implements ConnectorSplit
{
    private final AresDbConnectorId connectorId;
    private final List<AQLExpression> expressions;
    private final List<AresQL> aqls;
    private final int index;

    public static class AresQL
    {
        private final String aql;
        private final boolean cacheable;

        @JsonCreator
        public AresQL(@JsonProperty("aql") String aql, @JsonProperty("cacheable") boolean cacheable)
        {
            this.aql = aql;
            this.cacheable = cacheable;
        }

        @JsonProperty
        public String getAql()
        {
            return aql;
        }

        @JsonProperty
        public boolean isCacheable()
        {
            return cacheable;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("aql", aql)
                    .add("cacheable", cacheable)
                    .toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AresQL aresQL = (AresQL) o;
            return cacheable == aresQL.cacheable &&
                    Objects.equals(aql, aresQL.aql);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(aql, cacheable);
        }
    }

    @JsonCreator
    public AresDbSplit(
            @JsonProperty("connectorId") AresDbConnectorId connectorId,
            @JsonProperty("expressions") List<AQLExpression> expressions,
            @JsonProperty("aqls") List<AresQL> aqls,
            @JsonProperty("index") int index)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.expressions = requireNonNull(expressions, "expressions is null");
        this.aqls = requireNonNull(aqls, "aqls is null");
        this.index = index;
    }

    @JsonProperty
    public AresDbConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public List<AQLExpression> getExpressions()
    {
        return expressions;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return null;
    }

    @JsonProperty
    public List<AresQL> getAqls()
    {
        return aqls;
    }

    @JsonProperty
    public int getIndex()
    {
        return index;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("expressions", expressions)
                .add("aqls", aqls)
                .add("index", index)
                .toString();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
