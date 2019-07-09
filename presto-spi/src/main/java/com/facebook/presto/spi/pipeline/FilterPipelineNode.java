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
package com.facebook.presto.spi.pipeline;

import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FilterPipelineNode
        extends PipelineNode
{
    private final PushDownExpression predicate;
    private final List<String> outputColumns;
    private final List<Type> rowType;
    private final Optional<Map<String, TypeAndDomain>> symbolNameToDomains;
    private final Optional<PushDownExpression> remainingPredicate;

    @JsonCreator
    public FilterPipelineNode(
            @JsonProperty("predicate") PushDownExpression predicate,
            @JsonProperty("outputColumns") List<String> outputColumns,
            @JsonProperty("rowType") List<Type> rowType,
            @JsonProperty("symbolNameToDomains") Optional<Map<String, TypeAndDomain>> symbolNameToDomains,
            @JsonProperty("remainingPredicate") Optional<PushDownExpression> remainingPredicate)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
        this.rowType = requireNonNull(rowType, "rowType is null");
        this.symbolNameToDomains = symbolNameToDomains;
        this.remainingPredicate = remainingPredicate;
    }

    @JsonProperty
    public Optional<Map<String, TypeAndDomain>> getSymbolNameToDomains()
    {
        return symbolNameToDomains;
    }

    @JsonProperty
    public Optional<PushDownExpression> getRemainingPredicate()
    {
        return remainingPredicate;
    }

    @Override
    @JsonProperty
    public List<String> getOutputColumns()
    {
        return outputColumns;
    }

    @Override
    @JsonProperty
    public List<Type> getRowType()
    {
        return rowType;
    }

    @JsonProperty
    public PushDownExpression getPredicate()
    {
        return predicate;
    }

    @Override
    public String toString()
    {
        return "Filter: " + predicate;
    }

    @Override
    public <R, C> R accept(TableScanPipelineVisitor<R, C> visitor, C context)
    {
        return visitor.visitFilterNode(this, context);
    }

    public static class TypeAndDomain
    {
        private final String name;
        private final Type type;
        private final Domain domain;

        @JsonCreator
        public TypeAndDomain(@JsonProperty("name") String name, @JsonProperty("type") Type type, @JsonProperty("domain") Domain domain)
        {
            this.name = name;
            this.type = type;
            this.domain = domain;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @JsonProperty
        public Domain getDomain()
        {
            return domain;
        }
    }
}
