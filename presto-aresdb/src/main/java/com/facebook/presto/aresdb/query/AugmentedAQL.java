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

package com.facebook.presto.aresdb.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class AugmentedAQL
{
    final String aql;
    final List<AQLExpression> expressions;

    @JsonCreator
    public AugmentedAQL(@JsonProperty("aql") String aql, @JsonProperty("expressions") List<AQLExpression> expressions)
    {
        this.aql = aql;
        this.expressions = ImmutableList.copyOf(expressions);
    }

    @JsonProperty
    public String getAql()
    {
        return aql;
    }

    @JsonProperty
    public List<AQLExpression> getExpressions()
    {
        return expressions;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("aql", aql)
                .add("expressions", expressions)
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
        AugmentedAQL that = (AugmentedAQL) o;
        return Objects.equals(aql, that.aql) &&
                Objects.equals(expressions, that.expressions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aql, expressions);
    }
}
