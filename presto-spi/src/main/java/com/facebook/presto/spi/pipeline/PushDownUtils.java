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
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class PushDownUtils
{
    private PushDownUtils() {}

    public static Optional<PushDownExpression> getColumnPredicate(Domain domain, Type type, String columnAlias)
    {
        PushDownExpression inputColumn = new PushDownInputColumn(type.getTypeSignature(), columnAlias);
        List<PushDownExpression> conditions = new ArrayList<>();
        TypeSignature booleanType = BOOLEAN.getTypeSignature();

        final List<PushDownExpression> discreteValuesWhiteList = new ArrayList<>();

        domain.getValues().getValuesProcessor().consume(
                ranges -> {
                    for (Range range : ranges.getOrderedRanges()) {
                        if (range.isSingleValue()) {
                            // accumulate all white list values and later convert them into an IN list
                            discreteValuesWhiteList.add(getLiteralFromMarker(range.getLow()));
                        }
                        else {
                            // get low bound
                            List<PushDownExpression> bounds = new ArrayList<>();
                            if (!range.getLow().isLowerUnbounded()) {
                                String op = (range.getLow().getBound() == Marker.Bound.EXACTLY) ? "<=" : "<";
                                bounds.add(new PushDownLogicalBinaryExpression(booleanType, getLiteralFromMarker(range.getLow()), op, inputColumn));
                            }
                            // get high bound
                            if (!range.getHigh().isUpperUnbounded()) {
                                String op = range.getHigh().getBound() == Marker.Bound.EXACTLY ? "<=" : "<";
                                bounds.add(new PushDownLogicalBinaryExpression(booleanType, inputColumn, op, getLiteralFromMarker(range.getHigh())));
                            }

                            conditions.add(combineExpressions(bounds, "AND").get());
                        }
                    }
                },
                discreteValues -> {
                    if (discreteValues.getValues().isEmpty()) {
                        return;
                    }

                    List<PushDownExpression> inList = discreteValues.getValues().stream().map(v -> getLiteralFromMarkerObject(domain.getValues().getType(), v)).collect(Collectors.toList());
                    if (discreteValues.isWhiteList()) {
                        discreteValuesWhiteList.addAll(inList);
                    }
                    else {
                        conditions.add(new PushDownInExpression(booleanType, discreteValues.isWhiteList(), inputColumn, inList));
                    }
                },
                allOrNone ->
                {
                    //no-op
                });

        if (!discreteValuesWhiteList.isEmpty()) {
            if (discreteValuesWhiteList.size() == 1) {
                conditions.add(new PushDownLogicalBinaryExpression(booleanType, inputColumn, "=", discreteValuesWhiteList.get(0)));
            }
            else {
                conditions.add(new PushDownInExpression(booleanType, true, inputColumn, discreteValuesWhiteList));
            }
        }

        return combineExpressions(conditions, "OR");
    }

    public static Optional<PushDownExpression> combineExpressions(List<PushDownExpression> expressions, String op)
    {
        if (expressions.isEmpty()) {
            return Optional.empty();
        }

        // combine conjucts using the given op
        if (expressions.size() == 1) {
            return Optional.of(expressions.get(0));
        }

        int idx = expressions.size() - 1;
        PushDownExpression result = expressions.get(idx);
        while (idx >= 1) {
            result = new PushDownLogicalBinaryExpression(BOOLEAN.getTypeSignature(), expressions.get(idx - 1), op, result);
            --idx;
        }

        return Optional.of(result);
    }

    private static PushDownLiteral getLiteralFromMarker(Marker marker)
    {
        return getLiteralFromMarkerObject(marker.getType(), marker.getValue());
    }

    private static PushDownLiteral getLiteralFromMarkerObject(Type type, Object value)
    {
        if (value instanceof Slice) {
            Slice slice = (Slice) value;
            return new PushDownLiteral(type.getTypeSignature(), slice.toStringUtf8(), null, null, null);
        }

        if (value instanceof Long) {
            return new PushDownLiteral(type.getTypeSignature(), null, (Long) value, null, null);
        }

        if (value instanceof Double) {
            return new PushDownLiteral(type.getTypeSignature(), null, null, (Double) value, null);
        }

        if (value instanceof Boolean) {
            return new PushDownLiteral(type.getTypeSignature(), null, null, null, (Boolean) value);
        }

        throw new IllegalArgumentException("unsupported market type in TupleDomain: " + value.getClass());
    }
}
