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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.pipeline.PushDownArithmeticExpression;
import com.facebook.presto.spi.pipeline.PushDownBetweenExpression;
import com.facebook.presto.spi.pipeline.PushDownCastExpression;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownExpressionVisitor;
import com.facebook.presto.spi.pipeline.PushDownFunction;
import com.facebook.presto.spi.pipeline.PushDownInExpression;
import com.facebook.presto.spi.pipeline.PushDownInputColumn;
import com.facebook.presto.spi.pipeline.PushDownLiteral;
import com.facebook.presto.spi.pipeline.PushDownLogicalBinaryExpression;
import com.facebook.presto.spi.pipeline.PushDownNotExpression;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTimeZone;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.query.PinotExpression.derived;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Convert {@link PushDownExpression} in project into Pinot complaint expression text
 */
class PinotProjectExpressionConverter
        extends PushDownExpressionVisitor<PinotExpression, Map<String, Selection>>
{
    // Pinot does not support modulus yet
    private static final Map<String, String> PRESTO_TO_PINOT_OP = ImmutableMap.of(
            "-", "SUB",
            "+", "ADD",
            "*", "MULT",
            "/", "DIV");

    private static final Set<String> TIME_EQUIVALENT_TYPES = ImmutableSet.of(StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.TINYINT, StandardTypes.SMALLINT);

    private final Optional<ConnectorSession> session;

    public PinotProjectExpressionConverter(Optional<ConnectorSession> session)
    {
        this.session = session;
    }

    @Override
    public PinotExpression visitInputColumn(PushDownInputColumn inputColumn, Map<String, Selection> context)
    {
        Selection input = requireNonNull(context.get(inputColumn.getName()), format("Input column %s does not exist in the input", inputColumn.getName()));
        return new PinotExpression(input.getDefinition(), input.getOrigin());
    }

    @Override
    public PinotExpression visitFunction(PushDownFunction function, Map<String, Selection> context)
    {
        switch (function.getName().toLowerCase(ENGLISH)) {
            case "date_trunc":
                boolean usePrestoDateTrunc = session.isPresent() ? PinotSessionProperties.isUsePrestoDateTrunc(session.get()) : false;
                return usePrestoDateTrunc ?
                    handleDateTruncViaPrestoDateTrunc(function, context) :
                    handleDateTruncViaDateTimeConvert(function, context);
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("function %s not supported yet", function.getName()));
        }
    }

    private PushDownFunction getExpressionAsFunction(PushDownExpression originalExpression, PushDownExpression expression, Map<String, Selection> context)
    {
        if (expression instanceof PushDownFunction) {
            return (PushDownFunction) expression;
        }
        else if (expression instanceof PushDownCastExpression) {
            PushDownCastExpression castExpression = ((PushDownCastExpression) expression);
            if (isImplicitCast(castExpression)) {
                return getExpressionAsFunction(originalExpression, castExpression.getInput(), context);
            }
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Could not dig function out of expression: " + originalExpression + ", inside of " + expression);
    }

    private PinotExpression handleDateTruncViaDateTimeConvert(PushDownFunction function, Map<String, Selection> context)
    {
        // Convert SQL standard function `DATE_TRUNC(INTERVAL, DATE/TIMESTAMP COLUMN)` to
        // Pinot's equivalent function `dateTimeConvert(columnName, inputFormat, outputFormat, outputGranularity)`
        // Pinot doesn't have a DATE/TIMESTAMP type. That means the input column (second argument) has been converted from numeric type to DATE/TIMESTAMP using one of the
        // conversion functions in SQL. First step is find the function and find its input column units (seconds, secondsSinceEpoch etc.)
        PushDownExpression timeInputParameter = function.getInputs().get(1);
        String inputColumn;
        String inputFormat;

        PushDownFunction timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter, context);
        switch (timeConversion.getName().toLowerCase(ENGLISH)) {
            case "from_unixtime":
                inputColumn = timeConversion.getInputs().get(0).accept(this, context).getDefinition();
                inputFormat = "'1:SECONDS:EPOCH'";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "not supported: " + timeConversion.getName());
        }

        String outputFormat = "'1:MILLISECONDS:EPOCH'";
        String outputGranularity;

        PushDownExpression intervalParameter = function.getInputs().get(0);
        if (!(intervalParameter instanceof PushDownLiteral)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                    "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        PushDownLiteral intervalUnit = (PushDownLiteral) intervalParameter;
        switch (intervalUnit.getStrValue()) {
            case "second":
                outputGranularity = "'1:SECONDS'";
                break;
            case "minute":
                outputGranularity = "'1:MINUTES'";
                break;
            case "hour":
                outputGranularity = "'1:HOURS'";
                break;
            case "day":
                outputGranularity = "'1:DAYS'";
                break;
            case "week":
                outputGranularity = "'1:WEEKS'";
                break;
            case "month":
                outputGranularity = "'1:MONTHS'";
                break;
            case "quarter":
                outputGranularity = "'1:QUARTERS'";
                break;
            case "year":
                outputGranularity = "'1:YEARS'";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                        "interval in date_trunc is not supported: " + intervalUnit.getStrValue());
        }

        return derived("dateTimeConvert(" + inputColumn + ", " + inputFormat + ", " + outputFormat + ", " + outputGranularity + ")");
    }

    private static String getTimeZoneFromExpression(PushDownExpression pushDownExpression)
    {
        if (pushDownExpression instanceof PushDownLiteral) {
            String strValue = ((PushDownLiteral) pushDownExpression).getStrValue();
            if (strValue != null) {
                return strValue;
            }
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Only support string literal as the second argument of from_unixtime for timezone");
    }

    private PinotExpression handleDateTruncViaPrestoDateTrunc(PushDownFunction function, Map<String, Selection> context)
    {
        PushDownExpression timeInputParameter = function.getInputs().get(1);
        String inputColumn;
        String inputTimeZone;
        String inputFormat;

        PushDownFunction timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter, context);
        switch (timeConversion.getName().toLowerCase(ENGLISH)) {
            case "from_unixtime":
                inputColumn = timeConversion.getInputs().get(0).accept(this, context).getDefinition();
                inputTimeZone = timeConversion.getInputs().size() > 1 ? getTimeZoneFromExpression(timeConversion.getInputs().get(1)) : DateTimeZone.UTC.getID();
                inputFormat = "seconds";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "not supported: " + timeConversion.getName());
        }

        PushDownExpression intervalParameter = function.getInputs().get(0);
        if (!(intervalParameter instanceof PushDownLiteral)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        PushDownLiteral intervalUnit = (PushDownLiteral) intervalParameter;

        return derived("prestoDateTrunc(" + inputColumn + "," + inputFormat + ", " + inputTimeZone + ", " + intervalUnit.getStrValue() + ")");
    }

    @Override
    public PinotExpression visitLogicalBinary(PushDownLogicalBinaryExpression logical, Map<String, Selection> context)
    {
        return derived(format("(%s %s %s)",
                logical.getLeft().accept(this, context).getDefinition(), logical.getOperator(), logical.getRight().accept(this, context).getDefinition()));
    }

    @Override
    public PinotExpression visitInExpression(PushDownInExpression in, Map<String, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "IN <list> expressions is not supported in project");
    }

    @Override
    public PinotExpression visitBetweenExpression(PushDownBetweenExpression between, Map<String, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "BETWEEN expressions is not supported in project");
    }

    @Override
    public PinotExpression visitNotExpression(PushDownNotExpression not, Map<String, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "NOT expressions is not supported in project");
    }

    @Override
    public PinotExpression visitLiteral(PushDownLiteral literal, Map<String, Selection> context)
    {
        return new PinotExpression(literal.toString(), Origin.LITERAL);
    }

    private static boolean isImplicitCast(PushDownCastExpression cast)
    {
        return cast.isImplicitCast(Optional.of((resultType, inputTypeSignature) -> Objects.equals(StandardTypes.TIMESTAMP, resultType) && TIME_EQUIVALENT_TYPES.contains(inputTypeSignature.getBase())));
    }

    @Override
    public PinotExpression visitCastExpression(PushDownCastExpression cast, Map<String, Selection> context)
    {
        if (isImplicitCast(cast)) {
            return cast.getInput().accept(this, context);
        }
        else {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Non implicit casts not supported: " + cast);
        }
    }

    @Override
    public PinotExpression visitArithmeticExpression(PushDownArithmeticExpression expression, Map<String, Selection> context)
    {
        PinotExpression right = expression.getRight().accept(this, context);
        if (expression.getLeft() == null) {
            // unary ...
            String prefix = expression.getOperator().equals("-") ? "-" : "";
            return derived(prefix + right.getDefinition());
        }

        PinotExpression left = expression.getLeft().accept(this, context);
        String prestoOp = expression.getOperator();
        String pinotOp = PRESTO_TO_PINOT_OP.get(prestoOp);
        if (pinotOp == null) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported binary expression " + prestoOp);
        }

        return derived(format("%s(%s, %s)", pinotOp, left.getDefinition(), right.getDefinition()));
    }
}
