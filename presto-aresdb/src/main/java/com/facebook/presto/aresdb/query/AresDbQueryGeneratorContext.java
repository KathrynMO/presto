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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.aresdb.AresDbColumnHandle;
import com.facebook.presto.aresdb.AresDbException;
import com.facebook.presto.aresdb.AresDbSessionProperties;
import com.facebook.presto.aresdb.AresDbTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.pipeline.JoinPipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.predicate.AllOrNone;
import com.facebook.presto.spi.predicate.DiscreteValues;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.units.Duration;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.spi.pipeline.PushDownUtils.getColumnPredicate;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbQueryGeneratorContext
{
    // Fields defining the query
    // order map that maps the column definition in terms of input relation column(s)
    private final LinkedHashMap<String, Selection> selections;
    private final List<String> groupByColumns;
    private final AresDbTableHandle tableHandle;
    private final Optional<Domain> timeFilter;
    private final List<String> filters;
    private final Long limit;
    private final boolean aggregationApplied;
    private final List<JoinInfo> joins;

    AresDbQueryGeneratorContext(LinkedHashMap<String, Selection> selections, AresDbTableHandle tableHandle)
    {
        this(selections, tableHandle, Optional.empty(), ImmutableList.of(), false, ImmutableList.of(), null, ImmutableList.of());
    }

    private AresDbQueryGeneratorContext(LinkedHashMap<String, Selection> selections, AresDbTableHandle tableHandle, Optional<Domain> timeFilter,
            List<String> filters, boolean aggregationApplied, List<String> groupByColumns, Long limit, List<JoinInfo> joins)
    {
        this.selections = requireNonNull(selections, "selections can't be null");
        this.tableHandle = requireNonNull(tableHandle, "source can't be null");
        this.aggregationApplied = aggregationApplied;
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available");
        this.filters = filters;
        this.limit = limit;
        this.timeFilter = timeFilter;
        this.joins = joins;
    }

    /**
     * Apply the given filter to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withFilters(List<String> extraFilters, Optional<Domain> extraTimeFilter)
    {
        checkArgument(!hasAggregation(), "AresDB doesn't support filtering the results of aggregation");
        checkArgument(!hasLimit(), "AresDB doesn't support filtering on top of the limit");
        checkArgument(!extraTimeFilter.isPresent() || !this.timeFilter.isPresent(), "Cannot put a time filter on top of a time filter");
        List<String> newFilters = ImmutableList.<String>builder().addAll(this.filters).addAll(extraFilters).build();
        Optional<Domain> newTimeFilter = this.timeFilter.isPresent() ? this.timeFilter : extraTimeFilter;
        return new AresDbQueryGeneratorContext(selections, tableHandle, newTimeFilter, newFilters, aggregationApplied, groupByColumns, limit, joins);
    }

    /**
     * Apply the aggregation to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withAggregation(LinkedHashMap<String, Selection> newSelections, List<String> groupByColumns)
    {
        // there is only one aggregation supported.
        checkArgument(!hasAggregation(), "AresDB doesn't support aggregation on top of the aggregated data");
        checkArgument(!hasLimit(), "AresDB doesn't support aggregation on top of the limit");
        return new AresDbQueryGeneratorContext(newSelections, tableHandle, timeFilter, filters, true, groupByColumns, limit, joins);
    }

    /**
     * Apply new selections/project to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withProject(LinkedHashMap<String, Selection> newSelections)
    {
        checkArgument(!hasAggregation(), "AresDB doesn't support new selections on top of the aggregated data");
        return new AresDbQueryGeneratorContext(newSelections, tableHandle, timeFilter, filters, false, ImmutableList.of(), limit, joins);
    }

    /**
     * Apply limit to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withLimit(long limit)
    {
        checkArgument(!hasLimit(), "Don't support a limit atop a limit in ares");
        return new AresDbQueryGeneratorContext(selections, tableHandle, timeFilter, filters, aggregationApplied, groupByColumns, limit, joins);
    }

    private boolean hasAggregation()
    {
        return aggregationApplied;
    }

    private boolean hasLimit()
    {
        return limit != null;
    }

    public LinkedHashMap<String, Selection> getSelections()
    {
        return selections;
    }

    public Optional<String> getTimeColumn()
    {
        return tableHandle.getTimeColumnName();
    }

    public void validate(ConnectorSession session)
    {
        Long limit = this.limit;
        if (!hasAggregation()) {
            if (limit == null) {
                limit = -1L;
            }
            long maxLimit = AresDbSessionProperties.getMaxLimitWithoutAggregates(session);
            if (maxLimit > 0 && (limit < 0 || limit > maxLimit)) {
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Inferred limit %d (specified %d) is greater than max aresdb allowed limit of %d when no aggregates are present in table %s", limit, this.limit, maxLimit, tableHandle.getTableName()));
            }
        }
    }

    /**
     * Convert the current context to a AresDB request (AQL)
     */
    public AugmentedAQL toAresDbRequest(Optional<ConnectorSession> session)
    {
        List<Selection> measures;
        List<Selection> dimensions;
        if (groupByColumns.isEmpty() && !hasAggregation()) {
            // simple selections
            measures = ImmutableList.of(Selection.of("1", Origin.LITERAL, BIGINT));
            dimensions = selections.values().stream().collect(Collectors.toList());
        }
        else if (!groupByColumns.isEmpty()) {
            measures = selections.entrySet().stream()
                    .filter(c -> !groupByColumns.contains(c.getKey()))
                    .map(c -> c.getValue())
                    .collect(Collectors.toList());

            dimensions = selections.entrySet().stream()
                    .filter(c -> groupByColumns.contains(c.getKey()))
                    .map(c -> c.getValue())
                    .collect(Collectors.toList());
        }
        else {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "Ares does not handle non group by aggregation queries yet, either fix in Ares or add final aggregation to table " + tableHandle.getTableName());
        }

        JSONObject request = new JSONObject();
        JSONArray measuresJson = new JSONArray();
        JSONArray dimensionsJson = new JSONArray();

        for (Selection measure : measures) {
            JSONObject measureJson = new JSONObject();
            measureJson.put("sqlExpression", measure.getDefinition());
            measuresJson.add(measureJson);
        }

        Set<TimeZoneKey> timeZonesInDimension = new HashSet<>();
        for (Selection dimension : dimensions) {
            JSONObject dimensionJson = new JSONObject();
            dimensionJson.put("sqlExpression", dimension.getDefinition());
            dimension.timeTokenizer.ifPresent(timeTokenizer -> {
                timeZonesInDimension.add(timeTokenizer.getTimeZoneKey());
                dimensionJson.put("timeBucketizer", timeTokenizer.getExpression());
                dimensionJson.put("timeUnit", "second"); // timeUnit: millisecond does not really work
            });
            dimensionsJson.add(dimensionJson);
        }
        Optional<TimeZoneKey> tzKey = session.map(ConnectorSession::getTimeZoneKey);
        if (!timeZonesInDimension.isEmpty()) {
            if (timeZonesInDimension.size() > 1) {
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Found multiple time zone references in query: %", timeZonesInDimension));
            }
            tzKey = Optional.of(Iterables.getOnlyElement(timeZonesInDimension));
        }
        request.put("table", tableHandle.getTableName());
        request.put("measures", measuresJson);
        request.put("dimensions", dimensionsJson);
        tzKey.ifPresent(t -> request.put("timeZone", t.getId()));

        List<String> filters = new ArrayList<>(this.filters);
        addTimeFilter(session, request, filters);

        if (!filters.isEmpty()) {
            JSONArray filterJson = new JSONArray();
            filters.forEach(filterJson::add);
            request.put("rowFilters", filterJson);
        }

        Long limit = this.limit;
        if (!hasAggregation() && limit == null) {
            limit = -1L;
        }
        if (limit != null) {
            request.put("limit", limit);
        }

        JSONArray requestJoins = new JSONArray(joins.size());
        for (JoinInfo joinInfo : joins) {
            JSONObject join = new JSONObject();
            join.put("alias", joinInfo.alias);
            join.put("table", joinInfo.name);
            JSONArray conditions = new JSONArray();
            joinInfo.conditions.forEach(conditions::add);
            join.put("conditions", conditions);
            requestJoins.add(join);
        }
        if (!requestJoins.isEmpty()) {
            request.put("joins", requestJoins);
        }

        String aql = new JSONObject(ImmutableMap.of("queries", new JSONArray(ImmutableList.of(request)))).toJSONString();

        return new AugmentedAQL(aql, getReturnedAQLExpressions());
    }

    private List<AQLExpression> getReturnedAQLExpressions()
    {
        LinkedHashMap<String, Selection> expressionsInOrder = new LinkedHashMap<>();
        if (!groupByColumns.isEmpty()) {
            // Sanity check
            for (String groupByColumn : groupByColumns) {
                if (!selections.containsKey(groupByColumn)) {
                    throw new IllegalStateException(format("Group By column (%s) definition not found in input selections: ",
                            groupByColumn, Joiner.on(",").withKeyValueSeparator(":").join(selections)));
                }
            }

            // first add the time bucketizer group by columns
            for (String groupByColumn : groupByColumns) {
                Selection groupByColumnDefinition = selections.get(groupByColumn);
                if (groupByColumnDefinition.getTimeTokenizer().isPresent()) {
                    expressionsInOrder.put(groupByColumn, groupByColumnDefinition);
                }
            }

            // next add the non-time bucketizer group by columns
            for (String groupByColumn : groupByColumns) {
                Selection groupByColumnDefinition = selections.get(groupByColumn);
                if (!groupByColumnDefinition.getTimeTokenizer().isPresent()) {
                    expressionsInOrder.put(groupByColumn, groupByColumnDefinition);
                }
            }
            // Group by columns come first and have already been added above
            // so we are adding the metrics below
            expressionsInOrder.putAll(selections);
        }
        else {
            expressionsInOrder.putAll(selections);
        }

        ImmutableList.Builder<AQLExpression> outputInfos = ImmutableList.builder();
        for (Map.Entry<String, Selection> expression : expressionsInOrder.entrySet()) {
            outputInfos.add(new AQLExpression(expression.getKey(), expression.getValue().getTimeTokenizer()));
        }
        return outputInfos.build();
    }

    public static List<AresDbOutputInfo> getIndicesMappingFromAresDbSchemaToPrestoSchema(List<AQLExpression> expressionsInOrder, List<AresDbColumnHandle> handles)
    {
        checkState(handles.size() == expressionsInOrder.size(), "Expected returned expressions %s to match column handles %s",
                Joiner.on(",").join(expressionsInOrder), Joiner.on(",").join(handles));
        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < handles.size(); ++i) {
            String columnName = handles.get(i).getColumnName();
            Integer prev = nameToIndex.put(columnName.toLowerCase(ENGLISH), i);
            if (prev != null) {
                throw new IllegalStateException(format("Expected AresDB column handle %s to occur only once, but we have: %s", columnName, Joiner.on(",").join(handles)));
            }
        }
        ImmutableList.Builder<AresDbOutputInfo> outputInfos = ImmutableList.builder();
        expressionsInOrder.forEach(expression -> {
            Integer index = nameToIndex.get(expression.getName());
            if (index == null) {
                throw new IllegalStateException(format("Expected to find a AresDB column handle for the expression %s, but we have %s",
                        expression, Joiner.on(",").withKeyValueSeparator(":").join(nameToIndex)));
            }
            outputInfos.add(new AresDbOutputInfo(index, expression.getTimeTokenizer()));
        });
        return outputInfos.build();
    }

    public AresDbQueryGeneratorContext withNewTimeBound(Domain newTimeFilter)
    {
        return new AresDbQueryGeneratorContext(selections, tableHandle, Optional.of(newTimeFilter), filters, aggregationApplied, groupByColumns, limit, joins);
    }

    public static class TimeFilterParsed
    {
        private final String timeColumnName;
        private final long from;
        private final long to;
        private final Optional<PushDownExpression> extra;

        public TimeFilterParsed(String timeColumnName, long from, long to, Optional<PushDownExpression> extra)
        {
            this.timeColumnName = timeColumnName;
            this.from = from;
            this.to = to;
            this.extra = extra;
        }

        public long getBoundInSeconds()
        {
            return to - from;
        }

        public long getFrom()
        {
            return from;
        }

        public long getTo()
        {
            return to;
        }

        public Optional<PushDownExpression> getExtra()
        {
            return extra;
        }

        public String getTimeColumnName()
        {
            return timeColumnName;
        }
    }

    private static class TimeDomainConsumerHelper
    {
        final String timeColumn;
        Optional<Long> from = Optional.empty();
        Optional<Long> to = Optional.empty();
        boolean none;
        boolean needsRowFilter;

        public TimeDomainConsumerHelper(String timeColumn)
        {
            this.timeColumn = timeColumn;
        }

        void consumeRanges(Ranges ranges)
        {
            if (ranges.getRangeCount() == 0) {
                none = true;
            }
            else if (ranges.getRangeCount() == 1) {
                from = Optional.of(objectToTimeLiteral(ranges.getOrderedRanges().get(0).getLow().getValue(), true));
                to = Optional.of(objectToTimeLiteral(ranges.getOrderedRanges().get(0).getHigh().getValue(), false));
            }
            else if (ranges.getRangeCount() > 1) {
                Range span = ranges.getSpan();
                from = span.getLow().isLowerUnbounded() ? Optional.empty() : Optional.of(objectToTimeLiteral(span.getLow().getValue(), true));
                to = span.getHigh().isUpperUnbounded() ? Optional.empty() : Optional.of(objectToTimeLiteral(span.getHigh().getValue(), false));
                needsRowFilter = bound() > ranges.getRangeCount();
            }
        }

        private long bound()
        {
            return (to.get() - from.get() + 1);
        }

        void consumeDiscreteValues(DiscreteValues discreteValues)
        {
            List<Object> valuesSorted = discreteValues.getValues().stream().sorted(Comparator.comparingDouble(literal -> ((Number) literal).doubleValue())).collect(toImmutableList());
            from = Optional.of(objectToTimeLiteral(valuesSorted.get(0), true));
            to = Optional.of(objectToTimeLiteral(valuesSorted.get(valuesSorted.size() - 1), false));
            needsRowFilter = bound() > valuesSorted.size();
        }

        void consumeAllOrNone(AllOrNone allOrNone)
        {
            if (!allOrNone.isAll()) {
                none = true;
            }
        }

        public void restrict(Optional<TimeFilterParsed> retentionTime)
        {
            if (none || !retentionTime.isPresent()) {
                return;
            }
            long retentionFrom = retentionTime.get().getFrom();
            long retentionTo = retentionTime.get().getTo();
            from = Optional.of(from.orElse(retentionFrom));
            to = Optional.of(to.orElse(retentionTo));
        }

        public Optional<TimeFilterParsed> get(Supplier<Optional<PushDownExpression>> extraSupplier)
        {
            if (from.isPresent() && to.isPresent()) {
                Optional<PushDownExpression> extra = needsRowFilter ? extraSupplier.get() : Optional.empty();
                return Optional.of(new TimeFilterParsed(timeColumn, from.get(), to.get(), extra));
            }
            else {
                checkState(!from.isPresent() && !to.isPresent(), String.format("Both from and to should be absent: %s and %s", from, to));
                return Optional.empty();
            }
        }
    }

    private void addTimeFilter(Optional<ConnectorSession> session, JSONObject request, List<String> rowFilters)
    {
        getParsedTimeFilter(session).ifPresent(timeFilterParsed -> {
            JSONObject timeFilterJson = new JSONObject();
            timeFilterJson.put("column", timeFilterParsed.getTimeColumnName());
            timeFilterJson.put("from", String.format("%d", timeFilterParsed.from));
            timeFilterJson.put("to", String.format("%d", timeFilterParsed.to));
            request.put("timeFilter", timeFilterJson);
            timeFilterParsed.getExtra().ifPresent(extraPd -> rowFilters.add(extraPd.accept(new AresDbExpressionConverter(), selections).getDefinition()));
        });
    }

    public Optional<TimeFilterParsed> getParsedTimeFilter(Optional<ConnectorSession> session)
    {
        Optional<TimeFilterParsed> retentionTime = retentionTimeFilter(session, tableHandle);
        Optional<String> timeColumnName = tableHandle.getTimeColumnName();
        if (!timeColumnName.isPresent() || timeFilter.map(Domain::isAll).orElse(true)) {
            return retentionTime;
        }
        ValueSet values = timeFilter.get().getValues();
        String timeColumn = timeColumnName.get();
        TimeDomainConsumerHelper helper = new TimeDomainConsumerHelper(timeColumn);
        values.getValuesProcessor().consume(
                ranges -> helper.consumeRanges(ranges),
                discreteValues -> helper.consumeDiscreteValues(discreteValues),
                allOrNone -> helper.consumeAllOrNone(allOrNone));
        helper.restrict(retentionTime);
        return helper.get(() -> getColumnPredicate(Domain.create(values, false), BIGINT, timeColumn.toLowerCase(ENGLISH)));
    }

    private static long objectToTimeLiteral(Object literal, boolean isLow)
    {
        if (!(literal instanceof Number)) {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Expected the tuple domain bound %s to be a number when extracting time bounds", literal));
        }
        double num = ((Number) literal).doubleValue();
        return (long) (isLow ? Math.floor(num) : Math.ceil(num));
    }

    private static ISOChronology getChronology(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return ISOChronology.getInstanceUTC();
        }
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        DateTimeZone dateTimeZone = DateTimeZone.forID(timeZoneKey.getId());
        return ISOChronology.getInstance(dateTimeZone);
    }

    // Stolen from com.facebook.presto.operator.scalar.DateTimeFunctions.localTime
    public static long getSessionStartTimeInMs(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return session.getStartTime();
        }
        else {
            return getChronology(session).getZone().convertUTCToLocal(session.getStartTime());
        }
    }

    private static Optional<TimeFilterParsed> retentionTimeFilter(Optional<ConnectorSession> session, AresDbTableHandle tableHandle)
    {
        Optional<Type> timeStampType = tableHandle.getTimeStampType();
        Optional<String> timeColumnName = tableHandle.getTimeColumnName();
        Optional<Duration> retention = tableHandle.getRetention();
        if (!timeColumnName.isPresent() || !retention.isPresent() || !timeStampType.isPresent() || !session.isPresent()) {
            return Optional.empty();
        }
        long sessionStartTimeInMs = getSessionStartTimeInMs(session.get());
        long retentionInstantInMs = getChronology(session.get()).seconds().subtract(sessionStartTimeInMs, retention.get().roundTo(TimeUnit.SECONDS));
        long lowTimeSeconds = retentionInstantInMs / 1000; // round down
        long highTimeSeconds = (sessionStartTimeInMs + 999) / 1000; // round up
        return Optional.of(new TimeFilterParsed(timeColumnName.get(), lowTimeSeconds, highTimeSeconds, Optional.empty()));
    }

    public AresDbQueryGeneratorContext withJoin(AresDbQueryGeneratorContext otherContext, String otherTableName, List<JoinPipelineNode.EquiJoinClause> criterias)
    {
        List<String> joinCriterias = criterias.stream().map(criteria -> {
            AresDbExpressionConverter.AresDbExpression left = criteria.getLeft().accept(new AresDbExpressionConverter(), selections);
            AresDbExpressionConverter.AresDbExpression right = criteria.getRight().accept(new AresDbExpressionConverter(), otherContext.selections);
            return format("%s = %s", left.getDefinition(), right.getDefinition());
        }).collect(toImmutableList());
        JoinInfo newJoinInfo = new JoinInfo(otherTableName, otherTableName, joinCriterias);
        AresDbQueryGeneratorContext ret = new AresDbQueryGeneratorContext(selections, tableHandle, timeFilter, filters, aggregationApplied, groupByColumns, limit, ImmutableList.<JoinInfo>builder().addAll(joins).add(newJoinInfo).build());
        if (otherContext.hasAggregation() || !otherContext.groupByColumns.isEmpty() || otherContext.hasLimit()) {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "Can only join with a right side that does not have limits nor aggregations");
        }
        ret = ret.withFilters(otherContext.filters, otherContext.timeFilter);
        LinkedHashMap<String, Selection> originalLeftSelections = ret.selections;
        LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>(originalLeftSelections);
        otherContext.selections.forEach((otherSymbol, otherSelection) -> {
            if (newSelections.containsKey(otherSymbol)) {
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Found duplicate selection b/w left and right: %s. Left is %s, Right is %s", otherSymbol, originalLeftSelections, otherContext.selections));
            }
            newSelections.put(otherSymbol, otherSelection);
        });
        ret = ret.withProject(newSelections);
        return ret;
    }

    public boolean isInputTooBig(Optional<ConnectorSession> session)
    {
        Optional<TimeFilterParsed> timeFilterParsed = getParsedTimeFilter(session);
        if (!timeFilterParsed.isPresent() || !session.isPresent()) {
            // Assume global tables without retention are small
            return false;
        }
        Optional<Duration> singleSplitLimit = AresDbSessionProperties.getSingleSplitLimit(session.get());
        return singleSplitLimit.map(limit -> timeFilterParsed.get().getBoundInSeconds() > limit.roundTo(TimeUnit.SECONDS)).orElse(false);
    }

    public enum Origin
    {
        TABLE,
        LITERAL,
        DERIVED,
    }

    public static class Selection
    {
        private String definition;
        private Origin origin;
        private Type outputType;
        private Optional<TimeSpec> timeTokenizer;

        private Selection(String definition, Origin origin, Type outputType, Optional<TimeSpec> timeTokenizer)
        {
            this.definition = definition;
            this.origin = origin;
            this.outputType = outputType;
            this.timeTokenizer = timeTokenizer;
        }

        public static Selection of(String definition, Origin origin, Type outputType)
        {
            return new Selection(definition, origin, outputType, Optional.empty());
        }

        public static Selection of(String definition, Origin origin, Type outputType, Optional<TimeSpec> timeTokenizer)
        {
            return new Selection(definition, origin, outputType, timeTokenizer);
        }

        public String getDefinition()
        {
            return definition;
        }

        public Origin getOrigin()
        {
            return origin;
        }

        public Type getOutputType()
        {
            return outputType;
        }

        public Optional<TimeSpec> getTimeTokenizer()
        {
            return timeTokenizer;
        }
    }

    public static class JoinInfo
    {
        private final String alias;
        private final String name;
        private final List<String> conditions;

        public JoinInfo(String alias, String name, List<String> conditions)
        {
            this.alias = alias;
            this.name = name;
            this.conditions = conditions;
        }
    }

    public static class AresDbOutputInfo
    {
        public final int index;
        public final Optional<TimeSpec> timeBucketizer;

        public AresDbOutputInfo(int index, Optional<TimeSpec> timeBucketizer)
        {
            this.index = index;
            this.timeBucketizer = timeBucketizer;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (!(o instanceof AresDbOutputInfo)) {
                return false;
            }

            AresDbOutputInfo that = (AresDbOutputInfo) o;
            return index == that.index && Objects.equals(timeBucketizer, that.timeBucketizer);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(index, timeBucketizer);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("index", index)
                    .add("timeBucketizer", timeBucketizer)
                    .toString();
        }
    }
}
