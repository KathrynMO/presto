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
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.ValueSet;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Optional;

import static com.facebook.presto.spi.pipeline.PushDownUtils.getColumnPredicate;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestPushDownUtils
{
    @Test
    public void testSingleValueRanges()
    {
        Domain domain = com.facebook.presto.spi.predicate.Domain.multipleValues(BIGINT, new ArrayList<>(asList(1L, 10L)));

        assertEquals(getColumnPredicate(domain, BIGINT, "city_id").get().toString(), "city_id IN (1, 10)");
        assertEquals(getColumnPredicate(domain, BIGINT, "city_id_123").get().toString(), "city_id_123 IN (1, 10)");

        Domain domainSingleValue = com.facebook.presto.spi.predicate.Domain.multipleValues(BIGINT, new ArrayList<>(asList(1L)));

        assertEquals(getColumnPredicate(domainSingleValue, BIGINT, "city_id").get().toString(), "(city_id = 1)");
        assertEquals(getColumnPredicate(domainSingleValue, BIGINT, "city_id_123").get().toString(), "(city_id_123 = 1)");
    }

    @Test
    public void testRangeValues()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.greaterThan(BIGINT, 1L).intersect(Range.lessThan(BIGINT, 10L))), false);

        String expectedFilter = "((1 < city_id) AND (city_id < 10))";
        assertEquals(getColumnPredicate(domain, BIGINT, "city_id").get().toString(), expectedFilter);
    }

    @Test
    public void testOneSideRanges()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.lessThanOrEqual(BIGINT, 10L)), false);

        String expectedFilter = "(city_id <= 10)";
        assertEquals(getColumnPredicate(domain, BIGINT, "city_id").get().toString(), expectedFilter);
    }

    @Test
    public void testMultipleRanges()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.equal(BIGINT, 20L),
                Range.greaterThan(BIGINT, 1L).intersect(Range.lessThan(BIGINT, 10L)),
                Range.greaterThan(BIGINT, 12L).intersect(Range.lessThan(BIGINT, 18L))), false);

        String expectedFilter = "(((1 < city_id) AND (city_id < 10)) OR (((12 < city_id) AND (city_id < 18)) OR (city_id = 20)))";
        assertEquals(getColumnPredicate(domain, BIGINT, "city_id").get().toString(), expectedFilter);
    }

    @Test
    public void testEmptyDomain()
    {
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, new ArrayList<>());
        Domain domain = Domain.create(sortedRangeSet, false);

        assertEquals(getColumnPredicate(domain, BIGINT, "city_id"), Optional.empty());
    }
}
