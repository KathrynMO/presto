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
import com.facebook.presto.aresdb.query.TimeSpec;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestAresdbSplit
{
    private final JsonCodec<AresDbSplit> codec = JsonCodec.jsonCodec(AresDbSplit.class);

    @Test
    public void testJsonRoundTrip()
    {
        TestingConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        AresDbSplit expected = new AresDbSplit(new AresDbConnectorId("nothing"), ImmutableList.of(new AQLExpression("foo", Optional.of(new TimeSpec("bar", session.getTimeZoneKey())))), ImmutableList.of(new AresDbSplit.AresQL("{}", false)), 0);

        String json = codec.toJson(expected);
        AresDbSplit actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getExpressions(), expected.getExpressions());
        assertEquals(actual.getAddresses(), expected.getAddresses());
        assertEquals(actual.getAqls(), expected.getAqls());
        assertEquals(actual.getIndex(), expected.getIndex());
    }
}
