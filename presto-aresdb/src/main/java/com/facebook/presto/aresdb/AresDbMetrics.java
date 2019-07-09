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

import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.TimeUnit;

@ThreadSafe
public class AresDbMetrics
{
    // TODO: Is there a way I can have a map<String, Stat> and have that be properly exposed still via jmx with keys as names ?
    private final AresDbMetricsStat queryStats = new AresDbMetricsStat(true);

    @Managed
    @Nested
    public AresDbMetricsStat getQueryStats()
    {
        return queryStats;
    }

    public void monitorQueryRequest(Request request, StringResponseHandler.StringResponse response, long duration, TimeUnit timeUnit)
    {
        queryStats.record(request, response, duration, timeUnit);
    }
}
