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

package com.facebook.presto.rta;

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class RtaMetrics
{
    RtaMetricsStat definitionMetric = new RtaMetricsStat(true);
    RtaMetricsStat deploymentsMetric = new RtaMetricsStat(true);
    RtaMetricsStat tablesMetric = new RtaMetricsStat(true);
    RtaMetricsStat namespacesMetric = new RtaMetricsStat(true);

    @Managed
    @Nested
    public RtaMetricsStat getDefinitionMetric()
    {
        return definitionMetric;
    }

    @Managed
    @Nested
    public RtaMetricsStat getDeploymentsMetric()
    {
        return deploymentsMetric;
    }

    @Managed
    @Nested
    public RtaMetricsStat getTablesMetric()
    {
        return tablesMetric;
    }

    @Managed
    @Nested
    public RtaMetricsStat getNamespacesMetric()
    {
        return namespacesMetric;
    }
}
