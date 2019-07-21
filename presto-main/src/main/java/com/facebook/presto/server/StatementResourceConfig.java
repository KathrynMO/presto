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

package com.facebook.presto.server;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.Nonnull;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.base.Strings.isNullOrEmpty;

public class StatementResourceConfig
{
    private static final List<String> DEFAULT_HEADERS_FOR_USER_FIELD = ImmutableList.of(PRESTO_USER);
    private List<String> headersForUserField = DEFAULT_HEADERS_FOR_USER_FIELD;
    private Optional<Duration> defaultWaitForEntireResponseIntervalMs = Optional.empty();

    @Nonnull
    public List<String> getHeadersForUser()
    {
        return headersForUserField;
    }

    @Config("statement.headers-for-user")
    public StatementResourceConfig setHeadersForUser(String alternateHeadersForUser)
    {
        Set<String> userHeaders = new HashSet<>(DEFAULT_HEADERS_FOR_USER_FIELD);
        if (!isNullOrEmpty(alternateHeadersForUser)) {
            userHeaders.addAll(Splitter.on(",").omitEmptyStrings().trimResults().splitToList(alternateHeadersForUser));
        }
        headersForUserField = ImmutableList.copyOf(userHeaders);
        return this;
    }

    @Nonnull
    public Optional<Duration> getDefaultWaitForEntireResponseIntervalMs()
    {
        return defaultWaitForEntireResponseIntervalMs;
    }

    @Config("statement.default-wait-for-entire-response-interval")
    @MinDuration("0s")
    public StatementResourceConfig setDefaultWaitForEntireResponseIntervalMs(Duration defaultWaitForEntireResponseIntervalMs)
    {
        this.defaultWaitForEntireResponseIntervalMs = Optional.ofNullable(defaultWaitForEntireResponseIntervalMs);
        return this;
    }
}
