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

package com.facebook.presto.rta.schema;

import com.facebook.presto.rta.RtaConfig;
import com.facebook.presto.rta.RtaErrorCode;
import com.facebook.presto.rta.RtaException;
import com.facebook.presto.rta.RtaMetrics;
import com.facebook.presto.rta.RtaMetricsStat;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;

/**
 * This class communicates with the rta-ums (muttley)/rtaums (udeploy) service. It's api is available here:
 * https://rtaums.uberinternal.com/api/
 */
public class RTAMSClient
{
    private static final String MUTTLEY_RPC_CALLER_HEADER = "Rpc-Caller";
    private static final String MUTTLEY_RPC_CALLER_VALUE = "presto";
    private static final String MUTTLEY_RPC_SERVICE_HEADER = "Rpc-Service";
    private static final String APPLICATION_JSON = "application/json";
    private static final ObjectMapper mapper = new ObjectMapper();
    private final Ticker ticker = Ticker.systemTicker();
    private final Optional<RtaMetrics> rtaMetrics;

    public static boolean isValidResponseCode(int statusCode)
    {
        return statusCode >= 200 && statusCode < 300;
    }

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private HttpClient httpClient;
    private final String muttleyService;

    private RTAMSClient(HttpClient httpClient, String muttleyService, Optional<RtaMetrics> rtaMetrics)
    {
        this.muttleyService = muttleyService;
        this.httpClient = httpClient;
        this.rtaMetrics = rtaMetrics;
    }

    public List<RTADefinition> getExtraDefinitions(String extraDefinitionFiles)
            throws IOException
    {
        ImmutableList.Builder<RTADefinition> definitions = ImmutableList.builder();
        for (String definitionFile : Splitter.on(",").trimResults().omitEmptyStrings().splitToList(extraDefinitionFiles)) {
            RTADefinition definition = mapper.readValue(Files.readAllBytes(Paths.get(definitionFile)), RTADefinition.class);
            definitions.add(definition);
        }
        return definitions.build();
    }

    public List<List<RTADeployment>> getExtraDeployments(String extraDeploymentFiles)
            throws IOException
    {
        ImmutableList.Builder<List<RTADeployment>> deployments = ImmutableList.builder();
        for (String deploymentFile : Splitter.on(",").trimResults().omitEmptyStrings().splitToList(extraDeploymentFiles)) {
            List<RTADeployment> deployment = mapper.readValue(Files.readAllBytes(Paths.get(deploymentFile)), new TypeReference<List<RTADeployment>>() {});
            deployments.add(deployment);
        }
        return deployments.build();
    }

    public RTAMSClient(HttpClient httpClient, String rtaMsService)
    {
        this(httpClient, rtaMsService, Optional.empty());
    }

    @Inject
    public RTAMSClient(@ForRTAMS HttpClient httpClient, RtaConfig rtaConfig, RtaMetrics rtaMetrics)
    {
        this(httpClient, rtaConfig.getRtaUmsService(), Optional.of(rtaMetrics));
    }

    Request.Builder getBaseRequest()
    {
        return Request.builder().prepareGet().setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .setHeader(MUTTLEY_RPC_CALLER_HEADER, MUTTLEY_RPC_CALLER_VALUE)
                .setHeader(MUTTLEY_RPC_SERVICE_HEADER, muttleyService);
    }

    @FunctionalInterface
    private static interface ResponseHandler<T>
    {
        T call(String body)
                throws IOException;
    }

    private <T> T callGet(Optional<RtaMetricsStat> metricsStat, URI uri, ResponseHandler<T> handler)
            throws IOException
    {
        Request request = getBaseRequest().setUri(uri).build();
        long startTime = ticker.read();
        long duration;
        StringResponseHandler.StringResponse response;
        try {
            response = httpClient.execute(request, createStringResponseHandler());
        }
        finally {
            duration = ticker.read() - startTime;
        }
        String responseBody = response.getBody();
        metricsStat.ifPresent(p -> p.record(request, response, duration, TimeUnit.NANOSECONDS));
        if (isValidResponseCode(response.getStatusCode())) {
            return handler.call(responseBody);
        }
        else {
            throw new RtaException(RtaErrorCode.RTAMS_ERROR,
                    String.format("Unexpected response status: %d for request GET to url %s, with headers %s, full response %s", response.getStatusCode(), request.getUri(), request.getHeaders(), responseBody));
        }
    }

    public List<String> getNamespaces()
            throws IOException
    {
        return callGet(rtaMetrics.map(RtaMetrics::getNamespacesMetric), URI.create(RTAMSEndpoints.getNamespaces()), response -> Arrays.asList(mapper.readValue(response, String[].class)));
    }

    public List<String> getTables(String namespace)
            throws IOException
    {
        return callGet(rtaMetrics.map(RtaMetrics::getTablesMetric), URI.create(RTAMSEndpoints.getTablesFromNamespace(namespace)), response -> Arrays.asList(mapper.readValue(response, String[].class)));
    }

    public RTADefinition getDefinition(final String namespace, final String tableName)
            throws IOException
    {
        return callGet(rtaMetrics.map(RtaMetrics::getDefinitionMetric), URI.create(RTAMSEndpoints.getTableSchema(namespace, tableName)), response -> mapper.readValue(response, RTADefinition.class));
    }

    public List<RTADeployment> getDeployments(final String namespace, final String tableName)
            throws IOException
    {
        return callGet(rtaMetrics.map(RtaMetrics::getDeploymentsMetric), URI.create(RTAMSEndpoints.getDeployment(namespace, tableName)), response -> mapper.readValue(response, new TypeReference<List<RTADeployment>>() {}));
    }
}
