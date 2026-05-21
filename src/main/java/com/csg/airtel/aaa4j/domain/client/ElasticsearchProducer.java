package com.csg.airtel.aaa4j.domain.client;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

@ApplicationScoped
public class ElasticsearchProducer {

    @ConfigProperty(name = "elasticsearch.host", defaultValue = "localhost")
    String serverHost;

    @ConfigProperty(name = "elasticsearch.port", defaultValue = "9200")
    int serverPort;

    @ConfigProperty(name = "elasticsearch.protocol", defaultValue = "http")
    String protocol;

    @ConfigProperty(name = "elasticsearch.username", defaultValue = "")
    String userName;

    @ConfigProperty(name = "elasticsearch.password", defaultValue = "")
    String password;

    @ConfigProperty(name = "elasticsearch.pool.max-conn-total", defaultValue = "400")
    int maxConnTotal;

    @ConfigProperty(name = "elasticsearch.pool.max-conn-per-route", defaultValue = "200")
    int maxConnPerRoute;

    @ConfigProperty(name = "elasticsearch.pool.io-thread-count", defaultValue = "8")
    int ioThreadCount;

    @ConfigProperty(name = "elasticsearch.pool.connect-timeout-ms", defaultValue = "5000")
    int connectTimeoutMs;

    @ConfigProperty(name = "elasticsearch.pool.socket-timeout-ms", defaultValue = "60000")
    int socketTimeoutMs;

    @ConfigProperty(name = "elasticsearch.pool.connection-request-timeout-ms", defaultValue = "5000")
    int connectionRequestTimeoutMs;

    @Produces
    @Singleton
    public ElasticsearchAsyncClient createAsyncClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost(serverHost, serverPort, protocol));

        IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setIoThreadCount(ioThreadCount)
                .setSoKeepAlive(true)
                .build();

        boolean useAuth = userName != null && !userName.isEmpty();
        BasicCredentialsProvider credProvider = null;
        if (useAuth) {
            credProvider = new BasicCredentialsProvider();
            credProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(userName, password)
            );
        }

        final BasicCredentialsProvider finalCredProvider = credProvider;
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder
                    .setMaxConnTotal(maxConnTotal)
                    .setMaxConnPerRoute(maxConnPerRoute)
                    .setDefaultIOReactorConfig(ioReactorConfig);
            if (finalCredProvider != null) {
                httpClientBuilder.setDefaultCredentialsProvider(finalCredProvider);
            }
            return httpClientBuilder;
        });

        builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                .setConnectTimeout(connectTimeoutMs)
                .setSocketTimeout(socketTimeoutMs)
                .setConnectionRequestTimeout(connectionRequestTimeoutMs));

        RestClient restClient = builder.build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchAsyncClient(transport);
    }
}
