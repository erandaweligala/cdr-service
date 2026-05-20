package com.csg.airtel.aaa4j.domain.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.elasticsearch.client.RestClient;

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

    @Produces
    public ElasticsearchClient createClient() {
        RestClient restClient;

        // Only use credentials if username is provided
        if (userName != null && !userName.isEmpty()) {
            BasicCredentialsProvider credProvider = new BasicCredentialsProvider();
            credProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(userName, password)
            );

            restClient = RestClient.builder(
                    new HttpHost(serverHost, serverPort, protocol)
            ).setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credProvider)
            ).build();
        } else {
            // No authentication for local Elasticsearch
            restClient = RestClient.builder(
                    new HttpHost(serverHost, serverPort, protocol)
            ).build();
        }

        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }
}