package com.csg.airtel.aaa4j.domain.client;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ElasticsearchProducerTest {

    private ElasticsearchProducer producer;

    @BeforeEach
    void setUp() {
        producer = new ElasticsearchProducer();
    }

    @Test
    void createAsyncClient_withoutAuthentication() {
        producer.serverHost = "localhost";
        producer.serverPort = 9200;
        producer.protocol = "http";
        producer.userName = "";
        producer.password = "";

        ElasticsearchAsyncClient client = producer.createAsyncClient();

        assertNotNull(client);
    }

    @Test
    void createAsyncClient_withAuthentication() {
        producer.serverHost = "localhost";
        producer.serverPort = 9200;
        producer.protocol = "http";
        producer.userName = "elastic";
        producer.password = "password";

        ElasticsearchAsyncClient client = producer.createAsyncClient();

        assertNotNull(client);
    }

    @Test
    void createAsyncClient_withCustomHostAndPort() {
        producer.serverHost = "127.0.0.1";
        producer.serverPort = 9201;
        producer.protocol = "http";
        producer.userName = "";
        producer.password = "";

        ElasticsearchAsyncClient client = producer.createAsyncClient();

        assertNotNull(client);
    }

    @Test
    void createAsyncClient_withNullUsername_treatedAsNoAuth() {
        producer.serverHost = "localhost";
        producer.serverPort = 9200;
        producer.protocol = "http";
        producer.userName = null;
        producer.password = null;

        ElasticsearchAsyncClient client = producer.createAsyncClient();

        assertNotNull(client);
    }
}
