package com.csg.airtel.aaa4j.domain.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
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
    void createClient_withoutAuthentication() {
        // given (no username → no auth branch)
        producer.serverHost = "localhost";
        producer.serverPort = 9200;
        producer.protocol = "http";
        producer.userName = "";
        producer.password = "";

        // when
        ElasticsearchClient client = producer.createClient();

        // then
        assertNotNull(client);
    }

    @Test
    void createClient_withAuthentication() {
        // given (username provided → auth branch)
        producer.serverHost = "localhost";
        producer.serverPort = 9200;
        producer.protocol = "http";
        producer.userName = "elastic";
        producer.password = "password";

        // when
        ElasticsearchClient client = producer.createClient();

        // then
        assertNotNull(client);
    }

    @Test
    void createClient_withCustomHostAndPort() {
        // given
        producer.serverHost = "127.0.0.1";
        producer.serverPort = 9201;
        producer.protocol = "http";
        producer.userName = "";
        producer.password = "";

        // when
        ElasticsearchClient client = producer.createClient();

        // then
        assertNotNull(client);
    }

    @Test
    void createClient_withNullUsername_treatedAsNoAuth() {
        // given
        producer.serverHost = "localhost";
        producer.serverPort = 9200;
        producer.protocol = "http";
        producer.userName = null; // important edge case
        producer.password = null;

        // when
        ElasticsearchClient client = producer.createClient();

        // then
        assertNotNull(client);
    }
}
