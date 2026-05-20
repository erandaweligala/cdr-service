package com.csg.airtel.aaa4j.domain.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ResponseCodeEnumTest {

    @Test
    void eachEnumConstant_hasExpectedCodeAndDescription() {
        // SUCCESSFUL
        assertEquals("00", ResponseCodeEnum.SUCCESSFUL.code());
        assertEquals("SUCCESSFUL", ResponseCodeEnum.SUCCESSFUL.description());

        // EXCEPTION_SERVICE_LAYER
        assertEquals("96", ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code());
        assertEquals("EXCEPTION IN SERVICE LAYER", ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description());

        // EXCEPTION_REPOSITORY_LAYER
        assertEquals("98", ResponseCodeEnum.EXCEPTION_REPOSITORY_LAYER.code());
        assertEquals("EXCEPTION IN REPOSITORY LAYER", ResponseCodeEnum.EXCEPTION_REPOSITORY_LAYER.description());

        // INTERNAL_SERVER_ERROR
        assertEquals("99", ResponseCodeEnum.INTERNAL_SERVER_ERROR.code());
        assertEquals("INTERNAL SERVER ERROR", ResponseCodeEnum.INTERNAL_SERVER_ERROR.description());

        // EXCEPTION_ELASTIC_CLIENT
        assertEquals("98", ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.code());
        assertEquals("EXCEPTION IN ELASTICSEARCH CLIENT", ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.description());

        // EXCEPTION_MAPPER_LAYER
        assertEquals("97", ResponseCodeEnum.EXCEPTION_MAPPER_LAYER.code());
        assertEquals("EXCEPTION IN MAPPER", ResponseCodeEnum.EXCEPTION_MAPPER_LAYER.description());

        // BAD_REQUEST
        assertEquals("32", ResponseCodeEnum.BAD_REQUEST.code());
        assertEquals("BAD_REQUEST", ResponseCodeEnum.BAD_REQUEST.description());

        // SESSION_NOT_FOUND
        assertEquals("33", ResponseCodeEnum.SESSION_NOT_FOUND.code());
        assertEquals("No sessions found for the subscriber", ResponseCodeEnum.SESSION_NOT_FOUND.description());

        // SESSION_INSTANCE_NOT_FOUND
        assertEquals("34", ResponseCodeEnum.SESSION_INSTANCE_NOT_FOUND.code());
        assertEquals("No session instances found for the session ID", ResponseCodeEnum.SESSION_INSTANCE_NOT_FOUND.description());
    }

    @Test
    void values_containsAllConstants_andValueOfWorks() {
        ResponseCodeEnum[] values = ResponseCodeEnum.values();

        // basic sanity check on number of constants
        assertEquals(9, values.length);

        // ensure valueOf can resolve each constant name
        for (ResponseCodeEnum value : values) {
            assertSame(value, ResponseCodeEnum.valueOf(value.name()));
        }
    }
}
