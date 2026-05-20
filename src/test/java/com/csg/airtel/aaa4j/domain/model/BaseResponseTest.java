package com.csg.airtel.aaa4j.domain.model;

import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BaseResponseTest {

    @Test
    void constructorShouldSetAllFields() {
        // given
        String status = "200";
        String message = "OK";

        List<String> data = Arrays.asList("A", "B");

        PageDetails details = new PageDetails(
                50L,   // totalRecords
                2,     // pageNumber
                10     // pageElementCount
        );

        // when
        BaseResponse<String> response =
                new BaseResponse<>(status, message, data, details);

        // then
        assertEquals(status, response.getStatus());
        assertEquals(message, response.getMessage());
        assertEquals(data, response.getData());
        assertEquals(details, response.getPageDetails());
    }

    @Test
    void successFactoryShouldProduceValidResponse() {
        // given
        String message = "Success";
        List<Integer> data = Arrays.asList(100, 200);
        PageDetails pageDetails = new PageDetails(100L, 1, 5);

        // when
        BaseResponse<Integer> response =
                BaseResponse.success(message, data, pageDetails);

        // then
        assertEquals(ResponseCodeEnum.SUCCESSFUL.code(), response.getStatus());
        assertEquals(message, response.getMessage());
        assertEquals(data, response.getData());
        assertEquals(pageDetails, response.getPageDetails());
    }

    @Test
    void errorFactoryShouldProduceValidErrorResponse() {
        // given
        String status = "ERR-400";
        String message = "Bad Request";

        // when
        BaseResponse<Object> response = BaseResponse.error(status, message);

        // then
        assertEquals(status, response.getStatus());
        assertEquals(message, response.getMessage());
        assertNull(response.getData());
        assertNull(response.getPageDetails());
    }

    @Test
    void settersShouldUpdateFieldsCorrectly() {
        // given
        BaseResponse<String> response =
                new BaseResponse<>("INIT", "Initial", null, null);

        // when
        response.setStatus("UPDATED");
        response.setMessage("Updated message");
        response.setData(Arrays.asList("X", "Y"));

        PageDetails pageDetails = new PageDetails(500L, 5, 20);
        response.setPageDetails(pageDetails);

        // then
        assertEquals("UPDATED", response.getStatus());
        assertEquals("Updated message", response.getMessage());
        assertEquals(Arrays.asList("X", "Y"), response.getData());
        assertEquals(pageDetails, response.getPageDetails());
    }
}
