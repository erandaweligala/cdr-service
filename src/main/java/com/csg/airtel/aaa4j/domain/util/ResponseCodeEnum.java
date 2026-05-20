package com.csg.airtel.aaa4j.domain.util;

public enum ResponseCodeEnum {
    SUCCESSFUL("00","SUCCESSFUL"),

    EXCEPTION_SERVICE_LAYER("96", "EXCEPTION IN SERVICE LAYER"),
    EXCEPTION_REPOSITORY_LAYER("98","EXCEPTION IN REPOSITORY LAYER"),
    INTERNAL_SERVER_ERROR("99", "INTERNAL SERVER ERROR"),
    EXCEPTION_ELASTIC_CLIENT("98", "EXCEPTION IN ELASTICSEARCH CLIENT"),
    EXCEPTION_MAPPER_LAYER("97", "EXCEPTION IN MAPPER"),

    BAD_REQUEST("32","BAD_REQUEST"),
    SESSION_NOT_FOUND("33","No sessions found for the subscriber"),
    SESSION_INSTANCE_NOT_FOUND("34","No session instances found for the session ID"),


;
    private final String code;
    private final String description;
    ResponseCodeEnum(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String code() {
        return code;
    }
    public String description() {
        return description;
    }

}
