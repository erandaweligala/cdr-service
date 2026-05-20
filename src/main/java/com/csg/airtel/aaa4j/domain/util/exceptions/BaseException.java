package com.csg.airtel.aaa4j.domain.util.exceptions;

import org.apache.http.HttpStatus;

public class BaseException extends RuntimeException {
    private final String reason;
    private final int httpStatus;
    private final String resultCode;

    public BaseException(String message, String reason, int httpStatus, String resultCode) {
        super(message);
        this.reason = reason;
        this.httpStatus = httpStatus;
        this.resultCode = resultCode;
    }

    public String getReason() {
        return reason;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public String getResultCode() {
        return resultCode;
    }
}
