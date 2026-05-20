package com.csg.airtel.aaa4j.domain.util.exceptions;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.http.HttpStatus;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
public class ServiceExceptionHandler {

    public BaseException serviceLayerExceptionHandler(Exception ex) throws BaseException {
        if (ex instanceof ExecutionException || ex instanceof CompletionException) {
            if (ex.getCause() instanceof BaseException) {
                BaseException bxe = (BaseException) ex.getCause();
                throw new BaseException(ex.getMessage(), bxe.getReason(), bxe.getHttpStatus(), bxe.getResultCode());
            } else
                throw new BaseException(ex.getMessage(), ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(), HttpStatus.SC_INTERNAL_SERVER_ERROR, ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code());
        } else
            throw new BaseException(ex.getMessage(), ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(), HttpStatus.SC_INTERNAL_SERVER_ERROR, ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code());
    }

    public BaseException elasticsearchExceptionHandler(Exception ex) throws BaseException {
        if (ex instanceof ElasticsearchException)
            throw new BaseException(ex.getMessage(), ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.description(), HttpStatus.SC_SERVICE_UNAVAILABLE, ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.code());

        throw new BaseException(
                ex.getMessage(),
                ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.description(),
                HttpStatus.SC_INTERNAL_SERVER_ERROR,
                ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.code()
        );

    }

}
