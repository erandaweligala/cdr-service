package com.csg.airtel.aaa4j.domain.util.exceptions;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.http.HttpStatus;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
public class ServiceExceptionHandler {

    @Inject
    Instance<ExceptionMetricsService> metrics;

    public BaseException serviceLayerExceptionHandler(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException || ex instanceof ExecutionException)
                ? ex.getCause() : ex;

        recordMetric(cause != null ? cause : ex,
                ExceptionMetricsService.Layer.SERVICE,
                ExceptionMetricsService.Source.INTERNAL);

        if (cause instanceof BaseException be) {
            return be;
        }

        if (cause instanceof ElasticsearchException) {
            return elasticsearchExceptionHandler(cause);
        }

        return new BaseException(
                cause != null ? cause.getMessage() : ex.getMessage(),
                ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(),
                HttpStatus.SC_INTERNAL_SERVER_ERROR,
                ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code()
        );
    }

    public BaseException elasticsearchExceptionHandler(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException || ex instanceof ExecutionException)
                ? ex.getCause() : ex;

        recordMetric(cause != null ? cause : ex,
                ExceptionMetricsService.Layer.CLIENT,
                ExceptionMetricsService.Source.ELASTICSEARCH);

        if (cause instanceof BaseException be) {
            return be;
        }

        if (cause instanceof ElasticsearchException) {
            return new BaseException(
                    cause.getMessage(),
                    ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.description(),
                    HttpStatus.SC_SERVICE_UNAVAILABLE,
                    ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.code()
            );
        }

        return new BaseException(
                cause != null ? cause.getMessage() : ex.getMessage(),
                ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.description(),
                HttpStatus.SC_INTERNAL_SERVER_ERROR,
                ResponseCodeEnum.EXCEPTION_ELASTIC_CLIENT.code()
        );
    }

    private void recordMetric(Throwable t,
                              ExceptionMetricsService.Layer layer,
                              ExceptionMetricsService.Source source) {
        if (metrics == null || metrics.isUnsatisfied()) {
            return;
        }
        metrics.get().recordException(t, layer, source);
    }
}
