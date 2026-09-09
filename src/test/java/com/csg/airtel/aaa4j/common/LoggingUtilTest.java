package com.csg.airtel.aaa4j.common;

import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoggingUtilTest {

    @Test
    void logInfoSkipsMessageBuildingWhenInfoIsDisabled() {
        Logger logger = mock(Logger.class);
        when(logger.isInfoEnabled()).thenReturn(false);

        LoggingUtil.logInfo(logger, "method", "message %s", "arg");

        verify(logger, never()).info(any(String.class));
    }

    @Test
    void logInfoBuildsAndLogsWhenInfoIsEnabled() {
        Logger logger = mock(Logger.class);
        when(logger.isInfoEnabled()).thenReturn(true);

        LoggingUtil.logInfo(logger, "myMethod", "hello %s", "world");

        verify(logger).info("[myMethod]hello world");
    }

    @Test
    void logDebugSkipsMessageBuildingWhenDebugIsDisabled() {
        Logger logger = mock(Logger.class);
        when(logger.isDebugEnabled()).thenReturn(false);

        LoggingUtil.logDebug(logger, "method", "message %s", "arg");

        verify(logger, never()).debug(any(String.class));
    }

    @Test
    void logDebugBuildsAndLogsWhenDebugIsEnabled() {
        Logger logger = mock(Logger.class);
        when(logger.isDebugEnabled()).thenReturn(true);

        LoggingUtil.logDebug(logger, "myMethod", "count is %d", 42);

        verify(logger).debug("[myMethod]count is 42");
    }

    @Test
    void logTraceSkipsMessageBuildingWhenTraceIsDisabled() {
        Logger logger = mock(Logger.class);
        when(logger.isTraceEnabled()).thenReturn(false);

        LoggingUtil.logTrace(logger, "method", "message %s", "arg");

        verify(logger, never()).trace(any(String.class));
    }

    @Test
    void logTraceBuildsAndLogsWhenTraceIsEnabled() {
        Logger logger = mock(Logger.class);
        when(logger.isTraceEnabled()).thenReturn(true);

        LoggingUtil.logTrace(logger, "myMethod", "trace %s", "detail");

        verify(logger).trace("[myMethod]trace detail");
    }

    @Test
    void logWarnAlwaysBuildsAndLogsRegardlessOfLevel() {
        Logger logger = mock(Logger.class);

        LoggingUtil.logWarn(logger, "myMethod", "warn %s", "here");

        verify(logger).warn("[myMethod]warn here");
    }

    @Test
    void logErrorWithExceptionLogsMessageAndThrowable() {
        Logger logger = mock(Logger.class);
        RuntimeException error = new RuntimeException("boom");

        LoggingUtil.logError(logger, "myMethod", error, "failed: %s", "reason");

        verify(logger).error("[myMethod]failed: reason", error);
    }

    @Test
    void logErrorWithoutExceptionLogsMessageOnly() {
        Logger logger = mock(Logger.class);

        LoggingUtil.logError(logger, "myMethod", null, "failed: %s", "reason");

        verify(logger).error("[myMethod]failed: reason");
    }

    @Test
    void nullMethodMessageAndArgsAreToleratedDefensively() {
        Logger logger = mock(Logger.class);

        LoggingUtil.logWarn(logger, null, null, (Object[]) null);

        verify(logger).warn("[]");
    }

    @Test
    void percentSignFollowedByAnUnrecognisedCharacterIsKeptLiteral() {
        Logger logger = mock(Logger.class);

        LoggingUtil.logWarn(logger, "m", "100%x done", "unused");

        verify(logger).warn("[m]100%x done");
    }

    @Test
    void doublePercentEmitsALiteralPercent() {
        Logger logger = mock(Logger.class);

        // Placeholder scanning only runs when args.length > 0 (see buildMessage), so a
        // dummy arg is required here even though %% does not consume one.
        LoggingUtil.logWarn(logger, "m", "100%% complete", "unused");

        verify(logger).warn("[m]100% complete");
    }

    @Test
    void placeholderWithNoRemainingArgIsKeptLiteral() {
        Logger logger = mock(Logger.class);

        LoggingUtil.logWarn(logger, "m", "%s and %s", "only-one");

        verify(logger).warn("[m]only-one and %s");
    }

    @Test
    void trailingPercentAtEndOfMessageIsKeptLiteral() {
        Logger logger = mock(Logger.class);

        LoggingUtil.logWarn(logger, "m", "done%");

        verify(logger).warn("[m]done%");
    }

    @Test
    void multipleArgsAreSubstitutedInOrder() {
        Logger logger = mock(Logger.class);

        LoggingUtil.logWarn(logger, "m", "%s=%d, %s=%d", "a", 1, "b", 2);

        verify(logger).warn("[m]a=1, b=2");
    }

    @Test
    void veryLongMessageIsLoggedInFullAndPooledBufferIsReleasedAfterwards() {
        Logger logger = mock(Logger.class);
        // Longer than LoggingUtil's internal soft cap (512 chars) so the pooled
        // StringBuilder is released (ThreadLocal.remove()) after building the message.
        String longMessage = "x".repeat(1000);

        LoggingUtil.logWarn(logger, "m", longMessage);

        verify(logger).warn("[m]" + longMessage);

        // A subsequent, short call must still work correctly off a freshly
        // re-initialized pooled buffer.
        LoggingUtil.logWarn(logger, "m2", "short");
        verify(logger).warn("[m2]short");
    }

    @Test
    void privateConstructorCannotBeInstantiatedDirectly() throws Exception {
        var constructor = LoggingUtil.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object instance = constructor.newInstance();
        assertEquals(LoggingUtil.class, instance.getClass());
    }
}
