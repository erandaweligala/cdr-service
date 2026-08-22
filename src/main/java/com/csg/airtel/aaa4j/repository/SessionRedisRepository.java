package com.csg.airtel.aaa4j.repository;

import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.datasource.value.SetArgs;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Optional;

@ApplicationScoped
public class SessionRedisRepository {

    private static final Logger LOG = Logger.getLogger(SessionRedisRepository.class);
    private static final Duration SESSION_TTL = Duration.ofHours(24);
    private static final String SESSION_KEY_PREFIX = "cdr::";

    private final ReactiveValueCommands<String, Session> sessionCommands;
    private final ReactiveKeyCommands<String> keyCommands;

    @Inject
    Instance<ExceptionMetricsService> metrics;

    @Inject
    public SessionRedisRepository(ReactiveRedisDataSource redisDataSource) {
        this.sessionCommands = redisDataSource.value(Session.class);
        this.keyCommands = redisDataSource.key(String.class);
    }

    /**
     * Records a Redis failure against {@code Source.REDIS} so it reaches
     * ConnectivityMonitoringService and drives {@code dependency_up{dependency="redis"}}.
     *
     * <p>Every command below recovers rather than propagating, so without this the only
     * evidence of a Redis outage during live traffic would be a log line.</p>
     */
    private void recordRedisFailure(Throwable e) {
        if (metrics != null && !metrics.isUnsatisfied()) {
            metrics.get().recordException(e,
                    ExceptionMetricsService.Layer.CLIENT,
                    ExceptionMetricsService.Source.REDIS);
        }
    }

    public Uni<Void> save(Session session, String uniqueSessionId) {
        String key = buildKey(uniqueSessionId);
        // Single round-trip SET ... EX instead of SET followed by a separate EXPIRE.
        // At high TPS the extra EXPIRE call doubled the Redis write latency on every
        // START/INTERIM/STOP/COA event, which serialized the consumer pipeline and
        // contributed to Kafka lag. SET with EX is also atomic (no key without a TTL).
        return sessionCommands.set(key, session, new SetArgs().ex(SESSION_TTL))
                .invoke(() -> LoggingUtil.logDebug(LOG, "save", "Session saved to Redis: %s", session.getSessionId()))
                .onFailure().invoke(e -> {
                    LoggingUtil.logError(LOG, "save", e, "Error saving session to Redis: %s", session.getSessionId());
                    recordRedisFailure(e);
                });
    }

    public Uni<Optional<Session>> findBySessionId(String sessionId) {
        String key = buildKey(sessionId);
        return sessionCommands.get(key)
                .map(Optional::ofNullable)
                .onFailure().recoverWithItem(e -> {
                    LoggingUtil.logError(LOG, "findBySessionId", e, "Error retrieving session from Redis: %s", sessionId);
                    recordRedisFailure(e);
                    return Optional.empty();
                });
    }

    public Uni<Void> delete(String sessionId) {
        String key = buildKey(sessionId);
        return sessionCommands.getdel(key)
                .replaceWithVoid()
                .invoke(() -> LoggingUtil.logDebug(LOG, "delete", "Session deleted from Redis: %s", sessionId))
                .onFailure().recoverWithItem(e -> {
                    LoggingUtil.logError(LOG, "delete", e, "Error deleting session from Redis: %s", sessionId);
                    recordRedisFailure(e);
                    return null;
                });
    }

    public Uni<Boolean> exists(String sessionId) {
        return keyCommands.exists(buildKey(sessionId))
                .onFailure().recoverWithItem(e -> {
                    LoggingUtil.logError(LOG, "exists", e, "Error checking session existence: %s", sessionId);
                    recordRedisFailure(e);
                    return false;
                });
    }

    public Uni<Void> refreshTTL(String sessionId) {
        String key = buildKey(sessionId);
        return keyCommands.expire(key, SESSION_TTL)
                .replaceWithVoid()
                .invoke(() -> LoggingUtil.logDebug(LOG, "refreshTTL", "Session TTL refreshed: %s", sessionId))
                .onFailure().recoverWithItem(e -> {
                    LoggingUtil.logError(LOG, "refreshTTL", e, "Error refreshing session TTL: %s", sessionId);
                    recordRedisFailure(e);
                    return null;
                });
    }

    private String buildKey(String sessionId) {
        return SESSION_KEY_PREFIX + sessionId;
    }
}
