package com.csg.airtel.aaa4j.repository;

import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
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
    public SessionRedisRepository(ReactiveRedisDataSource redisDataSource) {
        this.sessionCommands = redisDataSource.value(Session.class);
        this.keyCommands = redisDataSource.key(String.class);
    }

    public Uni<Void> save(Session session, String uniqueSessionId) {
        String key = buildKey(uniqueSessionId);
        return sessionCommands.set(key, session)
                .chain(() -> keyCommands.expire(key, SESSION_TTL))
                .replaceWithVoid()
                .invoke(() -> LoggingUtil.logDebug(LOG, "save", "Session saved to Redis: %s", session.getSessionId()))
                .onFailure().invoke(e ->
                        LoggingUtil.logError(LOG, "save", e, "Error saving session to Redis: %s", session.getSessionId()));
    }

    public Uni<Optional<Session>> findBySessionId(String sessionId) {
        String key = buildKey(sessionId);
        return sessionCommands.get(key)
                .map(Optional::ofNullable)
                .onFailure().recoverWithItem(e -> {
                    LoggingUtil.logError(LOG, "findBySessionId", e, "Error retrieving session from Redis: %s", sessionId);
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
                    return null;
                });
    }

    public Uni<Boolean> exists(String sessionId) {
        return keyCommands.exists(buildKey(sessionId))
                .onFailure().recoverWithItem(e -> {
                    LoggingUtil.logError(LOG, "exists", e, "Error checking session existence: %s", sessionId);
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
                    return null;
                });
    }

    private String buildKey(String sessionId) {
        return SESSION_KEY_PREFIX + sessionId;
    }
}
