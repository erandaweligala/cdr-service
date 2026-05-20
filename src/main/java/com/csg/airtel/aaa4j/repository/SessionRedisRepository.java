package com.csg.airtel.aaa4j.repository;

import com.csg.airtel.aaa4j.domain.model.connectionhistory.Session;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.value.ValueCommands;
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

    private final ValueCommands<String, Session> sessionCommands;

    @Inject
    public SessionRedisRepository(RedisDataSource redisDataSource) {
        this.sessionCommands = redisDataSource.value(Session.class);
    }

    /**
     * Save or update a session in Redis
     */
    public void save(Session session, String uniqueSessionId) {
        try {
            String key = buildKey(uniqueSessionId);
            sessionCommands.set(key, session);
//            sessionCommands.expire(key, SESSION_TTL);
            LOG.infof("Session saved to Redis: %s", session.getSessionId());
        } catch (Exception e) {
            LOG.errorf(e, "Error saving session to Redis: %s", session.getSessionId());
            throw new RuntimeException("Failed to save session to Redis", e);
        }
    }

    /**
     * Find a session by sessionId
     */
    public Optional<Session> findBySessionId(String sessionId) {
        try {
            String key = buildKey(sessionId);
            Session session = sessionCommands.get(key);
            if (session != null) {
                LOG.debugf("Session found in Redis: %s", sessionId);
                return Optional.of(session);
            }
            LOG.debugf("Session not found in Redis: %s", sessionId);
            return Optional.empty();
        } catch (Exception e) {
            LOG.errorf(e, "Error retrieving session from Redis: %s", sessionId);
            return Optional.empty();
        }
    }

    /**
     * Delete a session from Redis
     */
    public void delete(String sessionId) {
        try {
            String key = buildKey(sessionId);
            sessionCommands.getdel(key);
            LOG.infof("Session deleted from Redis: %s", sessionId);
        } catch (Exception e) {
            LOG.errorf(e, "Error deleting session from Redis: %s", sessionId);
        }
    }

    /**
     * Check if a session exists
     */
    public boolean exists(String sessionId) {
        try {
            return findBySessionId(sessionId).isPresent();
        } catch (Exception e) {
            LOG.errorf(e, "Error checking session existence: %s", sessionId);
            return false;
        }
    }

    /**
     * Update session TTL
     */
    public void refreshTTL(String sessionId) {
        try {
            String key = buildKey(sessionId);
//            sessionCommands.expire(key, SESSION_TTL);
            LOG.debugf("Session TTL refreshed: %s", sessionId);
        } catch (Exception e) {
            LOG.errorf(e, "Error refreshing session TTL: %s", sessionId);
        }
    }

    private String buildKey(String sessionId) {
        return SESSION_KEY_PREFIX + sessionId;
    }
}