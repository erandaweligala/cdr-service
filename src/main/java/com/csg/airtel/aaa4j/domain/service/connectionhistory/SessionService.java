package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import com.csg.airtel.aaa4j.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.connectionhistory.*;
import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;
import com.csg.airtel.aaa4j.domain.util.exceptions.BaseException;
import com.csg.airtel.aaa4j.repository.SessionRedisRepository;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.http.HttpStatus;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;

@ApplicationScoped
public class SessionService {

    private static final Logger LOG = Logger.getLogger(SessionService.class);

    private final SessionRedisRepository redisRepository;
    private final ElasticSearchService elasticsearchService;

    public SessionService(SessionRedisRepository redisRepository, ElasticSearchService elasticsearchService) {
        this.redisRepository = redisRepository;
        this.elasticsearchService = elasticsearchService;
    }

    /**
     * Process ACCOUNTING_START event
     */
    public Uni<Void> processStartEvent(AccountingEvent event) {
        LoggingUtil.logDebug(LOG, "processStartEvent", "Processing START event: %s", event.getEventId());

        return Uni.createFrom().item(() -> getUniqueIdFromSessionCDR(event.getPayload().getSession()))
                .flatMap(uniqueSessionId -> redisRepository.findBySessionId(uniqueSessionId)
                        .flatMap(existing -> {
                            Session session;
                            if (existing.isPresent()) {
                                session = existing.get();
                                LoggingUtil.logWarn(LOG, "processStartEvent",
                                        "Session already exists for START event: %s, updating existing session",
                                        session.getSessionId());
                                updateSessionFromStart(session, event);
                            } else {
                                LoggingUtil.logWarn(LOG, "processStartEvent",
                                        "Session not found for %s event: %s, creating new session",
                                        event.getEventType(), uniqueSessionId);
                                session = createSession(event, uniqueSessionId);
                            }
                            return addInstanceInfoAndSave(session, event, true, uniqueSessionId)
                                    .chain(() -> elasticsearchService.indexSession(
                                            session, uniqueSessionId, session.getIndexName()))
                                    .invoke(() -> LoggingUtil.logDebug(LOG, "processStartEvent",
                                            "START event processed successfully: %s",
                                            session.getSessionId()));
                        }));
    }

    private String getUniqueIdFromSessionCDR(SessionCdr session) {
        if (session.getSessionId() != null && session.getNasPort() != null) {
            return session.getSessionId() + session.getNasPort();
        }
        throw new BaseException(
                "Incomplete CDR Data: sessionId and nasPort are required",
                ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(),
                HttpStatus.SC_BAD_REQUEST,
                ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code()
        );
    }

    /**
     * Process ACCOUNTING_INTERIM event
     */
    public Uni<Void> processInterimEvent(AccountingEvent event) {
        LoggingUtil.logDebug(LOG, "processInterimEvent", "Processing INTERIM event: %s", event.getEventId());

        return Uni.createFrom().item(() -> getUniqueIdFromSessionCDR(event.getPayload().getSession()))
                .flatMap(uniqueSessionId -> getOrCreateSession(uniqueSessionId, event)
                        .flatMap(session -> {
                            updateSessionFromInterim(session, event);
                            return addInstanceInfoAndSave(session, event, true, uniqueSessionId)
                                    .chain(() -> elasticsearchService.indexSession(
                                            session, uniqueSessionId, session.getIndexName()))
                                    .invoke(() -> LoggingUtil.logDebug(LOG, "processInterimEvent",
                                            "INTERIM event processed successfully: %s",
                                            uniqueSessionId));
                        }));
    }

    /**
     * Process ACCOUNTING_STOP event
     */
    public Uni<Void> processStopEvent(AccountingEvent event) {
        LoggingUtil.logDebug(LOG, "processStopEvent", "Processing STOP event: %s", event.getEventId());

        return Uni.createFrom().item(() -> getUniqueIdFromSessionCDR(event.getPayload().getSession()))
                .flatMap(uniqueSessionId -> getOrCreateSession(uniqueSessionId, event)
                        .flatMap(session -> {
                            updateSessionFromStop(session, event);
                            return addInstanceInfoAndSave(session, event, false, uniqueSessionId)
                                    .chain(() -> elasticsearchService.indexSession(
                                            session, uniqueSessionId, session.getIndexName()))
                                    .chain(() -> redisRepository.delete(uniqueSessionId))
                                    .invoke(() -> LoggingUtil.logDebug(LOG, "processStopEvent",
                                            "STOP event processed successfully: %s",
                                            uniqueSessionId));
                        }));
    }

    /**
     * Process COA_REQUEST event
     */
    public Uni<Void> processCoaRequestEvent(AccountingEvent event) {
        LoggingUtil.logDebug(LOG, "processCoaRequestEvent", "Processing COA REQUEST event: %s", event.getEventId());

        return Uni.createFrom().item(() -> getUniqueIdFromSessionCDR(event.getPayload().getSession()))
                .flatMap(uniqueSessionId -> getOrCreateSession(uniqueSessionId, event)
                        .flatMap(session -> {
                            updateSessionFromCoa(session, event);
                            return addInstanceInfoAndSave(session, event, false, uniqueSessionId)
                                    .chain(() -> elasticsearchService.indexSession(
                                            session, uniqueSessionId, session.getIndexName()))
                                    .invoke(() -> LoggingUtil.logDebug(LOG, "processCoaRequestEvent",
                                            "COA REQUEST event processed successfully: %s",
                                            uniqueSessionId));
                        }));
    }

    /**
     * Process COA_RESPONSE event
     */
    public Uni<Void> processCoaResponseEvent(AccountingEvent event) {
        LoggingUtil.logDebug(LOG, "processCoaResponseEvent", "Processing COA RESPONSE event: %s", event.getEventId());

        return Uni.createFrom().item(() -> getUniqueIdFromSessionCDR(event.getPayload().getSession()))
                .flatMap(uniqueSessionId -> getOrCreateSession(uniqueSessionId, event)
                        .flatMap(session -> {
                            updateSessionFromCoa(session, event);
                            COA coa = event.getPayload().getCoa();
                            session.setConnectionStatus(getSessionStatusFromCoaResponse(coa));

                            Uni<Void> tail = addInstanceInfoAndSave(session, event, false, uniqueSessionId);

                            if (coa.getStatus().equalsIgnoreCase("NAK")) {
                                tail = tail
                                        .chain(() -> elasticsearchService.indexSession(
                                                session, uniqueSessionId, session.getIndexName()))
                                        .chain(() -> redisRepository.delete(uniqueSessionId));
                            }

                            return tail.invoke(() -> LoggingUtil.logDebug(LOG, "processCoaResponseEvent",
                                    "COA RESPONSE event processed successfully: %s",
                                    uniqueSessionId));
                        }));
    }

    private SessionStatus getSessionStatusFromCoaResponse(COA coa) {
        if (coa != null && coa.getStatus() != null) {
            return switch (coa.getStatus().toUpperCase()) {
                case "ACK" -> SessionStatus.TERMINATION_REQUESTED;
                case "NAK" -> SessionStatus.TERMINATED;
                default -> {
                    LoggingUtil.logError(LOG, "getSessionStatusFromCoaResponse", null,
                            "Unknown COA status: %s", coa.getStatus());
                    throw new BaseException(
                            "Invalid COA Status: " + coa.getStatus(),
                            ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(),
                            HttpStatus.SC_BAD_REQUEST,
                            ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code()
                    );
                }
            };
        } else {
            LoggingUtil.logError(LOG, "getSessionStatusFromCoaResponse", null,
                    "Incomplete COA response received : %s", coa);
            throw new BaseException(
                    "Incomplete COA Data",
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.description(),
                    HttpStatus.SC_BAD_REQUEST,
                    ResponseCodeEnum.EXCEPTION_SERVICE_LAYER.code()
            );
        }
    }

    /**
     * Get existing session or create new one
     */
    private Uni<Session> getOrCreateSession(String sessionId, AccountingEvent event) {
        return redisRepository.findBySessionId(sessionId)
                .map(existing -> {
                    if (existing.isPresent()) {
                        return existing.get();
                    }
                    LoggingUtil.logWarn(LOG, "getOrCreateSession",
                            "Session not found for %s event: %s, creating new session",
                            event.getEventType(), sessionId);
                    return createSession(event, sessionId);
                });
    }

    /**
     * Create session from any event type
     */
    private Session createSession(AccountingEvent event, String uniqueId) {
        Session session = new Session();
        Payload payload = event.getPayload();

        session.setSessionId(payload.getSession().getSessionId());
        session.setUniqueId(uniqueId);
        session.setIndexName(elasticsearchService.getCurrentIndex());

        Instant startTime = payload.getSession().getStartTime();
        session.setStartTime(startTime != null ? Date.from(startTime) : Date.from(event.getEventTimestamp()));

        populateUserInfo(session, payload);
        session.setUsage(getUsageFromPayload(payload));

        if (event.getEventType().equalsIgnoreCase(String.valueOf(EventTypes.COA_RESPONSE))) {
            session.setConnectionStatus(SessionStatus.TERMINATION_REQUESTED);
        } else {
            session.setConnectionStatus(SessionStatus.ACTIVE);
        }

        session.setSessionInstances(new ArrayList<>());
        return session;
    }

    /**
     * Add instance info and save session
     */
    private Uni<Void> addInstanceInfoAndSave(Session session, AccountingEvent event, boolean saveToRedis, String uniqueSessionId) {
        SessionInstanceInfo instanceInfo = createInstanceInfo(event);
        session.getSessionInstances().add(instanceInfo);

        if (saveToRedis) {
            return redisRepository.save(session, uniqueSessionId);
        }
        return Uni.createFrom().voidItem();
    }

    /**
     * Update existing session from START event
     */
    private void updateSessionFromStart(Session session, AccountingEvent event) {
        Payload payload = event.getPayload();

        if (payload.getSession().getStartTime() != null) {
            session.setStartTime(Date.from(payload.getSession().getStartTime()));
        }

        populateUserInfo(session, payload);
        session.setUsage(getUsageFromPayload(payload));
        session.setUpdatedTime(Date.from(event.getEventTimestamp()));
        session.setConnectionStatus(SessionStatus.ACTIVE);
    }

    /**
     * Update session from INTERIM event
     */
    private void updateSessionFromInterim(Session session, AccountingEvent event) {
        session.setUpdatedTime(Date.from(event.getEventTimestamp()));
        session.setUsage(getUsageFromPayload(event.getPayload()));
    }

    /**
     * Update session from STOP event
     */
    private void updateSessionFromStop(Session session, AccountingEvent event) {
        Payload payload = event.getPayload();

        Instant sessionEndTime = payload.getSession().getSessionStopTime();
        session.setEndTime(sessionEndTime != null ? Date.from(sessionEndTime) : Date.from(event.getEventTimestamp()));
        session.setUpdatedTime(Date.from(event.getEventTimestamp()));
        session.setUsage(getUsageFromPayload(payload));
        if (session.getConnectionStatus().equals(SessionStatus.TERMINATION_REQUESTED)) {
            session.setConnectionStatus(SessionStatus.TERMINATED);
        } else {
            session.setConnectionStatus(SessionStatus.COMPLETED);
        }
    }

    /**
     * Update session from COA event
     */
    private void updateSessionFromCoa(Session session, AccountingEvent event) {
        session.setUpdatedTime(Date.from(event.getEventTimestamp()));
        session.setUsage(getUsageFromPayload(event.getPayload()));
        if (event.getEventType().equalsIgnoreCase(String.valueOf(EventTypes.COA_RESPONSE))) {
            Instant sessionEndTime = event.getPayload().getSession().getSessionStopTime();
            session.setEndTime(sessionEndTime != null ? Date.from(sessionEndTime) : Date.from(event.getEventTimestamp()));
            session.setConnectionStatus(getSessionStatusFromCoaResponse(event.getPayload().getCoa()));
        } else {
            session.setConnectionStatus(SessionStatus.TERMINATION_REQUESTED);
        }
    }

    /**
     * Populate user information from payload
     */
    private void populateUserInfo(Session session, Payload payload) {
        if (payload.getUser() != null) {
            session.setUserName(payload.getUser().getUserName());
            session.setGroupId(payload.getUser().getGroupId());
        }
    }

    /**
     * Get usage from payload, return 0 if not available
     */
    private Long getUsageFromPayload(Payload payload) {
        return (payload.getAccounting() != null)
                ? payload.getAccounting().getTotalUsage()
                : 0L;
    }

    /**
     * Create SessionInstanceInfo from event
     */
    private SessionInstanceInfo createInstanceInfo(AccountingEvent event) {
        SessionInstanceInfo info = new SessionInstanceInfo();

        info.setDateTime(Date.from(event.getEventTimestamp()));
        info.setMessageId(event.getEventId());
        info.setMessageType(event.getEventType());

        if (event.getPayload().getAccounting() != null) {
            Accounting accounting = event.getPayload().getAccounting();
            info.setUsage(accounting.getSessionUsage());
            info.setServiceId(accounting.getServiceId());
            info.setBucketId(accounting.getBucketId());
        } else {
            LoggingUtil.logWarn(LOG, "createInstanceInfo",
                    "No accounting details to be recorded for the event Id: %s", event.getEventId());
        }
        return info;
    }
}
