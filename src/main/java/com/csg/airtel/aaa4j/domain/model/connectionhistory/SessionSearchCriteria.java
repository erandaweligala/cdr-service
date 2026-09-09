package com.csg.airtel.aaa4j.domain.model.connectionhistory;

/**
 * Search filters and pagination for a session-history query.
 *
 * <p>Bundled into one object so callers do not have to pass eight positional
 * parameters to {@code ConnectionHistoryService.fetchSessionDetails}.
 */
public record SessionSearchCriteria(
        String username,
        String connectionStatus,
        String sessionId,
        String groupId,
        String startDate,
        String endDate,
        int pageSize,
        int page) {
}
