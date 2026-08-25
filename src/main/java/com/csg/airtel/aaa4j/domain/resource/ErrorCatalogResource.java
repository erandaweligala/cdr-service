package com.csg.airtel.aaa4j.domain.resource;

import com.csg.airtel.aaa4j.domain.service.ErrorCatalog;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The error catalog: every distinct fault this service has hit, worst first.
 *
 * <p>Each row names the exception, its error code, the reason it happened and how many
 * times — the four things needed to identify a problem without reading a log. A concrete
 * {@code sampleMessage} and the {@code origin} frame are included so the normalised
 * {@code reason} can always be traced back to something real.
 *
 * <pre>
 * GET /monitoring/errors
 *
 * {
 *   "total": 4821,
 *   "distinctErrors": 3,
 *   "truncated": false,
 *   "errors": [
 *     {
 *       "error": "SQLException",
 *       "code": "ORA-00001",
 *       "reason": "unique constraint (AAA.PK_SESSION) violated on id #",
 *       "occurrences": 4812,
 *       "layer": "repository",
 *       "source": "oracle",
 *       "sampleMessage": "ORA-00001: unique constraint (AAA.PK_SESSION) violated on id 88213",
 *       "origin": "SessionRepository.insert:214",
 *       "firstSeen": 1756108800000,
 *       "lastSeen": 1756112400000
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>{@code truncated} is {@code true} when the number of distinct faults has hit the
 * catalog ceiling and further new signatures are being folded into an {@code (other)}
 * row; {@code total} stays exact either way.
 *
 * <p>The same numbers are exported to Prometheus as
 * {@code application_error_occurrences_total}; this endpoint exists so the catalog can be
 * read directly, without a Prometheus query, during an incident.
 */
@Path("/monitoring/errors")
@ApplicationScoped
public class ErrorCatalogResource {

    private final ErrorCatalog errorCatalog;

    @Inject
    public ErrorCatalogResource(ErrorCatalog errorCatalog) {
        this.errorCatalog = errorCatalog;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> errors() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("total", errorCatalog.totalOccurrences());
        body.put("distinctErrors", errorCatalog.distinctSignatures());
        body.put("truncated", errorCatalog.atCapacity());
        body.put("errors", errorCatalog.snapshot());
        return body;
    }
}
