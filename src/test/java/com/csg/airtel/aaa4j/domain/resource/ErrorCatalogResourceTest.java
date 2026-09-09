package com.csg.airtel.aaa4j.domain.resource;

import com.csg.airtel.aaa4j.domain.service.ErrorCatalog;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ErrorCatalogResourceTest {

    @Test
    void errorsAssemblesCatalogSummaryFromErrorCatalog() {
        ErrorCatalog errorCatalog = mock(ErrorCatalog.class);
        when(errorCatalog.totalOccurrences()).thenReturn(4821L);
        when(errorCatalog.distinctSignatures()).thenReturn(3);
        when(errorCatalog.atCapacity()).thenReturn(false);
        List<ErrorCatalog.ErrorSummary> summaries = List.of(
                new ErrorCatalog.ErrorSummary("SQLException", "ORA-00001", "unique constraint ? violated on id #",
                        4812L, "repository", "oracle", "sample", "Origin.method:1", 1L, 2L));
        when(errorCatalog.snapshot()).thenReturn(summaries);

        ErrorCatalogResource resource = new ErrorCatalogResource(errorCatalog);

        Map<String, Object> body = resource.errors();

        assertEquals(4821L, body.get("total"));
        assertEquals(3, body.get("distinctErrors"));
        assertEquals(false, body.get("truncated"));
        assertFalse(((List<?>) body.get("errors")).isEmpty());
    }

    @Test
    void errorsReflectsTruncatedFlagWhenCatalogIsAtCapacity() {
        ErrorCatalog errorCatalog = mock(ErrorCatalog.class);
        when(errorCatalog.totalOccurrences()).thenReturn(0L);
        when(errorCatalog.distinctSignatures()).thenReturn(200);
        when(errorCatalog.atCapacity()).thenReturn(true);
        when(errorCatalog.snapshot()).thenReturn(List.of());

        ErrorCatalogResource resource = new ErrorCatalogResource(errorCatalog);

        Map<String, Object> body = resource.errors();

        assertEquals(true, body.get("truncated"));
    }
}
