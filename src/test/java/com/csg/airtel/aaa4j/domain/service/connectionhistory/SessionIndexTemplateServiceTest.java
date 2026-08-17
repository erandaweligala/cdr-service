package com.csg.airtel.aaa4j.domain.service.connectionhistory;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateRequest;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import jakarta.json.stream.JsonParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionIndexTemplateServiceTest {

    private static final String INDEX = "radius-sessions";

    private SessionIndexTemplateService service;

    @BeforeEach
    void setUp() {
        service = new SessionIndexTemplateService();
        service.sessionsIndex = INDEX;
        service.templateEnabled = true;
    }

    @Test
    void template_targetsTheDailySessionIndices() {
        PutIndexTemplateRequest request = service.buildTemplateRequest();

        assertEquals("radius-sessions-template", request.name());
        assertEquals(java.util.List.of("radius-sessions-*"), request.indexPatterns());
        assertEquals(SessionIndexTemplateService.TEMPLATE_PRIORITY, request.priority());
    }

    @Test
    void template_mapsSessionTimestampsAsDates() {
        Map<String, Property> properties = templateProperties();

        for (String field : java.util.List.of("startTime", "endTime", "updatedTime")) {
            Property property = properties.get(field);
            assertTrue(property.isDate(), field + " must be mapped as a date, not inferred");
            assertEquals(SessionIndexTemplateService.DATE_FORMAT, property.date().format());
        }
    }

    @Test
    void template_mapsSessionInstanceTimestampAsDate() {
        Property instances = templateProperties().get("sessionInstances");

        Property dateTime = instances.object().properties().get("dateTime");

        assertTrue(dateTime.isDate());
        assertEquals(SessionIndexTemplateService.DATE_FORMAT, dateTime.date().format());
    }

    @Test
    void template_keepsKeywordSubFieldsTheSearchQueriesUse() {
        Map<String, Property> properties = templateProperties();

        // ConnectionHistoryService filters on <field>.keyword for each of these.
        for (String field : java.util.List.of("userName", "connectionStatus", "sessionId", "groupId", "uniqueId")) {
            Property property = properties.get(field);
            assertTrue(property.isText(), field + " must stay a text field");
            assertTrue(property.text().fields().get("keyword").isKeyword(),
                    field + " must keep its .keyword sub-field");
        }
    }

    @Test
    void template_mapsUsageAsLong() {
        assertTrue(templateProperties().get("usage").isLong());
    }

    @Test
    void template_disablesDateDetection() {
        assertFalse(service.buildTemplateRequest().template().mappings().dateDetection());
    }

    @Test
    void firstNonDateDateField_detectsLegacyNumericStartTime() {
        GetMappingResponse mapping = mappingOf("radius-sessions-2026.08.17", """
                {"startTime":{"type":"long"},"endTime":{"type":"date"}}""");

        assertEquals("startTime",
                SessionIndexTemplateService.firstNonDateDateField(mapping, "radius-sessions-2026.08.17"));
    }

    @Test
    void firstNonDateDateField_acceptsACorrectlyMappedIndex() {
        GetMappingResponse mapping = mappingOf("radius-sessions-2026.08.17", """
                {"startTime":{"type":"date"},"endTime":{"type":"date"},"updatedTime":{"type":"date"}}""");

        assertNull(SessionIndexTemplateService.firstNonDateDateField(mapping, "radius-sessions-2026.08.17"));
    }

    @Test
    void firstNonDateDateField_ignoresAnIndexWithNoTimestampsYet() {
        GetMappingResponse mapping = mappingOf("radius-sessions-2026.08.17", """
                {"sessionId":{"type":"keyword"}}""");

        assertNull(SessionIndexTemplateService.firstNonDateDateField(mapping, "radius-sessions-2026.08.17"));
    }

    @Test
    void firstNonDateDateField_ignoresAnIndexMissingFromTheResponse() {
        GetMappingResponse mapping = mappingOf("radius-sessions-2026.08.17", """
                {"startTime":{"type":"long"}}""");

        assertNull(SessionIndexTemplateService.firstNonDateDateField(mapping, "radius-sessions-2026.08.18"));
    }

    private Map<String, Property> templateProperties() {
        TypeMapping mappings = service.buildTemplateRequest().template().mappings();
        return mappings.properties();
    }

    private static GetMappingResponse mappingOf(String index, String propertiesJson) {
        String json = "{\"" + index + "\":{\"mappings\":{\"properties\":" + propertiesJson + "}}}";
        JsonpMapper mapper = new JacksonJsonpMapper();
        try (JsonParser parser = mapper.jsonProvider().createParser(new StringReader(json))) {
            return GetMappingResponse._DESERIALIZER.deserialize(parser, mapper);
        }
    }
}
