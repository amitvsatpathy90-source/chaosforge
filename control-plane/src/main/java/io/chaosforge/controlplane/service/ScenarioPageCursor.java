package io.chaosforge.controlplane.service;

import io.chaosforge.controlplane.domain.Scenario;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

/** Opaque keyset cursor: base64((createdAt, scenarioId)). Package-private — REST/MCP never touch fields directly. */
record ScenarioPageCursor(Instant createdAt, UUID scenarioId) {

    // URL-safe opaque token; clients must not depend on the internal tuple representation.
    static String encode(Scenario s) {
        String raw = s.createdAt() + "|" + s.scenarioId();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    // Cursor is the exclusive continuation point for the repository's DESC ordering.
    static ScenarioPageCursor decode(String encoded) {
        try {
            String raw = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
            int sep = raw.indexOf('|');
            return new ScenarioPageCursor(Instant.parse(raw.substring(0, sep)), UUID.fromString(raw.substring(sep + 1)));
        } catch (RuntimeException e) {
            // Tampered/malformed cursor -> 400, not 500. Caught centrally (GlobalExceptionHandler#badInput).
            throw new IllegalArgumentException("invalid cursor");
        }
    }
}
