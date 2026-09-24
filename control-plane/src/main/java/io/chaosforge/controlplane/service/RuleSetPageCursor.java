package io.chaosforge.controlplane.service;

import io.chaosforge.controlplane.domain.RuleSet;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

/** Opaque keyset cursor: base64((createdAt, ruleSetId, version)). Package-private. */
record RuleSetPageCursor(Instant createdAt, UUID ruleSetId, int version) {

    static String encode(RuleSet ruleSet) {
        String raw = ruleSet.createdAt() + "|" + ruleSet.ruleSetId() + "|" + ruleSet.version();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    static RuleSetPageCursor decode(String encoded) {
        try {
            String raw = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
            String[] parts = raw.split("\\|", -1);
            if (parts.length != 3) {
                throw new IllegalArgumentException();   // e.g. a scenario cursor
            }
            return new RuleSetPageCursor(
                    Instant.parse(parts[0]), UUID.fromString(parts[1]), Integer.parseInt(parts[2]));
        } catch (RuntimeException e) {
            // Tenant scope comes from the query, so a forged cursor only moves position.
            throw new IllegalArgumentException("invalid cursor");
        }
    }
}
