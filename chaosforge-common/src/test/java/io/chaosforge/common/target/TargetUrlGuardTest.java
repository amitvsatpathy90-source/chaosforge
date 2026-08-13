package io.chaosforge.common.target;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class TargetUrlGuardTest {

    private static final TargetUrlGuard BLOCKING = new TargetUrlGuard(true, List.of());
    private static final TargetUrlGuard PERMISSIVE = new TargetUrlGuard(false, List.of());

    @Test
    void permissiveGuardAllowsAnythingHttp() {
        // dev/test default: block off, no allowlist → localhost passes (tests target it)
        assertDoesNotThrow(() -> PERMISSIVE.validate("http://localhost:8080/x"));
        assertDoesNotThrow(() -> PERMISSIVE.validate("http://target/step-1"));
    }

    @Test
    void blockingGuardRejectsMetadataHost() {
        TargetNotAllowedException e =
                assertThrows(TargetNotAllowedException.class, () -> BLOCKING.validate("http://169.254.169.254/latest/meta-data/"));
        assertEquals("internal_host_blocked", e.reason());
    }

    @Test
    void blockingGuardRejectsLocalhostAndLoopback() {
        assertEquals("internal_host_blocked",
                assertThrows(TargetNotAllowedException.class, () -> BLOCKING.validate("http://localhost/x")).reason());
        assertEquals("internal_host_blocked",
                assertThrows(TargetNotAllowedException.class, () -> BLOCKING.validate("http://127.0.0.1/x")).reason());
    }

    @Test
    void blockingGuardRejectsIpv6LoopbackAndLinkLocal() {
        // IPv6 literals resolve without DNS, so these are deterministic.
        assertEquals("internal_host_blocked",
                assertThrows(TargetNotAllowedException.class, () -> BLOCKING.validate("http://[::1]/x")).reason());
        assertEquals("internal_host_blocked",
                assertThrows(TargetNotAllowedException.class, () -> BLOCKING.validate("http://[fe80::1]/x")).reason());
    }

    @Test
    void blockingGuardRejectsIpv6UniqueLocal_theModernPrivateBlock() {
        // fc00::/7 ULA — Java's isSiteLocalAddress() misses this (only the deprecated fec0::/10), so the
        // built-in checks alone would let it through (arch-audit M4). Includes the AWS IMDSv6 endpoint.
        assertEquals("internal_host_blocked",
                assertThrows(TargetNotAllowedException.class, () -> BLOCKING.validate("http://[fd00::1]/x")).reason());
        assertEquals("internal_host_blocked",
                assertThrows(TargetNotAllowedException.class, () -> BLOCKING.validate("http://[fc00::1]/x")).reason());
        assertEquals("internal_host_blocked",
                assertThrows(TargetNotAllowedException.class,
                        () -> BLOCKING.validate("http://[fd00:ec2::254]/latest/meta-data/")).reason());
    }

    @Test
    void allowlistOverridesIpv6UniqueLocalBlock() {
        // An operator can still sanction an in-mesh IPv6 ULA host (the allowlist is the precise ceiling).
        TargetUrlGuard allowlisted = new TargetUrlGuard(true, List.of("[fd00::1]"));
        assertDoesNotThrow(() -> allowlisted.validate("http://[fd00::1]/ingest"));
    }

    @Test
    void rejectsNonHttpScheme() {
        assertEquals("scheme_not_allowed",
                assertThrows(TargetNotAllowedException.class, () -> BLOCKING.validate("file:///etc/passwd")).reason());
        assertEquals("scheme_not_allowed",
                assertThrows(TargetNotAllowedException.class, () -> PERMISSIVE.validate("gopher://x/")).reason());
    }

    @Test
    void allowlistIsAHardCeiling() {
        TargetUrlGuard allowlisted = new TargetUrlGuard(true, List.of("rpe.svc.internal-test"));
        assertEquals("not_in_allowlist",
                assertThrows(TargetNotAllowedException.class, () -> allowlisted.validate("https://evil.example.com/x")).reason());
        assertDoesNotThrow(() -> allowlisted.validate("https://rpe.svc.internal-test/ingest"));
    }

    @Test
    void validateAllSkipsNullAndBlank() {
        assertDoesNotThrow(() -> PERMISSIVE.validateAll(java.util.Arrays.asList(null, "", "  ", "http://ok/x")));
    }
}
