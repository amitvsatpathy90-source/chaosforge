package io.chaosforge.common.target;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Single SSRF / blast-radius policy for every outbound scenario target (arch-audit HIGH-2, ADR-0534).
 * Shared by CP authoring and Exec execution so the rule is defined once. Scheme must be http/https.
 * Allowlist mode (non-empty allowedHosts) is the ceiling and overrides the private-network block —
 * an allowlisted private host (e.g. RPE ingress) is permitted. Open mode applies the coarse SSRF
 * guard (localhost, metadata host, private/link-local ranges) when blockPrivateNetworks is on.
 * Both knobs default off; deployed profiles must set them (target-validation-rules.md).
 * Rejection reasons are shape tokens only — never the URL.
 */
public final class TargetUrlGuard {

    private static final String METADATA_HOST = "169.254.169.254";

    private final boolean blockPrivateNetworks;
    private final Set<String> allowedHosts;

    public TargetUrlGuard(boolean blockPrivateNetworks, List<String> allowedHosts) {
        this.blockPrivateNetworks = blockPrivateNetworks;
        this.allowedHosts = allowedHosts == null ? Set.of() : allowedHosts.stream()
                .map(h -> h.toLowerCase(Locale.ROOT).trim())
                .filter(h -> !h.isBlank())
                .collect(Collectors.toUnmodifiableSet());
    }

    /** Validates every non-blank URL. @throws TargetNotAllowedException on the first that fails. */
    public void validateAll(List<String> targetUrls) {
        if (targetUrls == null) {
            return;
        }
        for (String url : targetUrls) {
            if (url != null && !url.isBlank()) {
                validate(url);
            }
        }
    }

    /** @throws TargetNotAllowedException if the URL fails the policy. */
    public void validate(String rawUrl) {
        URI uri;
        try {
            uri = URI.create(rawUrl);
        } catch (IllegalArgumentException e) {
            throw new TargetNotAllowedException("malformed_url");
        }
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.equals("http") || scheme.equals("https"))) {
            throw new TargetNotAllowedException("scheme_not_allowed");
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new TargetNotAllowedException("no_host");
        }
        String h = host.toLowerCase(Locale.ROOT);
        if (!allowedHosts.isEmpty()) {
            // Allowlist mode: overrides the private-network block; operator sanctions the host explicitly.
            if (!allowedHosts.contains(h)) {
                throw new TargetNotAllowedException("not_in_allowlist");
            }
            return;
        }
        // Open mode (no allowlist): coarse SSRF guard against internal infrastructure.
        if (blockPrivateNetworks
                && (h.equals("localhost") || h.equals(METADATA_HOST) || resolvesToInternal(h))) {
            throw new TargetNotAllowedException("internal_host_blocked");
        }
    }

    /**
     * One DNS lookup; any resolved address in a non-public range fails the gate.
     * Known residual: DNS-rebinding TOCTOU — the HTTP client re-resolves at connect time.
     * Allowlist + egress policy are the mitigations; this guard is not rebinding-proof (ADR-0534).
     */
    private static boolean resolvesToInternal(String host) {
        try {
            for (InetAddress addr : InetAddress.getAllByName(host)) {
                if (addr.isLoopbackAddress() || addr.isLinkLocalAddress()
                        || addr.isSiteLocalAddress() || addr.isAnyLocalAddress()
                        || addr.isMulticastAddress() || isUniqueLocalV6(addr)) {
                    return true;
                }
            }
            return false;
        } catch (UnknownHostException e) {
            return true;   // unresolvable → cannot prove it is public → reject
        }
    }

    /** IPv6 ULA block fc00::/7 (incl. AWS IMDSv6) — isSiteLocalAddress() misses it (deprecated fec0::/10 only). */
    private static boolean isUniqueLocalV6(InetAddress addr) {
        return addr instanceof Inet6Address && (addr.getAddress()[0] & 0xfe) == 0xfc;
    }
}
