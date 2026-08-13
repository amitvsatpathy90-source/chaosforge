package io.chaosforge.controlplane.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpHeaders;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Re-verifies the original JWT forwarded by the gateway and derives {@code tenant_id} from the
 * VERIFIED claims (ADR-0524). mTLS authenticates the channel peer (the gateway); this filter
 * authenticates the principal (the tenant). The {@code X-Tenant-Id} header is never trusted here.
 */
public class JwtTenantExtractionFilter extends OncePerRequestFilter {

    private final JwtDecoder jwtDecoder;

    public JwtTenantExtractionFilter(JwtDecoder jwtDecoder) {
        this.jwtDecoder = jwtDecoder;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        // Permitted paths carry no public JWT: actuator probes and the mTLS-gated intra-service path.
        String path = request.getRequestURI();
        return path.startsWith("/actuator/") || path.startsWith("/internal/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith("Bearer ")) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        try {
            Jwt jwt = jwtDecoder.decode(header.substring(7));
            String tenantClaim = jwt.getClaimAsString("tenant_id");
            if (tenantClaim == null) {
                // SEC-03: valid token, no tenant_id — reject as 401, not a 500 from UUID.fromString(null).
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }
            TenantContext.set(UUID.fromString(tenantClaim));   // from VERIFIED claim; malformed → IAE → 401 below
            SecurityContextHolder.getContext().setAuthentication(
                    new JwtAuthenticationToken(jwt, extractAuthorities(jwt)));
        } catch (JwtException | IllegalArgumentException e) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        try {
            chain.doFilter(request, response);
        } finally {
            TenantContext.clear();   // never leak across requests
        }
    }

    private static Collection<GrantedAuthority> extractAuthorities(Jwt jwt) {
        List<String> roles = jwt.getClaimAsStringList("roles");
        if (roles == null) {
            return List.of();
        }
        return roles.stream()
                .map(r -> (GrantedAuthority) new SimpleGrantedAuthority("ROLE_" + r))
                .toList();
    }
}
