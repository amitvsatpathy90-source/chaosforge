package io.chaosforge.controlplane.config;

import io.chaosforge.controlplane.security.JwtTenantExtractionFilter;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.AuthorizationFilter;

/**
 * Spring Security 7 (Boot 4.1.x) — stateless resource server. The {@link JwtTenantExtractionFilter}
 * re-verifies the forwarded JWT and binds the tenant (ADR-0524). Intra-service mTLS is enforced at
 * the TLS layer via {@code server.ssl.bundle} (see mtls-rules.md) and is configured per-profile.
 */
@Configuration
@EnableMethodSecurity   // enables @PreAuthorize on tenant-scoped service methods (ADR-0509 Layer 2)
public class SecurityConfig {

    @Bean
    @Primary
    public JwtDecoder jwtDecoder(
            @Value("${spring.security.oauth2.resourceserver.jwt.jwk-set-uri}") String jwkSetUri,
            @Value("${chaosforge.security.jwt.issuer}") String issuer,
            @Value("${chaosforge.security.jwt.audience}") String audience) {
        // Lazy: the JWK set is fetched on first decode, not at startup. Auto-refreshes on key rotation.
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(jwkSetUri).build();
        decoder.setJwtValidator(jwtClaimsValidator(issuer, audience));
        return decoder;
    }

    /**
     * Signature + expiry alone accept any token minted against our JWKS — the confused-deputy hole.
     * Adds issuer exact-match + audience-contains check. Duplicated per service (common is framework-free).
     */
    static OAuth2TokenValidator<Jwt> jwtClaimsValidator(String issuer, String audience) {
        return new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefaultWithIssuer(issuer),   // timestamps + iss
                new JwtClaimValidator<List<String>>(JwtClaimNames.AUD,
                        aud -> aud != null && aud.contains(audience)));
    }

    @Bean
    public SecurityFilterChain filterChain(
            HttpSecurity http,
            @Qualifier("jwtDecoder") JwtDecoder jwtDecoder)
            throws Exception {
        http.csrf(AbstractHttpConfigurer::disable)
            .sessionManagement(sm -> sm.sessionCreationPolicy(
                    SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/actuator/health", "/actuator/info", "/actuator/prometheus").permitAll()
                .requestMatchers("/internal/**").permitAll()   // mTLS-gated intra-service path (no public JWT)
                // DLQ triage is cross-tenant — OPERATOR role required, not just authentication (ADR-0536).
                .requestMatchers("/v1/dlq/**").hasRole("OPERATOR")
                .anyRequest().authenticated())
            // Stands in for the built-in bearer filter; must run before authorization is enforced.
            .addFilterBefore(new JwtTenantExtractionFilter(jwtDecoder), AuthorizationFilter.class);
        return http.build();
    }

    @Bean
    JwtDecoder mcpJwtDecoder(
            @Value("${spring.security.oauth2.resourceserver.jwt.jwk-set-uri}") String jwkSetUri,
            @Value("${chaosforge.security.jwt.issuer}") String issuer,
            @Value("${chaosforge.security.mcp.jwt.audience}") String audience) {

        // Separate decoder instance because /mcp requires a different resource audience.
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(jwkSetUri).build();
        decoder.setJwtValidator(jwtClaimsValidator(issuer, audience));
        return decoder;
    }

    /**
     * MCP transport authentication boundary.
     *
     * <p>The MCP resource uses a dedicated audience while retaining the same JWKS and issuer.
     * The decoder is deliberately local to this chain rather than another application-wide JwtDecoder bean,
     * avoiding ambiguity with the existing decoder.
     *
     * <p>The existing {@link JwtTenantExtractionFilter} is reused so MCP requests populate the same
     * verified TenantContext and ROLE_* authorities as the normal API path.
     *
     * <p>Tool-level scope authorization is enforced per-tool via {@code @PreAuthorize} on the
     + {@code @McpTool}-annotated method (see {@code ScenarioMcpTools}), not here — this chain
     + only establishes transport-level authentication and tenant binding.
     */
    @Bean
    @Order(2)
    SecurityFilterChain mcpSecurityFilterChain(
            HttpSecurity http,
            @Qualifier("mcpJwtDecoder") JwtDecoder mcpJwtDecoder)
            throws Exception {
        http.securityMatcher("/mcp", "/mcp/**")
                .csrf(AbstractHttpConfigurer::disable) // Bearer-token MCP transport; no browser session/cookie auth.
                .sessionManagement(session->session.sessionCreationPolicy(
                        SessionCreationPolicy.STATELESS)) // MCP has no HTTP session; JWT authenticates every request.
                .authorizeHttpRequests(auth -> auth.anyRequest().authenticated())
                // Reuse filter to verify the JWT, derive tenant_id and populate TenantContext + ROLE_* authorities.
                .addFilterBefore(
                        new JwtTenantExtractionFilter(mcpJwtDecoder),
                        AuthorizationFilter.class);
        return http.build();
    }
}
