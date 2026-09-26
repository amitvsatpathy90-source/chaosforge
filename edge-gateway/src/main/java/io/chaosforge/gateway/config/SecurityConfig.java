package io.chaosforge.gateway.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.ServerAuthenticationEntryPoint;
import org.springframework.security.web.server.util.matcher.PathPatternParserServerWebExchangeMatcher;
import org.springframework.security.web.server.util.matcher.ServerWebExchangeMatcher;

import java.net.URI;
import java.util.List;

/**
 * Spring Security 7 reactive resource server. The gateway is the sole acceptor of public JWTs.
 * Invalid JWT → 401 here, never forwarded to the Control Plane.
 */
@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    @Bean
    public SecurityWebFilterChain springSecurityFilterChain(
            ServerHttpSecurity http,
            @Qualifier("jwtDecoder") ReactiveJwtDecoder jwtDecoder) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(ex -> ex
                        .pathMatchers(
                                "/actuator/health", "/actuator/info", "/actuator/prometheus",
                                "/.well-known/oauth-protected-resource")
                        .permitAll()
                        .anyExchange().authenticated())
                // Bind the existing API chain explicitly as MCP has its own decoder.
                .oauth2ResourceServer(o -> o.jwt(jwt -> jwt.jwtDecoder(jwtDecoder)))
                .build();
    }

    @Bean
    @Primary
    public ReactiveJwtDecoder jwtDecoder(
            @Value("${spring.security.oauth2.resourceserver.jwt.jwk-set-uri}") String jwkSetUri,
            @Value("${chaosforge.security.jwt.issuer}") String issuer,
            @Value("${chaosforge.security.jwt.audience}") String audience) {
        NimbusReactiveJwtDecoder decoder = NimbusReactiveJwtDecoder.withJwkSetUri(jwkSetUri).build();
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
    ReactiveJwtDecoder mcpJwtDecoder(
            @Value("${spring.security.oauth2.resourceserver.jwt.jwk-set-uri}") String jwkSetUri,
            @Value("${chaosforge.security.jwt.issuer}") String issuer,
            @Value("${chaosforge.security.mcp.jwt.audience}") String audience) {

        // MCP is a separate OAuth resource boundary. It uses the same issuer/JWKS,
        // but requires the dedicated MCP resource audience.
        NimbusReactiveJwtDecoder decoder = NimbusReactiveJwtDecoder.withJwkSetUri(jwkSetUri).build();
        decoder.setJwtValidator(jwtClaimsValidator(issuer, audience));
        return decoder;
    }

    @Bean
    @Order(1)
    public SecurityWebFilterChain mcpSecurityFilterChain(
            ServerHttpSecurity http,
            @Qualifier("mcpJwtDecoder") ReactiveJwtDecoder mcpJwtDecoder) {

        ServerWebExchangeMatcher mcpMatcher =
                new PathPatternParserServerWebExchangeMatcher("/mcp/**");

        // Built from the request's own scheme/authority, not a configured public URL — this lab has no
        // fixed one (free-tier deploy note: only the Gateway is ever public, and even then the address
        // varies by environment), so deriving it per-request avoids a new env var that would just drift.
        ServerAuthenticationEntryPoint mcpEntryPoint = (exchange, ex) -> {
            URI request = exchange.getRequest().getURI();
            String metadataUrl = request.getScheme() + "://" + request.getAuthority()
                    + "/.well-known/oauth-protected-resource";
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            exchange.getResponse().getHeaders().add(HttpHeaders.WWW_AUTHENTICATE,
                    "Bearer resource_metadata=\"" + metadataUrl + "\"");
            return exchange.getResponse().setComplete();
        };

        return http
                .securityMatcher(mcpMatcher)
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(ex -> ex.anyExchange().authenticated())
                .exceptionHandling(e -> e.authenticationEntryPoint(mcpEntryPoint))
                .oauth2ResourceServer(o -> o.jwt(jwt -> jwt.jwtDecoder(mcpJwtDecoder)))
                .build();
    }
}

