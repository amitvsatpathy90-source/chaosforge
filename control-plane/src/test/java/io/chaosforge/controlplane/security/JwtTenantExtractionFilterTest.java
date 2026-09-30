package io.chaosforge.controlplane.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import jakarta.servlet.ServletException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;

/**
 * Authority-mapping contract for {@link JwtTenantExtractionFilter}.
 *
 * <p>Preserves {@code roles[]} → {@code ROLE_*} and maps only the allow-listed JWT
 * {@code scope} values to {@code SCOPE_*}; unrelated scopes remain inert.
 */
class JwtTenantExtractionFilterTest {

    private static final String TOKEN = "test.jwt.token";

    private JwtDecoder jwtDecoder;
    private JwtTenantExtractionFilter filter;

    @BeforeEach
    void setUp() {
        jwtDecoder = mock(JwtDecoder.class);
        filter = new JwtTenantExtractionFilter(jwtDecoder);
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
        TenantContext.clear();
    }

    /**
     * Existing role authority mapping must remain unchanged by the new scope handling.
     */
    @Test
    void rolesOnly_preservesExistingRoleMapping() throws Exception {
        Authentication authentication = authenticate(
                jwt().claim("roles", List.of("OPERATOR")).build());

        assertThat(authorityNames(authentication))
                .containsExactly("ROLE_OPERATOR");
    }

    /**
     * All three allow-listed ChaosForge scopes become SCOPE_* authorities.
     */
    @Test
    void scopeMapsKnownAuthorities() throws Exception {
        Authentication authentication = authenticate(
                jwt().claim(
                                "scope",
                                "chaosforge.read chaosforge.operate chaosforge.dlq")
                        .build());

        assertThat(authorityNames(authentication))
                .containsExactlyInAnyOrder(
                        "SCOPE_chaosforge.read",
                        "SCOPE_chaosforge.operate",
                        "SCOPE_chaosforge.dlq");
    }

    /**
     * Unknown scope values remain inert rather than becoming application authorities.
     */
    @Test
    void unrelatedScopeValue_isDroppedNotMapped() throws Exception {
        Authentication authentication = authenticate(
                jwt().claim("scope", "chaosforge.read other.scope").build());

        assertThat(authorityNames(authentication))
                .containsExactly("SCOPE_chaosforge.read");
    }

    /**
     * A token without a scope claim must not produce any MCP scope authorities.
     */
    @Test
    void missingScopeClaim_noScopeAuthorities() throws Exception {
        Authentication authentication = authenticate(
                jwt().build());

        assertThat(authorityNames(authentication))
                .isEmpty();
    }

    /**
     * Roles and MCP scopes are additive; scope mapping must preserve existing role authorities.
     */
    @Test
    void rolesAndScope_bothMapped() throws Exception {
        Authentication authentication = authenticate(
                jwt()
                        .claim("roles", List.of("OPERATOR"))
                        .claim("scope", "chaosforge.operate")
                        .build());

        assertThat(authorityNames(authentication))
                .containsExactlyInAnyOrder(
                        "ROLE_OPERATOR",
                        "SCOPE_chaosforge.operate");
    }

    /**
     * Leading, trailing, and repeated whitespace must not affect allow-listed scope parsing.
     */
    @Test
    void scopeWithExtraWhitespace_parsedCorrectly() throws Exception {
        Authentication authentication = authenticate(
                jwt().claim(
                                "scope",
                                "  chaosforge.read   chaosforge.dlq  ")
                        .build());

        assertThat(authorityNames(authentication))
                .containsExactlyInAnyOrder(
                        "SCOPE_chaosforge.read",
                        "SCOPE_chaosforge.dlq");
    }

    /**
     * An explicitly empty scope claim must not produce any MCP scope authorities.
     */
    @Test
    void emptyScopeString_noScopeAuthorities() throws Exception {
        Authentication authentication = authenticate(
                jwt().claim("scope", "").build());

        assertThat(authorityNames(authentication))
                .isEmpty();
    }

// Test helpers: exercise the real filter contract and expose only the resulting authorities.

    private Authentication authenticate(Jwt jwt) throws IOException, ServletException {
        when(jwtDecoder.decode(TOKEN)).thenReturn(jwt);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN);

        filter.doFilter(
                request,
                new MockHttpServletResponse(),
                new MockFilterChain());

        return SecurityContextHolder.getContext().getAuthentication();
    }

    private static List<String> authorityNames(Authentication authentication) {
        return authentication.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .toList();
    }

    private static Jwt.Builder jwt() {
        Instant now = Instant.now();

        return Jwt.withTokenValue(TOKEN)
                .header("alg", "RS256")
                .subject("test-user")
                .claim("tenant_id", UUID.randomUUID().toString())
                .issuedAt(now)
                .expiresAt(now.plusSeconds(300));
    }
}
