package io.chaosforge.controlplane.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Claim-validator contract for the {@code jwtDecoder} bean (mtls-rules.md: signature, exp, iss,
 * aud). Signature verification is Nimbus's job and needs a live JWKS; these tests pin the CLAIM
 * layer: a signature-valid token minted by another issuer or for another relying party must still
 * be rejected (confused-deputy), and the default timestamp check must survive the customization.
 */
class SecurityConfigJwtValidatorTest {

    private static final String ISS = "http://localhost:9000";
    private static final String AUD = "chaosforge";

    private final OAuth2TokenValidator<Jwt> validator = SecurityConfig.jwtClaimsValidator(ISS, AUD);

    private static Jwt.Builder freshToken() {
        return Jwt.withTokenValue("t").header("alg", "RS256").subject("lab-user")
                .issuedAt(Instant.now().minusSeconds(30))
                .expiresAt(Instant.now().plusSeconds(300));
    }

    @Test
    void matchingIssuerAndAudience_passes() {
        Jwt jwt = freshToken().issuer(ISS).audience(List.of(AUD)).build();
        assertThat(validator.validate(jwt).hasErrors()).isFalse();
    }

    @Test
    void audienceListContainingOurs_passes() {
        Jwt jwt = freshToken().issuer(ISS).audience(List.of("other-rp", AUD)).build();
        assertThat(validator.validate(jwt).hasErrors()).isFalse();
    }

    @Test
    void wrongIssuer_rejected() {
        Jwt jwt = freshToken().issuer("http://evil-idp").audience(List.of(AUD)).build();
        assertThat(validator.validate(jwt).hasErrors()).isTrue();
    }

    @Test
    void wrongAudience_rejected() {
        Jwt jwt = freshToken().issuer(ISS).audience(List.of("some-other-rp")).build();
        assertThat(validator.validate(jwt).hasErrors()).isTrue();
    }

    @Test
    void missingAudienceClaim_rejected() {
        Jwt jwt = freshToken().issuer(ISS).build();
        assertThat(validator.validate(jwt).hasErrors()).isTrue();
    }

    @Test
    void expiredToken_stillRejected_defaultTimestampValidatorRetained() {
        // JwtTimestampValidator allows 60s clock skew — expire well past it.
        Jwt jwt = Jwt.withTokenValue("t").header("alg", "RS256").subject("lab-user")
                .issuedAt(Instant.now().minusSeconds(600))
                .expiresAt(Instant.now().minusSeconds(300))
                .issuer(ISS).audience(List.of(AUD)).build();
        assertThat(validator.validate(jwt).hasErrors()).isTrue();
    }
}
