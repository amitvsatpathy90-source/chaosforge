package io.chaosforge.controlplane.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.preauth.x509.SubjectX500PrincipalExtractor;

/**
 * Scopes each CP {@code /internal/**} path to the one client-cert subject allowed to call it (ADR-0532).
 * mTLS proves a valid internal-CA peer, not which tenant it claims — so authorization binds path to
 * peer: rule-sets → exec CN, tenant policy → gateway CN, anything else → denyAll (fail-closed).
 * Active only when {@code chaosforge.mtls.internal-peer-cn} is set (the mtls profile); otherwise
 * {@code /internal} falls through to the main chain's permitAll for dev/tests.
 */
@Configuration
@ConditionalOnProperty(name = "chaosforge.mtls.internal-peer-cn")
public class InternalMtlsSecurityConfig {

    static final String INTERNAL_EXEC_ROLE = "INTERNAL_EXEC";
    static final String INTERNAL_GATEWAY_ROLE = "INTERNAL_GATEWAY";

    /** @Order(1) chain scoped to /internal/** — evaluated before the main chain's permitAll. */
    @Bean
    @Order(1)
    public SecurityFilterChain internalMtlsFilterChain(
            HttpSecurity http,
            @Value("${chaosforge.mtls.internal-peer-cn}") String execCn,
            @Value("${chaosforge.mtls.internal-gateway-cn:}") String gatewayCn) throws Exception {
        http
            .securityMatcher("/internal/**")
            .csrf(AbstractHttpConfigurer::disable)
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/internal/rule-sets/**").hasRole(INTERNAL_EXEC_ROLE)
                .requestMatchers("/internal/tenants/*/policy").hasRole(INTERNAL_GATEWAY_ROLE)
                .anyRequest().denyAll())
            // Extractor pulls the cert's subject CN; the UDS below turns CN into a per-path role.
            .x509(x509 -> x509
                .x509PrincipalExtractor(new SubjectX500PrincipalExtractor())
                .userDetailsService(internalPeerUserDetailsService(execCn, gatewayCn)));
        return http.build();
    }

    /** Maps client-cert CN to its internal role; any other CN throws → 403. Password unused (X.509 auths). */
    UserDetailsService internalPeerUserDetailsService(String execCn, String gatewayCn) {
        return certCommonName -> {
            if (execCn.equals(certCommonName)) {
                return internalPeer(certCommonName, INTERNAL_EXEC_ROLE);
            }
            if (!gatewayCn.isBlank() && gatewayCn.equals(certCommonName)) {
                return internalPeer(certCommonName, INTERNAL_GATEWAY_ROLE);
            }
            throw new UsernameNotFoundException("client cert CN is not an authorized internal peer");
        };
    }

    private static UserDetails internalPeer(String cn, String role) {
        return User.withUsername(cn).password("").authorities("ROLE_" + role).build();
    }
}
