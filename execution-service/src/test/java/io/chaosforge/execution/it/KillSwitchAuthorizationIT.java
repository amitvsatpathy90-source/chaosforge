package io.chaosforge.execution.it;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import io.chaosforge.execution.control.KillSwitch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Kill-switch authorization (arch-audit M2). Engaging the switch is a GLOBAL operator action — it aborts
 * every in-flight run across all tenants — so it must require the {@code OPERATOR} role, not merely an
 * authenticated tenant token (the gap: {@code anyRequest().authenticated()} let any valid JWT trip it).
 *
 * <p>Mirrors the CP {@code InternalMtlsAuthorizationIT} idiom: a full context with the real exec security
 * filter chain, MockMvc built via {@code springSecurity()}, and {@code jwt()} post-processors injecting
 * authorities to exercise the {@code authorizeHttpRequests} rule. {@code KillSwitch} is mocked so the
 * assertions are purely about the authorization decision and no global state leaks across methods.
 *
 * <ul>
 *   <li>OPERATOR token → engage/read reach the controller (200);</li>
 *   <li>plain tenant token → 403;</li>
 *   <li>no token → 401.</li>
 * </ul>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = {"chaosforge.scenario.commands.v1", "chaosforge.scenario.results.v1"})
@Testcontainers
class KillSwitchAuthorizationIT {

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16.4-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", PG::getJdbcUrl);
        registry.add("spring.datasource.username", PG::getUsername);
        registry.add("spring.datasource.password", PG::getPassword);
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri",
                () -> "http://localhost:0/.well-known/jwks.json");
    }

    @MockitoBean
    private KillSwitch killSwitch;   // authz-only test — no global state, behaviour is covered by KillSwitchTest

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mvc;

    private static final String ENGAGE_BODY = "{\"reason\":\"game-day\"}";

    @BeforeEach
    void buildMvc() {
        mvc = webAppContextSetup(wac).apply(springSecurity()).build();
    }

    @Test
    void operatorToken_canEngage() throws Exception {
        mvc.perform(post("/internal/kill-switch")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_OPERATOR")))
                        .contentType(MediaType.APPLICATION_JSON).content(ENGAGE_BODY))
                .andExpect(status().isOk());
    }

    @Test
    void tenantToken_isForbiddenFromEngaging() throws Exception {
        // The exact vulnerability: an ordinary authenticated tenant must NOT trip a global halt.
        mvc.perform(post("/internal/kill-switch")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_TENANT")))
                        .contentType(MediaType.APPLICATION_JSON).content(ENGAGE_BODY))
                .andExpect(status().isForbidden());
    }

    @Test
    void tenantToken_isForbiddenFromReadingState() throws Exception {
        mvc.perform(get("/internal/kill-switch")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_TENANT"))))
                .andExpect(status().isForbidden());
    }

    @Test
    void noToken_isUnauthorized() throws Exception {
        mvc.perform(get("/internal/kill-switch")).andExpect(status().isUnauthorized());
    }
}
