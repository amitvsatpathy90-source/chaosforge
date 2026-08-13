package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.filter.ServerHttpObservationFilter;

/**
 * Proves the trace_id/MDC mandate (rules files: "structured log fields: trace_id") is real, not
 * aspirational: a request through the observation filter produces application log lines whose MDC
 * carries a non-blank {@code traceId}.
 *
 * <p>What this proves: the Boot 4 tracing autoconfig module is present (the injected
 * {@link ServerHttpObservationFilter} registration would not exist without it — the module-split
 * regression guard, same lesson as ADR-0530's Flyway test), and the Brave bridge populates MDC
 * correlation for logs emitted inside an observed request.
 *
 * <p>What this does NOT prove: servlet filter <em>order</em> in a real server (MockMvc applies the
 * observation filter explicitly), nor gateway (reactive) or exec (Kafka listener) MDC — those share
 * the same bridge but are wired via {@code spring.reactor.context-propagation=auto} and
 * {@code setObservationEnabled(true)} respectively.
 */
@Import(TraceIdMdcIT.TraceProbeConfig.class)
class TraceIdMdcIT extends AbstractCpIntegrationTest {

    @Autowired
    private WebApplicationContext wac;

    @Autowired   // fails the context — and this test — if the tracing/observation autoconfig is absent
    private FilterRegistrationBean<ServerHttpObservationFilter> observationFilterRegistration;

    private MockMvc mvc;
    private ListAppender<ILoggingEvent> capturedLogs;

    @BeforeEach
    void setUp() {
        mvc = webAppContextSetup(wac)
                .addFilters(observationFilterRegistration.getFilter())
                .apply(springSecurity())
                .build();
        capturedLogs = new ListAppender<>();
        capturedLogs.start();
        probeLogger().addAppender(capturedLogs);
    }

    @AfterEach
    void tearDown() {
        probeLogger().detachAppender(capturedLogs);
    }

    @Test
    void requestPathLogLine_carriesTraceIdInMdc() throws Exception {
        mvc.perform(get("/internal/trace-probe")).andExpect(status().isOk());

        ILoggingEvent probeLine = capturedLogs.list.stream()
                .filter(e -> e.getFormattedMessage().contains("trace probe"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("probe log line was not captured"));
        assertThat(probeLine.getMDCPropertyMap().get("traceId"))
                .as("traceId in MDC of a request-path log line")
                .isNotBlank()
                .matches("[0-9a-f]{16,32}");
    }

    private static Logger probeLogger() {
        return (Logger) LoggerFactory.getLogger(TraceProbeConfig.TraceProbeController.class);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TraceProbeConfig {

        /**
         * Test-only endpoint on the permitAll {@code /internal} path; logs inside the request.
         * Registered automatically as an annotated member class of this configuration — no
         * {@code @Bean} method (that would double-register it → ambiguous mapping).
         */
        @RestController
        static class TraceProbeController {
            private static final org.slf4j.Logger log =
                    LoggerFactory.getLogger(TraceProbeController.class);

            @GetMapping("/internal/trace-probe")
            String probe() {
                log.info("trace probe");
                return "ok";
            }
        }
    }
}
