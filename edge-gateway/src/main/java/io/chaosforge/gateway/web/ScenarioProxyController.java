package io.chaosforge.gateway.web;

import io.chaosforge.gateway.client.ControlPlaneClient;
import java.util.UUID;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Forwards scenario reads and replay requests to the Control Plane (after JWT validation + rate
 * limiting have run as filters). The {@code Authorization} header is passed through so CP re-verifies
 * the JWT (ADR-0524); {@code If-Match} and {@code Idempotency-Key} are forwarded so CP can run the
 * CAS + at-most-once claim (ADR-0528); the CP's 409 + {@code Retry-After} is relayed unchanged.
 */
@RestController
@RequestMapping("/v1/scenarios")
public class ScenarioProxyController {

    private final ControlPlaneClient controlPlane;

    public ScenarioProxyController(ControlPlaneClient controlPlane) {
        this.controlPlane = controlPlane;
    }

    @GetMapping("/{scenarioId}")
    public Mono<ResponseEntity<String>> get(@PathVariable UUID scenarioId,
                                            @RequestHeader(HttpHeaders.AUTHORIZATION) String authorization) {
        return controlPlane.getScenario(scenarioId, authorization);
    }

    @PostMapping("/{scenarioId}:run")
    public Mono<ResponseEntity<String>> run(@PathVariable UUID scenarioId,
                                            @RequestHeader(HttpHeaders.AUTHORIZATION) String authorization,
                                            @RequestHeader(HttpHeaders.IF_MATCH) String ifMatch,
                                            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        return controlPlane.initiateReplay(scenarioId, authorization, ifMatch, idempotencyKey);
    }
}
