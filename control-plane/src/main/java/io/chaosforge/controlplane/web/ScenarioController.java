package io.chaosforge.controlplane.web;

import io.chaosforge.controlplane.domain.Scenario;
import io.chaosforge.controlplane.replay.ReplayCommand;
import io.chaosforge.controlplane.replay.ReplayToken;
import io.chaosforge.controlplane.replay.ScenarioReplayOrchestrator;
import io.chaosforge.controlplane.security.TenantContext;
import io.chaosforge.controlplane.service.ScenarioService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/v1/scenarios")
public class ScenarioController {

    private static final long NO_VERSION = -1L;   // list sentinel; clients GET the scenario for its ETag

    private final ScenarioService scenarioService;
    private final ScenarioReplayOrchestrator orchestrator;

    public ScenarioController(ScenarioService scenarioService, ScenarioReplayOrchestrator orchestrator) {
        this.scenarioService = scenarioService;
        this.orchestrator = orchestrator;
    }

    @PostMapping
    public ResponseEntity<ScenarioResponse> create(@Valid @RequestBody CreateScenarioRequest req) {
        Scenario s = scenarioService.create(req.name(), req.ruleSetId(), req.ruleSetVersion());
        long rv = scenarioService.replayVersion(s.scenarioId());
        return ResponseEntity.status(HttpStatus.CREATED)
                .header(HttpHeaders.ETAG, etag(rv))
                .body(toResponse(s, rv));
    }

    @GetMapping("/{scenarioId}")
    public ResponseEntity<ScenarioResponse> get(@PathVariable UUID scenarioId) {
        Scenario s = scenarioService.get(scenarioId);                  // cross-tenant → 404
        long rv = scenarioService.replayVersion(scenarioId);
        return ResponseEntity.ok().header(HttpHeaders.ETAG, etag(rv)).body(toResponse(s, rv));
    }

    @GetMapping
    public List<ScenarioResponse> list() {
        return scenarioService.list().stream().map(s -> toResponse(s, NO_VERSION)).toList();
    }

    /**
     * Initiate a replay (ADR-0528). {@code If-Match} carries the {@code expectedVersion} from the
     * client's prior GET; {@code Idempotency-Key} (a client-generated UUID per logical attempt) makes
     * a lost-response retry safe. A fresh claim → {@code 202 Accepted} + {@code Location} + {@code ETag};
     * an idempotent replay of a completed key → {@code 200 OK} with the original token. CAS contention
     * → 409 + jittered {@code Retry-After}, handled centrally.
     */
    @PostMapping("/{scenarioId}:run")
    public ResponseEntity<ReplayResponse> run(@PathVariable UUID scenarioId,
                                              @RequestHeader(HttpHeaders.IF_MATCH) String ifMatch,
                                              @RequestHeader("Idempotency-Key") String idempotencyKey) {
        long expectedVersion = parseIfMatch(ifMatch);
        UUID idemKey = parseIdempotencyKey(idempotencyKey);
        ReplayToken token = orchestrator.initiateReplay(
                new ReplayCommand(scenarioId, TenantContext.require(), expectedVersion, idemKey));

        ReplayResponse body = new ReplayResponse(token.scenarioId(), token.fencingToken(),
                token.ruleSetId(), token.ruleSetVersion(), token.outboxMessageId());
        String etag = etag(token.fencingToken());
        if (token.idempotentReplay()) {
            return ResponseEntity.ok().header(HttpHeaders.ETAG, etag).body(body);
        }
        return ResponseEntity.accepted()
                .header(HttpHeaders.LOCATION, "/v1/scenarios/" + scenarioId + "/runs/" + token.fencingToken())
                .header(HttpHeaders.ETAG, etag)
                .body(body);
    }

    private static String etag(long version) {
        return "\"" + version + "\"";
    }

    private static long parseIfMatch(String ifMatch) {
        String v = ifMatch.trim();
        if (v.startsWith("W/")) {
            v = v.substring(2);
        }
        v = v.replace("\"", "").trim();
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "If-Match must be the numeric replay version");
        }
    }

    private static UUID parseIdempotencyKey(String raw) {
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must be a UUID");
        }
    }

    private static ScenarioResponse toResponse(Scenario s, long replayVersion) {
        return new ScenarioResponse(s.scenarioId(), s.tenantId(), s.name(),
                s.ruleSetId(), s.ruleSetVersion(), s.status(), replayVersion);
    }

    public record CreateScenarioRequest(
            @NotBlank String name,
            @NotNull UUID ruleSetId,
            @NotNull Integer ruleSetVersion) {}

    public record ScenarioResponse(
            UUID scenarioId, UUID tenantId, String name,
            UUID ruleSetId, int ruleSetVersion, String status, long replayVersion) {}

    public record ReplayResponse(
            UUID scenarioId, long replayVersion, UUID ruleSetId, int ruleSetVersion, UUID messageId) {}
}
