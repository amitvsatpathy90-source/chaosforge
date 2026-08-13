package io.chaosforge.execution.control;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.jspecify.annotations.Nullable;

/**
 * Operator control for the executor {@link KillSwitch} (acceptance gate C19). A management/admin HTTP
 * endpoint — channel-secured by intra-service mTLS and authenticated by the re-verified JWT (ADR-0524).
 * Because engaging it aborts every in-flight run across all tenants, it requires the {@code OPERATOR}
 * role, not merely an authenticated token (arch-audit M2 — see the exec {@code SecurityConfig}). The
 * Kafka consumer path is unaffected; this only flips the in-memory flag the executor reads between steps.
 */
@RestController
@RequestMapping("/internal/kill-switch")
public class KillSwitchController {

    private final KillSwitch killSwitch;

    public KillSwitchController(KillSwitch killSwitch) {
        this.killSwitch = killSwitch;
    }

    /** Engage the kill switch — in-flight runs abort at the next step boundary, new runs do no steps. */
    @PostMapping
    public KillSwitch.State engage(@RequestBody EngageRequest request) {
        killSwitch.engage(request.reason());
        return killSwitch.state();
    }

    /** Disengage — fault injection resumes for subsequently executed commands. */
    @DeleteMapping
    public KillSwitch.State disengage() {
        killSwitch.disengage();
        return killSwitch.state();
    }

    @GetMapping
    public KillSwitch.State state() {
        return killSwitch.state();
    }

    public record EngageRequest(@Nullable String reason) {}
}
