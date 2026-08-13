package io.chaosforge.execution.ruleset;

import java.util.List;
import java.util.UUID;

/** A pinned, immutable rule set (ADR-0503). Cached forever by (ruleSetId, version) — never invalidated. */
public record RuleSetRef(UUID ruleSetId, int version, List<StepSpec> steps) {
}
