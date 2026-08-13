package io.chaosforge.controlplane.ai;

/**
 * Advisory triage verdict for one DLQ record (ai-rules.md Tier 2). The AI advises; a human ops
 * engineer decides — nothing in this type (or anything that produces it) can replay, ack, or
 * mutate broker/DB state.
 */
public record DlqTriageResult(String hypothesis, SuggestedAction suggestedAction) {

    public enum SuggestedAction { DISCARD, INVESTIGATE, REPLAY_WHEN_READY }
}
