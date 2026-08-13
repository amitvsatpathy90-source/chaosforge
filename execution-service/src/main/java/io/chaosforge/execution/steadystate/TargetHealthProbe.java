package io.chaosforge.execution.steadystate;

/**
 * Probes whether a target is still healthy — the signal the steady-state hypothesis is measured against
 * (acceptance gate C20). Independent of the fault-injection steps: it answers "is the blast radius
 * still within tolerance?", not "did this injected fault return an error?".
 */
public interface TargetHealthProbe {

    /** @return true if the target's health endpoint responds 2xx; false on any non-2xx, timeout, or error. */
    boolean isHealthy(String healthUrl);
}
