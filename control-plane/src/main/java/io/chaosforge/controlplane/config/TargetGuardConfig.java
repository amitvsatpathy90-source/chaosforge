package io.chaosforge.controlplane.config;

import io.chaosforge.common.target.TargetUrlGuard;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the {@link TargetUrlGuard} the Control Plane applies to rule-set targets at authoring time
 * (arch-audit HIGH-2) — the same policy the Execution Service enforces at execution time, so a scenario
 * is rejected early rather than only when it runs. Off by default (dev/tests target localhost); deployed
 * profiles set {@code chaosforge.target.block-private-networks=true} — see {@code application-mtls.yml}.
 */
@Configuration
public class TargetGuardConfig {

    @Bean
    public TargetUrlGuard targetUrlGuard(
            @Value("${chaosforge.target.block-private-networks:false}") boolean blockPrivateNetworks,
            @Value("${chaosforge.target.allowed-hosts:}") List<String> allowedHosts) {
        return new TargetUrlGuard(blockPrivateNetworks, allowedHosts);
    }
}
