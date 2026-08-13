package io.chaosforge.execution.config;

import io.chaosforge.common.target.TargetUrlGuard;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the {@link TargetUrlGuard} the executor applies to every pinned step target (arch-audit HIGH-2).
 * Off by default so dev/tests/local-Compose can target localhost; deployed profiles set
 * {@code chaosforge.target.block-private-networks=true} and populate {@code allowed-hosts}
 * (see {@code application-mtls.yml} and {@code target-validation-rules.md}).
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
