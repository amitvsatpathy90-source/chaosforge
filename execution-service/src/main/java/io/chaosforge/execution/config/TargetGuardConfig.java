package io.chaosforge.execution.config;

import io.chaosforge.common.target.TargetUrlGuard;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the {@link TargetUrlGuard} applied to every pinned step target during execution (ADR-0534).
 *
 * <p>The guard is permissive by default for dev/tests/local Compose. Deployed profiles enable
 * private-network blocking and pin {@code allowed-hosts}.
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
