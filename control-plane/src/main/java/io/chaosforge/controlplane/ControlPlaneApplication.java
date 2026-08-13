package io.chaosforge.controlplane;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Control Plane — Spring MVC + Java 21 virtual threads (ADR-0500).
 * CRUD for Tenant / Scenario / RuleSet plus the CAS replay critical section (ADR-0522).
 * No reactive code anywhere in this service.
 */
@SpringBootApplication
@EnableScheduling   // outbox poller (added in the outbox-relay step)
public class ControlPlaneApplication {

    public static void main(String[] args) {
        SpringApplication.run(ControlPlaneApplication.class, args);
    }
}
