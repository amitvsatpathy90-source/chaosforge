package io.chaosforge.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Edge Gateway — Spring WebFlux (Netty), the only public ingress (ADR-0500). Reactive end-to-end;
 * no blocking calls anywhere (CI grep + ArchUnit enforced). No Spring AI, no JDBC, no L2 cache.
 */
@SpringBootApplication
public class EdgeGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(EdgeGatewayApplication.class, args);
    }
}
