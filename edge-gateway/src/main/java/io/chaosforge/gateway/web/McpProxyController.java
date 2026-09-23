package io.chaosforge.gateway.web;

import io.chaosforge.gateway.client.ControlPlaneClient;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Thin MCP transport proxy. Gateway handles ingress security and rate limiting; Control Plane owns MCP
 * authentication, authorization, and protocol semantics.
 */
@RestController
@RequestMapping("/mcp")
public class McpProxyController {

    private final ControlPlaneClient controlPlane;

    public McpProxyController(ControlPlaneClient controlPlane) {
        this.controlPlane = controlPlane;
    }

    /**
     * Forwards the opaque MCP stream and required transport headers unchanged.
     */
    @PostMapping
    public Mono<ResponseEntity<Flux<DataBuffer>>> forward(
            @RequestBody Flux<DataBuffer> body,
            @RequestHeader(HttpHeaders.AUTHORIZATION) String authorization,
            @RequestHeader(HttpHeaders.CONTENT_TYPE) String contentType,
            @RequestHeader(value = HttpHeaders.ACCEPT, required = false) String accept,
            @RequestHeader(value = "MCP-Protocol-Version", required = false) String protocolVersion) {

        return controlPlane.forwardMcp(
                body,
                authorization,
                contentType,
                accept,
                protocolVersion);
    }
}
