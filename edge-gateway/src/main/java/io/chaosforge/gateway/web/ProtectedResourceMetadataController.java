package io.chaosforge.gateway.web;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.oauth2.server.resource.OAuth2ProtectedResourceMetadata;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * RFC 9728 discovery endpoint for the MCP resource boundary.
 *
 * <p>resource MUST be an absolute URL — Spring enforces this at build time (confirmed empirically;
 * the bare "chaosforge-mcp" audience string is not valid here). Built per-request from the request's
 * own scheme/authority, same approach as the 401 entry point in SecurityConfig: this lab has no fixed
 * public URL, so a static config value would either be wrong in some environments or need its own
 * env var kept in sync with nothing real. resource and the JWT aud claim are deliberately different
 * values — aud is the internal audience check, resource is the published discovery identifier.
 *
 * <p>Only the currently MCP-exposed read scope is advertised — chaosforge.operate/chaosforge.dlq stay
 * undisclosed by design (scopes_supported is what the resource server is willing to disclose, not
 * everything it accepts).
 */
@RestController
public class ProtectedResourceMetadataController {

    private final String issuer;

    public ProtectedResourceMetadataController(@Value("${chaosforge.security.jwt.issuer}") String issuer) {
        this.issuer = issuer;
    }

    @GetMapping("/.well-known/oauth-protected-resource")
    public Map<String, Object> metadata(ServerHttpRequest request) {
        String resource = request.getURI().getScheme() + "://" + request.getURI().getAuthority() + "/mcp";
        return OAuth2ProtectedResourceMetadata.builder()
                .resource(resource)
                .authorizationServer(issuer)
                .scope("chaosforge.read")
                .bearerMethod("header")
                .build()
                .getClaims();
    }
}
