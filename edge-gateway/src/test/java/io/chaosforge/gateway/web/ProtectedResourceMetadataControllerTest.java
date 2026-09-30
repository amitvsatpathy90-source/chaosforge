package io.chaosforge.gateway.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;

class ProtectedResourceMetadataControllerTest {

    private final ProtectedResourceMetadataController controller =
            new ProtectedResourceMetadataController("http://localhost:9000");

    private static final MockServerHttpRequest REQUEST =
            MockServerHttpRequest.get("http://localhost:8080/.well-known/oauth-protected-resource").build();

    @Test
    void advertisesResourceDerivedFromTheRequest_andTheIssuer() {
        var metadata = controller.metadata(REQUEST);

        assertThat(metadata.get("resource")).isEqualTo("http://localhost:8080/mcp");
        assertThat(metadata.get("authorization_servers")).isEqualTo(List.of("http://localhost:9000"));
    }

    @Test
    void advertisesOnlyTheReadScope_notOperateOrDlq() {
        assertThat(controller.metadata(REQUEST).get("scopes_supported")).isEqualTo(List.of("chaosforge.read"));
    }

    @Test
    void advertisesHeaderAsTheOnlyBearerMethod() {
        assertThat(controller.metadata(REQUEST).get("bearer_methods_supported")).isEqualTo(List.of("header"));
    }
}
