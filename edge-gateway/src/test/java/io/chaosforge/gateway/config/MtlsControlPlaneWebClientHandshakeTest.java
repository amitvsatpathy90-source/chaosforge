package io.chaosforge.gateway.config;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.boot.http.client.reactive.ClientHttpConnectorBuilder;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundleKey;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.ssl.SslStoreBundle;
import org.springframework.boot.ssl.jks.JksSslStoreBundle;
import org.springframework.boot.ssl.jks.JksSslStoreDetails;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;

/**
 * Proves the Edge Gateway → Control Plane mTLS wiring (ADR-0531) on a real TLS socket, exercising the
 * reactive (Netty) connector path that {@link GatewayConfig#controlPlaneWebClient} builds:
 *
 * <ol>
 *   <li>The actual bean, given the internal-CA bundle, presents its client certificate and completes a
 *       mutual-TLS handshake against a {@code needClientAuth(true)} server.</li>
 *   <li>A connector that trusts the server but presents no client certificate is rejected at the handshake
 *       — mirroring {@code server.ssl.client-auth: need} at the Control Plane.</li>
 * </ol>
 *
 * {@code .block()} is used here because this is a test; the gateway's {@code src/main} stays reactive
 * end-to-end (ArchUnit-enforced).
 */
class MtlsControlPlaneWebClientHandshakeTest {

    private static final String PASS = "changeit";

    @TempDir
    static Path certs;

    static SslBundle fullBundle;
    static SslBundle trustOnlyBundle;
    static HttpsServer server;
    static String baseUrl;

    @BeforeAll
    static void setUp() throws Exception {
        Path keystore = certs.resolve("service.p12");
        Path truststore = certs.resolve("truststore.p12");
        Path exported = certs.resolve("service.cer");

        keytool("-genkeypair", "-alias", "service", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
                "-dname", "CN=localhost,O=ChaosForge", "-ext", "san=dns:localhost,ip:127.0.0.1",
                "-keystore", keystore.toString(), "-storetype", "PKCS12",
                "-storepass", PASS, "-keypass", PASS);
        keytool("-exportcert", "-alias", "service", "-keystore", keystore.toString(),
                "-storetype", "PKCS12", "-storepass", PASS, "-rfc", "-file", exported.toString());
        keytool("-importcert", "-noprompt", "-alias", "service", "-file", exported.toString(),
                "-keystore", truststore.toString(), "-storetype", "PKCS12", "-storepass", PASS);

        JksSslStoreDetails ksDetails =
                new JksSslStoreDetails("PKCS12", null, keystore.toUri().toString(), PASS);
        JksSslStoreDetails tsDetails =
                new JksSslStoreDetails("PKCS12", null, truststore.toUri().toString(), PASS);

        fullBundle = SslBundle.of(new JksSslStoreBundle(ksDetails, tsDetails), SslBundleKey.of(PASS, "service"));
        SslStoreBundle trustOnly = new JksSslStoreBundle(null, tsDetails);
        trustOnlyBundle = SslBundle.of(trustOnly, SslBundleKey.NONE);

        SSLContext ctx = fullBundle.createSslContext();
        server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(ctx) {
            @Override
            public void configure(HttpsParameters params) {
                SSLParameters sslParams = ctx.getDefaultSSLParameters();
                sslParams.setNeedClientAuth(true);
                params.setSSLParameters(sslParams);
            }
        });
        server.createContext("/v1/tenants", exchange -> {
            byte[] body = "ok".getBytes(UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.setExecutor(null);
        server.start();
        baseUrl = "https://localhost:" + server.getAddress().getPort();
    }

    @AfterAll
    static void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void controlPlaneWebClient_withMtlsBundle_completesMutualTlsHandshake() {
        DefaultSslBundleRegistry registry = new DefaultSslBundleRegistry("internal-mtls", fullBundle);
        @SuppressWarnings("unchecked")
        ObjectProvider<SslBundles> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(registry);

        // Plain WebClient.builder() stands in for the auto-configured (observation-wired) builder —
        // this test proves the mTLS handshake, not tracing; the connector logic is identical.
        WebClient client = new GatewayConfig()
                .controlPlaneWebClient(baseUrl, "internal-mtls", provider, WebClient.builder());

        String body = client.get().uri("/v1/tenants").retrieve().bodyToMono(String.class)
                .block(Duration.ofSeconds(10));
        assertThat(body).isEqualTo("ok");
    }

    @Test
    void needClientAuthServer_rejectsConnectorThatPresentsNoCertificate() {
        ClientHttpConnector connector = ClientHttpConnectorBuilder.reactor()
                .build(HttpClientSettings.defaults().withSslBundle(trustOnlyBundle));
        WebClient noCert = WebClient.builder().baseUrl(baseUrl).clientConnector(connector).build();

        assertThatThrownBy(() -> noCert.get().uri("/v1/tenants").retrieve().bodyToMono(String.class)
                .block(Duration.ofSeconds(10)))
                .isInstanceOf(WebClientRequestException.class);
    }

    private static void keytool(String... args) throws Exception {
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        String[] cmd = new String[args.length + 1];
        cmd[0] = keytool;
        System.arraycopy(args, 0, cmd, 1, args.length);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes(), UTF_8);
        int code = p.waitFor();
        if (code != 0) {
            throw new IllegalStateException("keytool exited " + code + ":\n" + output);
        }
    }
}
