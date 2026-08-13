package io.chaosforge.execution.config;

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
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundleKey;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.ssl.SslStoreBundle;
import org.springframework.boot.ssl.jks.JksSslStoreBundle;
import org.springframework.boot.ssl.jks.JksSslStoreDetails;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/**
 * Proves the Execution Service → Control Plane mTLS wiring (ADR-0531) end-to-end on a real TLS socket,
 * with no Spring context, Postgres, or Kafka:
 *
 * <ol>
 *   <li>The actual {@link HttpClientConfig#controlPlaneRestClient} bean, given the internal-CA bundle,
 *       presents its client certificate and completes a mutual-TLS handshake.</li>
 *   <li>A client that trusts the server but presents NO client certificate is rejected at the handshake —
 *       mirroring {@code server.ssl.client-auth: need}. This is the security guarantee, proven, not asserted.</li>
 * </ol>
 *
 * The server here is a JDK {@link HttpsServer} configured with {@code needClientAuth(true)} — the exact
 * posture of the {@code mtls} profile's {@code server.ssl.client-auth: need}.
 */
class MtlsControlPlaneClientHandshakeTest {

    private static final String PASS = "changeit";

    @TempDir
    static Path certs;

    static SslBundle fullBundle;       // keystore (service key+cert) + truststore (CA) → presents a client cert
    static SslBundle trustOnlyBundle;  // truststore only (no key)      → trusts server, presents NO client cert
    static HttpsServer server;
    static String baseUrl;

    @BeforeAll
    static void setUp() throws Exception {
        Path keystore = certs.resolve("service.p12");
        Path truststore = certs.resolve("truststore.p12");
        Path exported = certs.resolve("service.cer");

        // Self-signed service identity; truststore trusts it. (The production chain is CA-signed via
        // docker/mtls/generate-certs.sh; this test isolates the Spring wiring + need-client-auth behaviour.)
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
        SslStoreBundle trustOnly = new JksSslStoreBundle(null, tsDetails);   // null keystore → no client cert
        trustOnlyBundle = SslBundle.of(trustOnly, SslBundleKey.NONE);

        SSLContext ctx = fullBundle.createSslContext();
        server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(ctx) {
            @Override
            public void configure(HttpsParameters params) {
                SSLParameters sslParams = ctx.getDefaultSSLParameters();
                sslParams.setNeedClientAuth(true);   // == server.ssl.client-auth: need
                params.setSSLParameters(sslParams);
            }
        });
        server.createContext("/internal/ruleset", exchange -> {
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
    void controlPlaneRestClient_withMtlsBundle_completesMutualTlsHandshake() {
        DefaultSslBundleRegistry registry = new DefaultSslBundleRegistry("internal-mtls", fullBundle);
        @SuppressWarnings("unchecked")
        ObjectProvider<SslBundles> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(registry);

        RestClient client = new HttpClientConfig().controlPlaneRestClient(baseUrl, "internal-mtls", provider);

        String body = client.get().uri("/internal/ruleset").retrieve().body(String.class);
        assertThat(body).isEqualTo("ok");
    }

    @Test
    void needClientAuthServer_rejectsClientThatPresentsNoCertificate() {
        // Trusts the server cert, but holds no key → presents no client cert. `need` must reject it.
        HttpClientSettings settings = HttpClientSettings.defaults().withSslBundle(trustOnlyBundle);
        ClientHttpRequestFactory factory = ClientHttpRequestFactoryBuilder.detect().build(settings);
        RestClient noCert = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();

        assertThatThrownBy(() -> noCert.get().uri("/internal/ruleset").retrieve().body(String.class))
                .isInstanceOf(ResourceAccessException.class);
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
