package io.chaosforge.controlplane.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.chaosforge.controlplane.it.VirtualThreadPinningRecorder.PinningReport;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * VT-pinning harness for the Control Plane's JDBC path (build-prompt §49; the deferred item the
 * {@code application-dev.yml} comments forward-reference: "the Testcontainers suite asserts virtual
 * thread pinning == 0 under load").
 *
 * <p><b>What it proves.</b> The CP runs MVC on Java 21 virtual threads with JDBC executed directly on
 * the request VT (architecture specifications "option 1 — accept pinning"; there is no dedicated {@code jdbcExecutor}).
 * On JDK 21 a {@code synchronized} section that blocks pins its carrier, so whether this is actually
 * safe hinges on whether the real driver+pool stack holds a monitor across a blocking call. This test
 * <i>measures</i> that empirically instead of asserting it: it drives an adversarial JDBC workload —
 * 64 virtual threads contending on a single hot row ({@code SELECT … FOR UPDATE} held across a
 * server-side sleep) through a deliberately small Hikari pool, so every VT blocks both on connection
 * acquisition and on the row lock — while a {@link VirtualThreadPinningRecorder} captures
 * {@code jdk.VirtualThreadPinned} events (pinned <i>and</i> blocked — the carrier-starving kind).
 *
 * <p>A clean result (zero events) is the load-bearing claim: PgJDBC {@code 42.7.x} + HikariCP {@code 7}
 * use {@code ReentrantLock}, not {@code synchronized}, around blocking I/O, so even on JDK 21 the
 * request VT unmounts cleanly while waiting on the socket or the pool — option 1 needs no dedicated
 * executor. A regression to a pinning driver/pool would surface here as a non-zero count with the
 * offending frame named, before it ever starves a carrier in production.
 *
 * <p>No Spring context, no Flyway, no mocks — a real Postgres, the real production pool, real sockets.
 */
class VirtualThreadJdbcPinningIT {

    private static final int VTHREADS = 64;
    private static final int POOL_SIZE = 8;       // < VTHREADS, so most VTs also block acquiring a connection
    private static final String HOT_ROW_LOCK = "SELECT n FROM pin_probe WHERE id = 1 FOR UPDATE";
    private static final String HOLD_LOCK = "SELECT pg_sleep(0.002)";   // hold the row lock ~2ms → real waiters
    private static final String BUMP = "UPDATE pin_probe SET n = n + 1 WHERE id = 1";

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16.4-alpine");

    private static HikariDataSource ds;

    @BeforeAll
    static void setUp() throws Exception {
        PG.start();   // Testcontainers Ryuk reaps it at JVM exit
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(PG.getJdbcUrl());
        cfg.setUsername(PG.getUsername());
        cfg.setPassword(PG.getPassword());
        cfg.setMaximumPoolSize(POOL_SIZE);
        cfg.setConnectionTimeout(TimeUnit.SECONDS.toMillis(30));
        // Mirror the production connection-init (application.yml): a per-connection SET, so the harness
        // also exercises whatever the driver does on hand-out, not just the steady-state query path.
        cfg.setConnectionInitSql("SET statement_timeout = '5s'");
        ds = new HikariDataSource(cfg);
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE pin_probe (id INT PRIMARY KEY, n BIGINT NOT NULL)");
            st.execute("INSERT INTO pin_probe (id, n) VALUES (1, 0)");
        }
    }

    @AfterAll
    static void tearDown() {
        if (ds != null) {
            ds.close();
        }
    }

    @Test
    void jdbcOnVirtualThreads_underRowLockAndPoolContention_doesNotPinCarriers() throws Exception {
        List<Throwable> failures = new CopyOnWriteArrayList<>();

        PinningReport report = VirtualThreadPinningRecorder.record(() -> runContendedWorkload(failures));

        // The workload must have actually executed (else a clean pinning report would be vacuous).
        assertThat(failures)
                .as("every contended JDBC transaction should commit without error")
                .isEmpty();
        assertThat(committedCount())
                .as("all %d virtual-thread transactions committed against the hot row", VTHREADS)
                .isEqualTo(VTHREADS);

        // The actual claim: blocking JDBC on request VTs never pins a carrier (PgJDBC 42.7 + Hikari 7
        // use ReentrantLock around blocking I/O, so the VT unmounts while it waits). A non-zero count
        // names the offending frame — that is the signal to move JDBC to a dedicated executor (option 2).
        assertThat(report.clean())
                .as("harmful virtual-thread pins under load (longest=%s): %s",
                        report.longestPin(), report.sampleSites())
                .isTrue();
    }

    /**
     * Negative control. A "zero pins" result is only meaningful if the recorder can detect a pin at
     * all, so here we cause one deliberately: a virtual thread that blocks ({@code Thread.sleep})
     * <i>inside</i> a {@code synchronized} block cannot unmount on JDK 21, so it pins its carrier and
     * must fire {@code jdk.VirtualThreadPinned}. If this ever reports zero, the harness is blind and
     * the clean JDBC result above cannot be trusted.
     */
    @Test
    void recorder_detectsDeliberatePinning_soACleanJdbcResultIsMeaningful() throws Exception {
        Object monitor = new Object();

        PinningReport report = VirtualThreadPinningRecorder.record(() -> {
            Thread vt = Thread.ofVirtual().start(() -> {
                synchronized (monitor) {           // entering a monitor pins the carrier...
                    try {
                        Thread.sleep(50);          // ...and blocking while pinned starves it → event fires
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            vt.join();
        });

        assertThat(report.eventCount())
                .as("a synchronized+blocking virtual thread must register as a harmful pin; sites=%s",
                        report.sampleSites())
                .isGreaterThanOrEqualTo(1);
        assertThat(report.sampleSites())
                .as("the pin site should name this test's frame, not be anonymous")
                .anyMatch(s -> s.contains("VirtualThreadJdbcPinningIT"));
    }

    private void runContendedWorkload(List<Throwable> failures) throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(VTHREADS);
        try (ExecutorService vts = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < VTHREADS; i++) {
                vts.submit(() -> {
                    try {
                        start.await();           // release all VTs together → maximal contention
                        oneContendedTransaction();
                    } catch (Throwable t) {
                        failures.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(60, TimeUnit.SECONDS))
                    .as("all virtual-thread transactions finished within the timeout")
                    .isTrue();
        }
    }

    /** One transaction: acquire a pooled connection, take the hot-row lock, hold it, bump, commit. */
    private void oneContendedTransaction() throws Exception {
        try (Connection c = ds.getConnection()) {       // blocks on the pool when exhausted
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                st.execute(HOT_ROW_LOCK);                // blocks on the row lock behind other VTs
                st.execute(HOLD_LOCK);                   // hold it long enough that waiters truly block
                st.executeUpdate(BUMP);
            }
            c.commit();
        }
    }

    private long committedCount() throws Exception {
        try (Connection c = ds.getConnection();
                Statement st = c.createStatement();
                var rs = st.executeQuery("SELECT n FROM pin_probe WHERE id = 1")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
