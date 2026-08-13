package io.chaosforge.controlplane.it;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordingFile;

/**
 * In-process detector for <b>harmful</b> virtual-thread pinning.
 *
 * <p>It records the JDK Flight Recorder event {@code jdk.VirtualThreadPinned}, which fires <i>only</i>
 * when a virtual thread has to <b>block</b> (park) while it is pinned to its carrier — i.e. it cannot
 * unmount because it is inside a {@code synchronized} block or a native frame. That is exactly the
 * condition that starves the carrier pool; a pin that never blocks is harmless and is (correctly) not
 * reported. The event therefore measures the thing we actually care about, not every pin.
 *
 * <p>On JDK 21 a blocking {@code synchronized} section pins (JEP 491's "{@code synchronized} doesn't
 * pin" only landed in JDK 24), so on this toolchain the event is a faithful probe of whether the real
 * JDBC stack (PgJDBC + HikariCP) holds a monitor across a blocking call. The threshold is set to zero
 * so even sub-millisecond blocking-while-pinned is captured, and stack traces are kept so a positive
 * result points at the exact frame.
 *
 * <p>We use a dump-to-file {@link Recording} (read back via {@link RecordingFile}) rather than a live
 * {@code RecordingStream} so capture is deterministic — no periodic-flush race between the workload
 * finishing and the assertion reading events.
 */
final class VirtualThreadPinningRecorder {

    private static final String EVENT = "jdk.VirtualThreadPinned";

    private VirtualThreadPinningRecorder() {}

    /** Result of one recording window: how many harmful pins fired, and where (for diagnostics). */
    record PinningReport(int eventCount, List<String> sampleSites, Duration longestPin) {
        boolean clean() {
            return eventCount == 0;
        }
    }

    /**
     * Runs {@code workload} with a {@code jdk.VirtualThreadPinned} recording active and returns what
     * fired. The workload is expected to throw nothing; any throwable propagates after the recording
     * is closed so the caller still sees the failure.
     */
    static PinningReport record(ThrowingRunnable workload) throws Exception {
        Path jfr = Files.createTempFile("vt-pinning", ".jfr");
        try {
            try (Recording r = new Recording()) {
                // Empty config: nothing is enabled by default, so we pay for only this one event.
                r.enable(EVENT).withThreshold(Duration.ofMillis(0)).withStackTrace();
                r.start();
                try {
                    workload.run();
                } finally {
                    r.stop();          // flushes in-flight events into the recording
                    r.dump(jfr);       // before close(): write the recording out for deterministic read-back
                }
            }
            return summarise(RecordingFile.readAllEvents(jfr));
        } finally {
            Files.deleteIfExists(jfr);
        }
    }

    private static PinningReport summarise(List<RecordedEvent> events) {
        Map<String, Integer> sites = new LinkedHashMap<>();
        Duration longest = Duration.ZERO;
        for (RecordedEvent e : events) {
            Duration d = e.getDuration();
            if (d != null && d.compareTo(longest) > 0) {
                longest = d;
            }
            sites.merge(firstAppFrame(e), 1, Integer::sum);
        }
        List<String> samples = new ArrayList<>();
        sites.forEach((site, count) -> samples.add(count + "× " + site));
        return new PinningReport(events.size(), samples, longest);
    }

    /** First non-JDK stack frame, so a positive result names the library that pinned (e.g. PgJDBC). */
    private static String firstAppFrame(RecordedEvent e) {
        if (e.getStackTrace() == null) {
            return "<no stack>";
        }
        for (RecordedFrame f : e.getStackTrace().getFrames()) {
            if (!f.isJavaFrame() || f.getMethod() == null) {
                continue;
            }
            String cls = f.getMethod().getType().getName();
            if (cls.startsWith("java.") || cls.startsWith("jdk.") || cls.startsWith("sun.")) {
                continue;
            }
            return cls + "#" + f.getMethod().getName();
        }
        // All-JDK stack (e.g. a bare socketRead under a JDK monitor): name the top Java frame.
        for (RecordedFrame f : e.getStackTrace().getFrames()) {
            if (f.isJavaFrame() && f.getMethod() != null) {
                return f.getMethod().getType().getName() + "#" + f.getMethod().getName();
            }
        }
        return "<native>";
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }
}
