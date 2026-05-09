package fi.hsl.pulsar.mqtt.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class FailFastShutdown {

    private static final Logger log = LoggerFactory.getLogger(FailFastShutdown.class);

    /**
     * Minimum uptime before the shutdown thread calls {@link SpringApplication#exit}. This ensures
     * the Kubernetes startup probe has time to fire at least once after the embedded web server
     * opens port 8080, so the pod is not caught in CrashLoopBackOff when a dependency (e.g. Pulsar)
     * is unreachable at startup.
     *
     * <p>The value must exceed (MQTT connection timeout) + (web-server startup) +
     * (startupProbe.periodSeconds). With a 10 s MQTT timeout, ~2 s web-server startup, and 5 s
     * probe period, 30 s gives comfortable headroom.
     */
    static final long STARTUP_GRACE_PERIOD_MS = 30_000;

    private final ApplicationContext applicationContext;
    private final AtomicBoolean exitTriggered = new AtomicBoolean(false);
    private final long startTimeMs;

    public FailFastShutdown(ApplicationContext applicationContext) {
        this(applicationContext, System.currentTimeMillis());
    }

    FailFastShutdown(ApplicationContext applicationContext, long startTimeMs) {
        this.applicationContext = applicationContext;
        this.startTimeMs = startTimeMs;
    }

    public void exitWithFailure(Throwable cause) {
        if (!exitTriggered.compareAndSet(false, true)) {
            return;
        }

        log.error("Fail-fast shutdown triggered", cause);

        Thread shutdownThread = new Thread(() -> {
            long minExitAt = startTimeMs + STARTUP_GRACE_PERIOD_MS;
            long waitMs = minExitAt - System.currentTimeMillis();
            if (waitMs > 0) {
                log.info("Delaying shutdown by {}ms so the startup probe can pass", waitMs);
                try {
                    Thread.sleep(waitMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            doExit();
        });
        shutdownThread.setName("FailFastShutdownThread");
        shutdownThread.setDaemon(false);
        shutdownThread.start();
    }

    void doExit() {
        int exitCode = SpringApplication.exit(applicationContext, () -> 1);
        System.exit(exitCode);
    }
}
