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

    private final ApplicationContext applicationContext;
    private final AtomicBoolean exitTriggered = new AtomicBoolean(false);

    public FailFastShutdown(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public void exitWithFailure(Throwable cause) {
        if (!exitTriggered.compareAndSet(false, true)) {
            return;
        }

        log.error("Fail-fast shutdown triggered", cause);

        Thread shutdownThread = new Thread(() -> {
            int exitCode = SpringApplication.exit(applicationContext, () -> 1);
            System.exit(exitCode);
        });
        shutdownThread.setName("FailFastShutdownThread");
        shutdownThread.setDaemon(false);
        shutdownThread.start();
    }
}
