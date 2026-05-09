package fi.hsl.pulsar.mqtt.service;

import fi.hsl.pulsar.mqtt.config.PulsarProperties;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class PulsarPublisher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(PulsarPublisher.class);

    private static final String KEY_SOURCE_MESSAGE_TIMESTAMP_MS = "source-ts";
    private static final String KEY_PROTOBUF_SCHEMA = "protobuf-schema";
    private static final String KEY_SCHEMA_VERSION = "schema-version";

    /** How long {@link #connect()} retries before giving up. Overridden to 0 in unit tests. */
    static final long DEFAULT_CONNECT_RETRY_TIMEOUT_MS = 90_000;

    private static final long CONNECT_RETRY_DELAY_MS = 5_000;

    private final PulsarProperties props;
    private final FailFastShutdown failFastShutdown;
    private final long connectRetryTimeoutMs;

    private volatile PulsarClient client;
    private volatile Producer<byte[]> producer;
    private volatile boolean running = false;

    @Autowired
    public PulsarPublisher(PulsarProperties props, FailFastShutdown failFastShutdown) {
        this(props, failFastShutdown, DEFAULT_CONNECT_RETRY_TIMEOUT_MS);
    }

    PulsarPublisher(PulsarProperties props, FailFastShutdown failFastShutdown, long connectRetryTimeoutMs) {
        this.props = props;
        this.failFastShutdown = failFastShutdown;
        this.connectRetryTimeoutMs = connectRetryTimeoutMs;
    }

    PulsarPublisher(PulsarClient client, Producer<byte[]> producer) {
        this.props = null;
        this.failFastShutdown = null;
        this.connectRetryTimeoutMs = 0;
        this.client = client;
        this.producer = producer;
        this.running = true;
    }

    /**
     * Spawns a virtual thread that calls {@link #connect()} so the web server (and its health
     * endpoints) can start on port 8080 while the Pulsar connection is being established. This
     * prevents the startup probe from seeing "connection refused" when Pulsar is slow or
     * temporarily unreachable.
     *
     * <p>{@link #connect()} retries for up to {@link #DEFAULT_CONNECT_RETRY_TIMEOUT_MS} ms before
     * triggering fail-fast shutdown, giving the Pulsar broker time to become available and ensuring
     * the web server has started before the pod exits.
     *
     * <p>Phase {@code Integer.MAX_VALUE / 2 - 1} ensures this runs before the Spring Integration
     * MQTT inbound adapter (phase {@code Integer.MAX_VALUE / 2}), so the producer is ready by the
     * time MQTT messages start flowing.
     */
    @Override
    public void start() {
        Thread.ofVirtual().name("pulsar-connect").start(this::connect);
    }

    void connect() {
        long deadline = System.currentTimeMillis() + connectRetryTimeoutMs;
        PulsarClientException lastException = null;
        do {
            PulsarClient c = null;
            try {
                c = PulsarClient.builder().serviceUrl("pulsar://" + props.host() + ":" + props.port())
                        .connectionTimeout(10, TimeUnit.SECONDS).operationTimeout(30, TimeUnit.SECONDS).build();
                Producer<byte[]> p = c.newProducer(Schema.BYTES).topic(props.topic())
                        .sendTimeout(props.sendTimeoutSeconds(), TimeUnit.SECONDS)
                        .maxPendingMessages(props.maxPendingMessages()).blockIfQueueFull(true).create();
                this.client = c;
                this.producer = p;
                this.running = true;
                log.info("Pulsar producer created, topic={}", props.topic());
                return;
            } catch (PulsarClientException e) {
                lastException = e;
                if (c != null) {
                    try {
                        c.close();
                    } catch (PulsarClientException closeEx) {
                        e.addSuppressed(closeEx);
                    }
                }
                if (System.currentTimeMillis() < deadline) {
                    log.warn("Failed to connect to Pulsar, retrying in {}ms", CONNECT_RETRY_DELAY_MS, e);
                    try {
                        Thread.sleep(CONNECT_RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        failFastShutdown.exitWithFailure(ie);
                        return;
                    }
                }
            }
        } while (System.currentTimeMillis() < deadline);
        log.error("Failed to connect to Pulsar after {}ms, triggering fail-fast shutdown", connectRetryTimeoutMs,
                lastException);
        failFastShutdown.exitWithFailure(lastException);
    }

    @Override
    public void stop() {
        running = false;
        Producer<byte[]> p = this.producer;
        PulsarClient c = this.client;
        if (p != null) {
            try {
                p.flush();
            } catch (Exception e) {
                log.warn("Failed to flush Pulsar producer before close", e);
            }
            try {
                p.close();
            } catch (Exception e) {
                log.warn("Failed to close Pulsar producer", e);
            }
        }
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                log.warn("Failed to close Pulsar client", e);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE / 2 - 1;
    }

    /**
     * Submit a message to the Pulsar producer asynchronously.
     *
     * <p>Completion order equals send order per producer, which the caller relies on to preserve
     * per-MQTT-topic ordering when acknowledging MQTT messages in the completion callback.
     *
     * <p>If {@code sendAsync} itself throws synchronously (for example because the producer is
     * already closed or the calling thread was interrupted while waiting for queue space), the
     * returned future completes exceptionally so that callers handle all failures uniformly.
     *
     * <p>If the producer is not yet initialized (i.e. {@link #start()} has not completed), the
     * returned future completes exceptionally with {@link IllegalStateException}, which the caller
     * treats as a fatal error and triggers fail-fast shutdown.
     */
    public CompletableFuture<MessageId> publish(byte[] payload, long eventTimeMs, String protobufSchema,
            int schemaVersion) {
        Producer<byte[]> p = this.producer;
        if (p == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Pulsar producer is not yet initialized"));
        }
        Map<String, String> properties = Map.of(KEY_SOURCE_MESSAGE_TIMESTAMP_MS, String.valueOf(eventTimeMs),
                KEY_PROTOBUF_SCHEMA, protobufSchema, KEY_SCHEMA_VERSION, Integer.toString(schemaVersion));
        try {
            return p.newMessage().eventTime(eventTimeMs).value(payload).properties(properties).sendAsync();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
