package fi.hsl.pulsar.mqtt.service;

import fi.hsl.pulsar.mqtt.config.PulsarProperties;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private final PulsarProperties props;
    private final FailFastShutdown failFastShutdown;

    private volatile PulsarClient client;
    private volatile Producer<byte[]> producer;
    private volatile boolean running = false;

    public PulsarPublisher(PulsarProperties props, FailFastShutdown failFastShutdown) {
        this.props = props;
        this.failFastShutdown = failFastShutdown;
    }

    PulsarPublisher(PulsarClient client, Producer<byte[]> producer) {
        this.props = null;
        this.failFastShutdown = null;
        this.client = client;
        this.producer = producer;
        this.running = true;
    }

    /**
     * Connects to Pulsar in a background thread so the web server (and its health endpoints) can
     * start on port 8080 before the connection attempt completes. This prevents the startup probe
     * from seeing "connection refused" when Pulsar is slow or temporarily unreachable.
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
        try {
            PulsarClient c = PulsarClient.builder()
                    .serviceUrl("pulsar://" + props.host() + ":" + props.port())
                    .connectionTimeout(10, TimeUnit.SECONDS)
                    .operationTimeout(30, TimeUnit.SECONDS)
                    .build();
            Producer<byte[]> p;
            try {
                p = c.newProducer(Schema.BYTES).topic(props.topic())
                        .sendTimeout(props.sendTimeoutSeconds(), TimeUnit.SECONDS)
                        .maxPendingMessages(props.maxPendingMessages()).blockIfQueueFull(true).create();
            } catch (PulsarClientException e) {
                try {
                    c.close();
                } catch (PulsarClientException closeEx) {
                    e.addSuppressed(closeEx);
                }
                throw e;
            }
            this.client = c;
            this.producer = p;
            this.running = true;
            log.info("Pulsar producer created, topic={}", props.topic());
        } catch (PulsarClientException e) {
            log.error("Failed to connect to Pulsar, triggering fail-fast shutdown", e);
            failFastShutdown.exitWithFailure(e);
        }
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
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Pulsar producer is not yet initialized"));
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
