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
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class PulsarPublisher {

    private static final Logger log = LoggerFactory.getLogger(PulsarPublisher.class);

    private static final String KEY_SOURCE_MESSAGE_TIMESTAMP_MS = "source-ts";
    private static final String KEY_PROTOBUF_SCHEMA = "protobuf-schema";
    private static final String KEY_SCHEMA_VERSION = "schema-version";

    private final PulsarClient client;
    private final Producer<byte[]> producer;

    @Autowired
    public PulsarPublisher(PulsarProperties props) throws PulsarClientException {
        this.client = PulsarClient.builder().serviceUrl("pulsar://" + props.host() + ":" + props.port()).build();
        try {
            this.producer = client.newProducer(Schema.BYTES).topic(props.topic())
                    .sendTimeout(props.sendTimeoutSeconds(), TimeUnit.SECONDS)
                    .maxPendingMessages(props.maxPendingMessages()).blockIfQueueFull(true).create();
        } catch (PulsarClientException e) {
            try {
                client.close();
            } catch (PulsarClientException closeEx) {
                e.addSuppressed(closeEx);
            }
            throw e;
        }
        log.info("Pulsar producer created, topic={}", props.topic());
    }

    PulsarPublisher(PulsarClient client, Producer<byte[]> producer) {
        this.client = client;
        this.producer = producer;
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
     */
    public CompletableFuture<MessageId> publish(byte[] payload, long eventTimeMs, String protobufSchema,
            int schemaVersion) {
        Map<String, String> properties = Map.of(KEY_SOURCE_MESSAGE_TIMESTAMP_MS, String.valueOf(eventTimeMs),
                KEY_PROTOBUF_SCHEMA, protobufSchema, KEY_SCHEMA_VERSION, Integer.toString(schemaVersion));

        try {
            return producer.newMessage().eventTime(eventTimeMs).value(payload).properties(properties).sendAsync();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @PreDestroy
    public void close() {
        try {
            producer.flush();
        } catch (Exception e) {
            log.warn("Failed to flush Pulsar producer before close", e);
        }
        try {
            producer.close();
        } catch (Exception e) {
            log.warn("Failed to close Pulsar producer", e);
        }
        try {
            client.close();
        } catch (Exception e) {
            log.warn("Failed to close Pulsar client", e);
        }
    }
}
