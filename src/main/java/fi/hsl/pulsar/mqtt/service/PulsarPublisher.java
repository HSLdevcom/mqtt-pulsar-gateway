package fi.hsl.pulsar.mqtt.service;

import fi.hsl.pulsar.mqtt.config.PulsarProperties;
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
        if (props.getServiceUrl() == null || props.getServiceUrl().isBlank()) {
            throw new IllegalArgumentException("pulsar.serviceUrl is required");
        }
        if (props.getTopic() == null || props.getTopic().isBlank()) {
            throw new IllegalArgumentException("pulsar.topic is required");
        }

        this.client = PulsarClient.builder().serviceUrl(props.getServiceUrl()).build();
        this.producer = client.newProducer(Schema.BYTES).topic(props.getTopic())
                .sendTimeout(props.getSendTimeoutSeconds(), TimeUnit.SECONDS)
                .maxPendingMessages(props.getMaxPendingMessages()).blockIfQueueFull(true).create();
        log.info("Pulsar producer created, topic={}", props.getTopic());
    }

    PulsarPublisher(PulsarClient client, Producer<byte[]> producer) {
        this.client = client;
        this.producer = producer;
    }

    public void publish(byte[] payload, long eventTimeMs, String protobufSchema, int schemaVersion)
            throws PulsarClientException {
        Map<String, String> properties = Map.of(KEY_SOURCE_MESSAGE_TIMESTAMP_MS, String.valueOf(eventTimeMs),
                KEY_PROTOBUF_SCHEMA, protobufSchema, KEY_SCHEMA_VERSION, Integer.toString(schemaVersion));

        producer.newMessage().eventTime(eventTimeMs).value(payload).properties(properties).send();
    }

    @PreDestroy
    public void close() {
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
