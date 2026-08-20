package fi.hsl.pulsar.mqtt.service;

import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.SimpleAcknowledgment;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.CompletionException;

@Component
public class MqttToPulsarMessageHandler implements MessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MqttToPulsarMessageHandler.class);

    private static final String PROTOBUF_SCHEMA_MQTT_RAW_MESSAGE = "mqtt-raw";

    private final RawMessageMapper rawMessageMapper;
    private final PulsarPublisher pulsarPublisher;
    private final FailFastShutdown failFastShutdown;

    public MqttToPulsarMessageHandler(RawMessageMapper rawMessageMapper, PulsarPublisher pulsarPublisher,
            FailFastShutdown failFastShutdown) {
        this.rawMessageMapper = rawMessageMapper;
        this.pulsarPublisher = pulsarPublisher;
        this.failFastShutdown = failFastShutdown;
    }

    @Override
    public void handleMessage(Message<?> message) throws MessagingException {
        final String topic = Objects.toString(message.getHeaders().get(MqttHeaders.RECEIVED_TOPIC), null);
        if (topic == null || topic.isBlank()) {
            throw new MessagingException(message, "Missing MQTT topic header");
        }

        Object ack = message.getHeaders().get(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK);
        if (!(ack instanceof SimpleAcknowledgment acknowledgment)) {
            throw new MessagingException(message, "Missing MQTT acknowledgment header (manual acks must be enabled)");
        }

        if (!(message.getPayload() instanceof byte[] mqttPayload)) {
            throw new MessagingException(message, "Expected byte[] payload");
        }

        final long now = System.currentTimeMillis();
        final byte[] pulsarPayload = rawMessageMapper.toRawMessageBytes(topic, mqttPayload);
        final int schemaVersion = rawMessageMapper.schemaVersion();

        // Acknowledge MQTT only after Pulsar confirms persistence. Per-producer completion order
        // equals send order, so MQTT acks fire in the same order the messages arrived.
        pulsarPublisher.publish(pulsarPayload, now, PROTOBUF_SCHEMA_MQTT_RAW_MESSAGE, schemaVersion)
                .thenAccept(messageId -> acknowledgment.acknowledge()).exceptionally(throwable -> {
                    Throwable cause = unwrap(throwable);
                    if (cause instanceof PulsarClientException.TimeoutException) {
                        log.error("Pulsar send timed out; failing fast to avoid ingesting buffered MQTT messages",
                                cause);
                    } else {
                        log.error("Pulsar send failed; failing fast", cause);
                    }
                    failFastShutdown.exitWithFailure(cause);
                    return null;
                });
    }

    private static Throwable unwrap(Throwable throwable) {
        if (throwable instanceof CompletionException && throwable.getCause() != null) {
            return throwable.getCause();
        }
        return throwable;
    }
}
