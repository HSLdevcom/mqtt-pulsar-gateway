package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletionException;

@Component
public class MqttInboundHandler {

    private final IMqttMessageHandler mqttMessageHandler;
    private final boolean mqttManualAck;

    public MqttInboundHandler(IMqttMessageHandler mqttMessageHandler, Config config) {
        this.mqttMessageHandler = mqttMessageHandler;
        this.mqttManualAck = config.getBoolean("mqtt-broker.manualAck");
    }

    @ServiceActivator(inputChannel = "mqttInputChannel", adviceChain = "mqttRetryAdvice")
    public void handleMessage(Message<?> message) {

        String topic = message.getHeaders().get(MqttHeaders.RECEIVED_TOPIC, String.class);

        byte[] payloadBytes = toBytes(message.getPayload());

        Integer qos = message.getHeaders().get(MqttHeaders.RECEIVED_QOS, Integer.class);
        int qosValue = (qos != null) ? qos : 0;

        MqttMessage pahoMsg = new MqttMessage(payloadBytes);
        pahoMsg.setQos(qosValue);

        try {
            mqttMessageHandler.handleMessage(topic, pahoMsg).join();
        } catch (CompletionException e) {
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause);
        }

        if (mqttManualAck) {
            AcknowledgmentCallback ack =
                    StaticMessageHeaderAccessor.getAcknowledgmentCallback(message);

            Objects.requireNonNull(ack, "Missing acknowledgmentCallback header (manualAck=true)");
            ack.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
        }
    }

    private static byte[] toBytes(Object payload) {
        if (payload instanceof byte[] b) {
            return b;
        }
        if (payload instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException("Unsupported MQTT payload type: " + payload.getClass());
    }
}
