package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.Objects;

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

        byte[] payload = (byte[]) message.getPayload();

        Integer qos = message.getHeaders().get(MqttHeaders.RECEIVED_QOS, Integer.class);
        MqttMessage paho = new MqttMessage(payload);
        paho.setQos(qos != null ? qos : 0);

        mqttMessageHandler.handleMessage(topic, paho).join();

        if (mqttManualAck) {
            AcknowledgmentCallback ack = StaticMessageHeaderAccessor.getAcknowledgmentCallback(message);
            Objects.requireNonNull(ack, "Missing acknowledgmentCallback header");
            ack.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
        }
    }
}
