package fi.hsl.pulsar.mqtt;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class MqttInboundHandler {

    private final IMqttMessageHandler mqttMessageHandler;

    public MqttInboundHandler(IMqttMessageHandler mqttMessageHandler) {
        this.mqttMessageHandler = mqttMessageHandler;
    }

    @ServiceActivator(inputChannel = "mqttInputChannel", adviceChain = "mqttRetryAdvice")
    public void handleMessage(Message<?> message) {
        String topic = message.getHeaders().get(MqttHeaders.RECEIVED_TOPIC, String.class);

        MqttMessage mqttMessage = (MqttMessage) message.getPayload();

        mqttMessageHandler.handleMessage(topic, mqttMessage).whenComplete((res, err) -> {
            if (err != null) {
                throw new RuntimeException(err);
            }

            Objects.requireNonNull(StaticMessageHeaderAccessor.getAcknowledgment(message)).acknowledge();
        });

    }
}
