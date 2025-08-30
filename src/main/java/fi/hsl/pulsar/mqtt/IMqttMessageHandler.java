package fi.hsl.pulsar.mqtt;

import java.util.concurrent.CompletableFuture;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public interface IMqttMessageHandler {
    /**
     * Handle MQTT message. Method must return a completable future that completes when the message
     * is processed successfully or throws an exception when processing fails
     *
     * @param topic MQTT topic
     * @param message MQTT message
     * @return Completable future that completes when message is processed
     */
    CompletableFuture<Void> handleMessage(String topic, MqttMessage message);
}
