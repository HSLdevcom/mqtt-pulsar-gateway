package fi.hsl.pulsar.mqtt;

import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Paho has multiple interfaces but none of them seem to support our use case of receiving events for both
 * message-arrived and client-disconnected event. So we'll wrap our own interface.
 */
public interface IMqttMessageHandler {
    void handleMessage(String topic, MqttMessage message) throws Exception;
    void connectionLost(Throwable cause);
}
