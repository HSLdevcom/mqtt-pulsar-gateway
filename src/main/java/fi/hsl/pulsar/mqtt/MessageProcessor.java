package fi.hsl.pulsar.mqtt;

import fi.hsl.common.pulsar.PulsarApplication;

import org.apache.pulsar.client.api.Producer;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final Producer<byte[]> producer;
    final MqttConnector connector;
    final PulsarApplication pulsarApp;

    private MessageProcessor(MqttConnector connector, PulsarApplication pulsarApp) {
        this.connector = connector;
        this.pulsarApp = pulsarApp;
        this.producer = pulsarApp.getContext().getProducer();
    }

    public static MessageProcessor newInstance(MqttConnector connector, PulsarApplication pulsarApp) throws Exception {
        MessageProcessor processor = new MessageProcessor(connector, pulsarApp);
        log.info("MessageProcessor subscribing to receive MQTT events");
        connector.subscribe(processor);
        return processor;
    }

    static int counter = 0;
    @Override
    public void handleMessage(String topic, MqttMessage message) throws Exception {
        // Let's feed the message directly to Pulsar.
        // We could have a queue here to separate these two functionality into separate threads,
        // but error handling would get more complicated
        counter++;
        if (counter % 1000 == 0) {
            log.info("Got {} messages", counter);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.info("Mqtt connection lost");
        //Let mqtt connection handler clean up itself
        close(false);
    }

    public void close(boolean closeMqtt) {
        log.warn("Closing MessageProcessor resources");
        pulsarApp.close();
        log.info("Pulsar connection closed");
        if (closeMqtt) {
            connector.close();
            log.info("MQTT connection closed");
        }
    }
}
