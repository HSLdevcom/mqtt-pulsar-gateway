package fi.hsl.pulsar.mqtt;

import fi.hsl.common.pulsar.PulsarApplication;

import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final Producer<byte[]> producer;
    final PulsarApplication pulsarApp;

    public MessageProcessor(PulsarApplication pulsarApp) {
        this.pulsarApp = pulsarApp;
        this.producer = pulsarApp.getContext().getProducer();
    }

    static int counter = 0;
    @Override
    public void handleMessage(String topic, MqttMessage message) throws Exception {
        // Let's feed the message directly to Pulsar.
        // We could have a queue here to separate these two functionality into separate threads,
        // but error handling would get more complicated
        try {
            //If we want to deliver messages once and only once, in insertion order, we might have to
            //do some magic here for performance reasons...

            //TODO insert server timestamp to go with the payload.
            //TransitdataProperties.
            producer.send(message.getPayload());
            counter++;
            if (counter % 1000 == 0) {
                log.info("Got {} messages", counter);
            }
        }
        catch (PulsarClientException e) {
            log.error("Failed to send Pulsar message", e);
            //might be that we have to close also the mqtt connection. how to make sure this same event will be re-delivered and not acked?
            close();
            throw e;
        }


    }

    @Override
    public void connectionLost(Throwable cause) {
        log.info("Mqtt connection lost");
        close();
    }

    public void close() {
        log.warn("Closing MessageProcessor resources");
        pulsarApp.close();
        log.info("Pulsar connection closed");
    }
}
