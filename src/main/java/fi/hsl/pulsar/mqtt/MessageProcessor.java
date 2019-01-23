package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.pulsar.PulsarApplication;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class MessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final Producer<byte[]> producer;
    final PulsarApplication pulsarApp;
    final MqttConnector connector;

    private boolean shutdownInProgress = false;
    private final AtomicInteger inFlightCounter = new AtomicInteger(0);
    private final int inFlightAlertThreshold;

    public MessageProcessor(Config config, PulsarApplication pulsarApp, MqttConnector connector) {
        this.pulsarApp = pulsarApp;
        this.producer = pulsarApp.getContext().getProducer();
        this.connector = connector;

        inFlightAlertThreshold = config.getInt("application.inFlightAlertThreshold");
        log.info("Using inFlightAlertThreshold: {}", inFlightAlertThreshold);
    }

    @Override
    public void handleMessage(String topic, MqttMessage message) throws Exception {
        try {
            // Optimally we would like to send the event to Pulsar synchronously and validate that it was a success,
            // and only after that acknowledge the message to mqtt by returning gracefully from this function.
            // This works if the rate of incoming messages is low enough to complete the Pulsar transaction.
            // This would allow us to deliver messages once and only once, in insertion order

            // If we want to improve our performance and lose guarantees of once-and-only-once,
            // we can optimize the pipeline by sending messages asynchronously to Pulsar.
            // This means that data will be lost on insertion errors.
            // Using a single producer however should guarantee insertion-order guarantee between two consecutive messages.

            // Current implementation uses the latter approach

            if (!producer.isConnected()) {
                log.error("Pulsar Producer is no longer connected. Exiting application");
                close(true);
            }

            long now = System.currentTimeMillis();
            producer.newMessage()
                    .eventTime(now)
                    .value(message.getPayload())
                    .sendAsync()
                    .whenComplete((MessageId id, Throwable t) -> {
                        if (t != null) {
                            log.error("Failed to send Pulsar message", t);
                            //Let's close everything and restart
                            close(true);
                        }
                        else {
                            inFlightCounter.decrementAndGet();
                        }
                    });

            int inFlight = inFlightCounter.incrementAndGet();
            if (inFlight < 0 || inFlight > inFlightAlertThreshold) {
                log.error("Pulsar insert cannot keep up with the MQTT feed! inflight: {}", inFlight);
            }
        }
        catch (Exception e) {
            log.error("Error while handling the message", e);
            // Let's close everything and restart.
            // Closing the MQTT connection should enable us to receive the same message again.
            close(true);
        }

    }

    @Override
    public void connectionLost(Throwable cause) {
        log.info("Mqtt connection lost");
        close(false);
    }

    public void close(boolean closeMqtt) {
        if (shutdownInProgress) {
            return;
        }
        shutdownInProgress = true;

        log.warn("Closing MessageProcessor resources");
        //Let's first close the MQTT to stop the event stream.
        if (closeMqtt) {
            connector.close();
            log.info("MQTT connection closed");
        }

        pulsarApp.close();
        log.info("Pulsar connection closed");

    }
}
