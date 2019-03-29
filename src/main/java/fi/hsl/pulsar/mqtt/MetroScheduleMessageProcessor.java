package fi.hsl.pulsar.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.proto.MetroMessages;
import fi.hsl.pulsar.mqtt.models.MetroSchedule;
import fi.hsl.pulsar.mqtt.models.MetroForecast;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class MetroScheduleMessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MetroScheduleMessageProcessor.class);

    final Producer<byte[]> producer;
    final PulsarApplication pulsarApp;
    final MqttConnector connector;

    private boolean shutdownInProgress = false;
    private final AtomicInteger inFlightCounter = new AtomicInteger(0);
    private int msgCounter = 0;

    private final int IN_FLIGHT_ALERT_THRESHOLD;
    private final int MSG_MONITORING_INTERVAL;

    private final BiFunction<String, byte[], byte[]> mapper;
    private final Map<String, String> properties;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public MetroScheduleMessageProcessor(Config config, PulsarApplication pulsarApp, MqttConnector connector) {
        this.pulsarApp = pulsarApp;
        this.producer = pulsarApp.getContext().getProducer();
        this.connector = connector;

        IN_FLIGHT_ALERT_THRESHOLD = config.getInt("application.inFlightAlertThreshold");
        MSG_MONITORING_INTERVAL = config.getInt("application.msgMonitoringInterval");
        log.info("Using in-flight alert threshold of {} with monitoring interval of {} messages", IN_FLIGHT_ALERT_THRESHOLD, MSG_MONITORING_INTERVAL);

        IMapperFactory factory = new RawMessageFactory();
        mapper = factory.createMapper();
        properties = factory.properties();
    }

    @Override
    public void handleMessage(String topic, MqttMessage message) throws Exception {
        try {
            // This method is invoked synchronously by the MQTT client (via our connector), so all events arrive in the same thread
            // https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttCallback.html

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

            byte[] payload = message.getPayload();
            String foo = new String(payload);
            if (mapper != null) {
                payload = mapper.apply(topic, payload);
            }

            if (payload != null) {
                String bar = new String(payload);
                //MetroMessages.Schedule.Builder builder = MetroMessages.Schedule.newBuilder();
                //MetroSchedule schedule = objectMapper.readValue(foo, MetroSchedule.class);
                MetroForecast forecast = objectMapper.readValue(foo, MetroForecast.class);

                TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage()
                        .eventTime(now)
                        .value(payload);

                if (properties != null) {
                    msgBuilder.properties(properties);
                }

                msgBuilder.sendAsync()
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
                if (++msgCounter % MSG_MONITORING_INTERVAL == 0) {
                    if (inFlight < 0 || inFlight > IN_FLIGHT_ALERT_THRESHOLD) {
                        log.error("Pulsar insert cannot keep up with the MQTT feed! In flight: {}", inFlight);
                    }
                    else {
                        log.info("Currently messages in flight: {}", inFlight);
                    }
                }
            }
            else {
                log.warn("Cannot forward Message to Pulsar because (mapped) content is null");
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
