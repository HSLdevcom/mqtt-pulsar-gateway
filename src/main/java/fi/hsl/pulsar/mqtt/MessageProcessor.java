package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.pulsar.PulsarApplication;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.pulsar.mqtt.utils.BusyWait;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class MessageProcessor implements IMqttMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private static final long NS_IN_SECOND = 1_000_000_000;

    final Producer<byte[]> producer;
    final PulsarApplication pulsarApp;
    final MqttConnector connector;

    private boolean shutdownInProgress = false;
    private final AtomicInteger inFlightCounter = new AtomicInteger(0);
    private int msgCounter = 0;
    private long lastMsgTimestamp;

    private final int UNHEALTHY_MSG_SEND_INTERVAL_SECS;
    private final int IN_FLIGHT_ALERT_THRESHOLD;
    private final int MSG_MONITORING_INTERVAL;

    private final long delayBetweenMessagesNs;

    private final BlockingQueue<TypedMessageBuilder<byte[]>> messageQueue = new LinkedBlockingQueue<>();

    private final Thread messageSendThread;

    private final BiFunction<String, byte[], byte[]> mapper;
    private final Map<String, String> properties;

    public MessageProcessor(Config config, PulsarApplication pulsarApp, MqttConnector connector) {
        this.pulsarApp = pulsarApp;
        this.producer = pulsarApp.getContext().getProducer();
        this.connector = connector;
        this.lastMsgTimestamp = System.currentTimeMillis();

        UNHEALTHY_MSG_SEND_INTERVAL_SECS = config.getInt("application.unhealthyMsgSendIntervalSecs");
        IN_FLIGHT_ALERT_THRESHOLD = config.getInt("application.inFlightAlertThreshold");
        MSG_MONITORING_INTERVAL = config.getInt("application.msgMonitoringInterval");

        delayBetweenMessagesNs = NS_IN_SECOND / config.getLong("application.maxMessagesPerSecond");

        log.info("Using in-flight alert threshold of {} with monitoring interval of {} messages", IN_FLIGHT_ALERT_THRESHOLD, MSG_MONITORING_INTERVAL);
        log.info("Using unhealthy message send interval threshold of {} s for health check (-1 = not in use)", UNHEALTHY_MSG_SEND_INTERVAL_SECS);

        IMapperFactory factory = new RawMessageFactory();
        mapper = factory.createMapper();
        properties = factory.properties();

        messageSendThread = new Thread(() -> {
            while (!shutdownInProgress) {
                try {
                    sendMessageFromQueue();
                } catch (InterruptedException e) {
                    if (!shutdownInProgress) {
                        //Thread should only get interrupted when the service is being shutdown
                        log.warn("Thread {} was interrupted unexpectedly", Thread.currentThread().getName(), e);
                    }
                }

                if (delayBetweenMessagesNs > 0) {
                    BusyWait.delay(delayBetweenMessagesNs);
                }
            }
        });
        messageSendThread.setName("MessageSendThread");
        messageSendThread.setDaemon(true);
        messageSendThread.start();
    }

    public void sendMessageFromQueue() throws InterruptedException {
        TypedMessageBuilder<byte[]> msgBuilder = messageQueue.take();

        msgBuilder.sendAsync()
                .whenComplete((MessageId id, Throwable t) -> {
                    if (t != null) {
                        log.error("Failed to send Pulsar message", t);
                        //Let's close everything and restart
                        close(true);
                    } else {
                        this.lastMsgTimestamp = System.currentTimeMillis();
                        inFlightCounter.decrementAndGet();
                    }
                });

        int inFlight = inFlightCounter.incrementAndGet();
        if (++msgCounter % MSG_MONITORING_INTERVAL == 0) {
            if (inFlight < 0 || inFlight > IN_FLIGHT_ALERT_THRESHOLD) {
                log.error("Pulsar insert cannot keep up with the MQTT feed! In flight: {}", inFlight);
            } else {
                log.info("Currently messages in flight: {}", inFlight);
            }
        }
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
            if (mapper != null) {
                payload = mapper.apply(topic, payload);
            }

            if (payload != null) {
                TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage()
                        .eventTime(now)
                        .value(payload);

                Map<String, String> properties = new HashMap<>();

                if (this.properties != null) {
                    properties.putAll(this.properties);
                }
                properties.put(TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS, String.valueOf(now));

                msgBuilder.properties(properties);

                messageQueue.offer(msgBuilder);
            } else {
                log.warn("Cannot forward Message to Pulsar because (mapped) content is null");
            }

        } catch (Exception e) {
            log.error("Error while handling the message", e);
            // Let's close everything and restart.
            // Closing the MQTT connection should enable us to receive the same message again.
            close(true);
        }

    }

    @Override
    public void connectionLost(Throwable cause) {
        log.warn("MQTT connection lost", cause);
        close(false);
    }

    public void close(boolean closeMqtt) {
        if (shutdownInProgress) {
            return;
        }
        shutdownInProgress = true;

        log.info("Closing MessageProcessor resources");
        //Let's first close the MQTT to stop the event stream.
        if (closeMqtt) {
            connector.close();
            log.info("MQTT connection closed");
        }

        pulsarApp.close();
        log.info("Pulsar connection closed");

        log.info("Interrupting {}", messageSendThread.getName());
        messageSendThread.interrupt();
    }

    public boolean isMqttConnected() {
        if (connector != null) {
            boolean mqttConnected = connector.isMqttConnected();
            if (!mqttConnected) {
                log.error("Health check: MQTT is not connected");
            }
            return mqttConnected;
        }
        return false;
    }

    public boolean isLastMsgSendIntervalHealthy() {
        if (this.lastMsgTimestamp == -1) {
            return true;
        }
        if (UNHEALTHY_MSG_SEND_INTERVAL_SECS == -1) {
            return true;
        }
        long intervalMillis = System.currentTimeMillis() - this.lastMsgTimestamp;
        long intervalSecs = Math.round((double) intervalMillis/1000);
        if (intervalSecs >= UNHEALTHY_MSG_SEND_INTERVAL_SECS) {
            log.error("Exceeded UNHEALTHY_MSG_SEND_INTERVAL_SECS threshold: {} s with interval of {} s, considering mqtt subscription unhealthy",
                    UNHEALTHY_MSG_SEND_INTERVAL_SECS, intervalSecs);
            return false;
        }
        return true;
    }
}
