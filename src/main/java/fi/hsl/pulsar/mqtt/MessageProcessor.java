package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.pulsar.mqtt.utils.BusyWait;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessor implements IMqttMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private static final long NS_IN_SECOND = 1_000_000_000;

    final Producer<byte[]> producer;
    final PulsarApplication pulsarApp;

    private final AtomicInteger inFlightCounter = new AtomicInteger(0);
    private int msgCounter = 0;
    private long lastMsgTimestamp = -1;

    private final int UNHEALTHY_MSG_SEND_INTERVAL_SECS;
    private final int IN_FLIGHT_ALERT_THRESHOLD;
    private final int MSG_MONITORING_INTERVAL;

    private final long delayBetweenMessagesNs;

    private final BlockingQueue<QueuedMessage> messageQueue = new LinkedBlockingQueue<>();

    private final BiFunction<String, byte[], byte[]> mapper;
    private final Map<String, String> properties;

    private final Thread messageProcessorThread;

    public MessageProcessor(Config config, PulsarApplication pulsarApp) {
        this.pulsarApp = pulsarApp;
        this.producer = pulsarApp.getContext().getSingleProducer();

        UNHEALTHY_MSG_SEND_INTERVAL_SECS = config.getInt("application.unhealthyMsgSendIntervalSecs");
        IN_FLIGHT_ALERT_THRESHOLD = config.getInt("application.inFlightAlertThreshold");
        MSG_MONITORING_INTERVAL = config.getInt("application.msgMonitoringInterval");

        delayBetweenMessagesNs = NS_IN_SECOND / config.getLong("application.maxMessagesPerSecond");

        log.info("Using in-flight alert threshold of {} with monitoring interval of {} messages",
                IN_FLIGHT_ALERT_THRESHOLD, MSG_MONITORING_INTERVAL);
        log.info("Using unhealthy message send interval threshold of {} s for health check (-1 = not in use)",
                UNHEALTHY_MSG_SEND_INTERVAL_SECS);

        IMapperFactory factory = new RawMessageFactory();
        mapper = factory.createMapper();
        properties = factory.properties();

        messageProcessorThread = new Thread(() -> {
            while (!Thread.interrupted()) {
                try {
                    sendMessageFromQueue();

                    if (delayBetweenMessagesNs > 0) {
                        BusyWait.delay(delayBetweenMessagesNs);
                    }
                } catch (InterruptedException e) {
                    log.info("{} was interrupted, stopping message processing", Thread.currentThread().getName());
                }
            }
        });
        messageProcessorThread.setName("MessageProcessorThread");
    }

    public void processMessages() throws InterruptedException {
        messageProcessorThread.start();
        messageProcessorThread.join();
    }

    public void sendMessageFromQueue() throws InterruptedException {
        QueuedMessage message = messageQueue.take();

        message.message.sendAsync()
                // Use timeout to more quickly detect if Pulsar connection is down
                .orTimeout(20, TimeUnit.SECONDS).whenComplete((MessageId id, Throwable t) -> {
                    if (t != null) {
                        log.error("Failed to send Pulsar message", t);

                        message.messageSentFuture.completeExceptionally(t);
                        // Let's close everything and restart
                        close();
                    } else {
                        this.lastMsgTimestamp = System.nanoTime();
                        inFlightCounter.decrementAndGet();

                        message.messageSentFuture.complete(null);
                    }
                });

        final int inFlight = inFlightCounter.incrementAndGet();
        if (++msgCounter % MSG_MONITORING_INTERVAL == 0) {
            if (inFlight < 0 || inFlight > IN_FLIGHT_ALERT_THRESHOLD) {
                log.error("Pulsar insert cannot keep up with the MQTT feed! In flight: {}", inFlight);
            } else {
                log.info("Currently messages in flight: {}", inFlight);
            }
        }
    }

    @Override
    public CompletableFuture<Void> handleMessage(String topic, MqttMessage message) {
        // CompletableFuture which will be completed when the Pulsar message has been sent
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        // This method is invoked by the MQTT client (via our connector), so all events arrive in the same thread
        // https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttCallback.html

        // If manual ack is enabled, MQTT messages will be acknowledged to the broker only after the CompletableFuture is completed (i.e. when message has been sent to Pulsar)
        // This ensures that no data is lost if this application crashes

        // If message rate is too high, manual ack can cause performance problems
        // Disabling manual ack can cause data loss if this application crashes

        final long now = System.currentTimeMillis();

        byte[] payload = message.getPayload();
        if (mapper != null) {
            payload = mapper.apply(topic, payload);
        }

        if (payload != null) {
            TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage().eventTime(now).value(payload);

            Map<String, String> properties = new HashMap<>();

            if (this.properties != null) {
                properties.putAll(this.properties);
            }
            properties.put(TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS, String.valueOf(now));

            msgBuilder.properties(properties);

            messageQueue.offer(new QueuedMessage(msgBuilder, completableFuture));
        } else {
            log.warn("Cannot forward Message to Pulsar because (mapped) content is null, payload before mapping: {}",
                    new String(message.getPayload(), StandardCharsets.UTF_8));

            // Ack MQTT message so that we don't receive it again
            completableFuture.complete(null);
        }

        return completableFuture;
    }

    public void close() {
        log.info("Closing MessageProcessor resources");

        messageProcessorThread.interrupt();
        log.info("{} interrupted", messageProcessorThread.getName());

        pulsarApp.close();
        log.info("Pulsar connection closed");
    }

    public boolean isLastMsgSendIntervalHealthy() {
        if (this.lastMsgTimestamp == -1) {
            return true;
        }
        if (UNHEALTHY_MSG_SEND_INTERVAL_SECS == -1) {
            return true;
        }
        final Duration interval = Duration.ofNanos(System.nanoTime() - this.lastMsgTimestamp);
        if (interval.getSeconds() >= UNHEALTHY_MSG_SEND_INTERVAL_SECS) {
            log.error(
                    "Exceeded UNHEALTHY_MSG_SEND_INTERVAL_SECS threshold: {} s with interval of {} s, considering MQTT subscription unhealthy",
                    UNHEALTHY_MSG_SEND_INTERVAL_SECS, interval.getSeconds());
            return false;
        }
        return true;
    }

    private static class QueuedMessage {
        public final TypedMessageBuilder<byte[]> message;
        public final CompletableFuture<Void> messageSentFuture;

        private QueuedMessage(TypedMessageBuilder<byte[]> message, CompletableFuture<Void> messageSentFuture) {
            this.message = message;
            this.messageSentFuture = messageSentFuture;
        }
    }
}
