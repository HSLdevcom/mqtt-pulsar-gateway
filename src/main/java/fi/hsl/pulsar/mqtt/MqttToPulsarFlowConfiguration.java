package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.advice.RequestHandlerRetryAdvice;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import static java.lang.System.exit;

@Configuration
public class MqttToPulsarFlowConfiguration {

    @Bean
    public BiFunction<String, byte[], byte[]> mapper() {
        IMapperFactory factory = new RawMessageFactory();
        return factory.createMapper();
    }

    @Bean
    public Map<String, String> baseProperties() {
        IMapperFactory factory = new RawMessageFactory();
        Map<String, String> props = factory.properties();
        return (props != null) ? props : Map.of();
    }

    @Bean
    public IntegrationFlow mqttToPulsarFlow(Config config,
                                            Producer<byte[]> producer,
                                            BiFunction<String, byte[], byte[]> mapper,
                                            Map<String, String> baseProperties,
                                            RequestHandlerRetryAdvice mqttRetryAdvice) {
        boolean manualAck = config.getBoolean("mqtt-broker.manualAck");

        return IntegrationFlow.from("mqttInputChannel")
                .transform(Message.class, mqttMessage -> pulsarMessage(mqttMessage, mapper, baseProperties))
                .handle((message) -> {
                    PulsarMessage pulsarMessage = (PulsarMessage) message.getPayload();

                    Map<String, Object> headers = message.getHeaders();

                    producer.newMessage()
                            .eventTime(pulsarMessage.eventTimeMs())
                            .properties(pulsarMessage.properties())
                            .value(pulsarMessage.payload())
                            .sendAsync()
                            .whenCompleteAsync((messageId, ex) -> {
                                if (ex == null) {
                                    AckSupport.ack(message.getHeaders());
                                } else {
                                    exit(1);
                                }
                            })
                            .orTimeout(20, TimeUnit.SECONDS)
                            .join();

                    if(manualAck) {
                        AckSupport.ack(headers);
                    }
                }, spec -> spec.advice(mqttRetryAdvice))
                .get();
    }

    private static PulsarMessage pulsarMessage(Message<?> mqttMessage, BiFunction<String, byte[], byte[]> mapper, Map<String, String> baseProperties) {
        String topic = mqttMessage.getHeaders().get(MqttHeaders.RECEIVED_TOPIC, String.class);
        byte[] payload = (byte[]) mqttMessage.getPayload();

        long nowMs = System.currentTimeMillis();

        byte[] mapped = mapper != null ? mapper.apply(topic, payload) : payload;

        if (mapped == null) {
            return null;
        }

        Map<String, String> props = new HashMap<>();
        if (baseProperties != null) {
            props.putAll(baseProperties);
        }
        props.put(TransitdataProperties.KEY_SOURCE_MESSAGE_TIMESTAMP_MS, String.valueOf(nowMs));
        if (topic != null) {
            props.put("mqtt_topic", topic);
        }

        return new PulsarMessage(mapped, nowMs, props);
    }

    static final class AckSupport {
        private AckSupport() {}

        static void ack(Map<String, Object> headers) {
            Object ackObj = headers.get("acknowledgmentCallback");
            if (ackObj == null) {
                throw new IllegalStateException("manualAck=true but no 'acknowledgmentCallback' header present");
            }
            try {
                ackObj.getClass().getMethod("acknowledge").invoke(ackObj);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to acknowledge MQTT message; type=" + ackObj.getClass().getName(), e);
            }
        }
    }

}
