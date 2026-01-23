package fi.hsl.pulsar.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SpringBootTest(classes = {
    MqttConnectorIT.TestHandlerConfig.class
})
public class MqttConnectorIT {

    @Rule
    public GenericContainer mqttBroker = new GenericContainer(DockerImageName.parse("hivemq/hivemq4"))
            .withExposedPorts(1883);

    static final AtomicInteger counter = new AtomicInteger();
    static final CountDownLatch latch = new CountDownLatch(5);

/*    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        final String brokerUri = "tcp://" + mqttBroker.getHost() + ":" + mqttBroker.getFirstMappedPort();

        // These must match your @ConfigurationProperties keys (adapt as needed)
        registry.add("mqtt-broker.topic", () -> "#");
        registry.add("mqtt-broker.qos", () -> 1);
        registry.add("mqtt-broker.host", () -> brokerUri);
        registry.add("mqtt-broker.manualAck", () -> true);
        registry.add("mqtt-broker.maxInflight", () -> 10000);
        registry.add("mqtt-broker.cleanSession", () -> true);
        registry.add("mqtt-broker.keepAliveInterval", () -> 30);
        registry.add("mqtt-broker.clientId", () -> "test_client");
        registry.add("mqtt-broker.addRandomnessToClientId", () -> true);
    }*/

    @Test
    @Timeout(20)
    public void testMqttConnector() throws Exception {
        final String brokerUri = "tcp://" + mqttBroker.getHost() + ":" + mqttBroker.getFirstMappedPort();

        Config config = mock(Config.class);
        when(config.getString("mqtt-broker.topic")).thenReturn("#");
        when(config.getInt("mqtt-broker.qos")).thenReturn(1);
        when(config.getString("mqtt-broker.host")).thenReturn(brokerUri);
        when(config.getBoolean("mqtt-broker.manualAck")).thenReturn(true);
        when(config.getInt("mqtt-broker.maxInflight")).thenReturn(10000);
        when(config.getBoolean("mqtt-broker.cleanSession")).thenReturn(true);
        when(config.getInt("mqtt-broker.keepAliveInterval")).thenReturn(30);
        when(config.getString("mqtt-broker.clientId")).thenReturn("test_client");
        when(config.getBoolean("mqtt-broker.addRandomnessToClientId")).thenReturn(true);

        MqttClient publisher = new MqttClient(brokerUri, MqttClient.generateClientId(), new MemoryPersistence());
        publisher.connect();

        for (int i = 0; i < 5; i++) {
            publisher.publish("test", String.valueOf(i).getBytes(StandardCharsets.UTF_8), 1, false);
        }

        publisher.disconnect();
        publisher.close(true);

        boolean all = latch.await(10, TimeUnit.SECONDS);
        assertThat(all).isTrue();
        assertThat(counter.get()).isEqualTo(5);
    }

    @TestConfiguration
    static class TestHandlerConfig {

        @Bean
        public Object testInboundHandler() {
            return new Object() {
                @ServiceActivator(inputChannel = "mqttInputChannel")
                public void handle(Message<?> message) {
                    // Spring Integration MQTT payload is a Paho MqttMessage by default
                    MqttMessage mqttMessage = (MqttMessage) message.getPayload();

                    // Optional: topic is in header
                    String topic = (String) message.getHeaders().get(MqttHeaders.RECEIVED_TOPIC);

                    counter.incrementAndGet();
                    latch.countDown();

                    // If your production design uses manual acks, your real handler should ACK after success.
                    // For this test, you can omit ACK if your adapter is configured with manualAck=false,
                    // or keep manualAck=true and ensure your production handler ACKs.
                }
            };
        }
    }

/*    @Test
    public void testMqttConnector() throws Exception {
        final String brokerUri = "tcp://" + mqttBroker.getHost() + ":" + mqttBroker.getFirstMappedPort();

        Config config = mock(Config.class);
        when(config.getString("mqtt-broker.topic")).thenReturn("#");
        when(config.getInt("mqtt-broker.qos")).thenReturn(1);
        when(config.getString("mqtt-broker.host")).thenReturn(brokerUri);
        when(config.getBoolean("mqtt-broker.manualAck")).thenReturn(true);
        when(config.getInt("mqtt-broker.maxInflight")).thenReturn(10000);
        when(config.getBoolean("mqtt-broker.cleanSession")).thenReturn(true);
        when(config.getInt("mqtt-broker.keepAliveInterval")).thenReturn(30);
        when(config.getString("mqtt-broker.clientId")).thenReturn("test_client");
        when(config.getBoolean("mqtt-broker.addRandomnessToClientId")).thenReturn(true);

        final AtomicInteger messageCounter = new AtomicInteger(0);

        final MqttConnector mqttConnector = new MqttConnector(config, Optional.empty(), new IMqttMessageHandler() {
            @Override
            public CompletableFuture<Void> handleMessage(String topic, MqttMessage message) {
                return CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(500);

                        messageCounter.incrementAndGet();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        });
        mqttConnector.connect();

        MqttClient client = new MqttClient(brokerUri, MqttClient.generateClientId(), new MemoryPersistence());
        client.connect();

        for (int i = 0; i < 5; i++) {
            client.publish("test", String.valueOf(i).getBytes(StandardCharsets.UTF_8), 1, false);
        }

        Thread.sleep(2000);
        client.disconnect();
        client.close(true);

        assertEquals(5, messageCounter.get());
    }*/
}
