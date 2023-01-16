package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MqttConnectorTest {
    @Rule
    public GenericContainer mqttBroker = new GenericContainer(DockerImageName.parse("hivemq/hivemq4"))
            .withExposedPorts(1883);

    @Test
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
    }
}
