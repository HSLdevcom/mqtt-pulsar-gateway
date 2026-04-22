package fi.hsl.pulsar.mqtt;

import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.pulsar.mqtt.config.IntegrationConfiguration;
import fi.hsl.pulsar.mqtt.config.MqttProperties;
import fi.hsl.pulsar.mqtt.service.FailFastShutdown;
import fi.hsl.pulsar.mqtt.service.MqttToPulsarMessageHandler;
import fi.hsl.pulsar.mqtt.service.PulsarPublisher;
import fi.hsl.pulsar.mqtt.service.RawMessageMapper;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.integration.channel.DirectChannel;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MqttToPulsarFlowIT {

    private static final GenericContainer<?> mqttBroker = new GenericContainer<>(
            DockerImageName.parse("eclipse-mosquitto:2.1.2-alpine")).withExposedPorts(1883)
            .withCopyToContainer(Transferable.of("listener 1883 0.0.0.0\nallow_anonymous true\n"),
                    "/mosquitto/config/mosquitto.conf");

    @BeforeAll
    static void startContainer() {
        mqttBroker.start();
    }

    @AfterAll
    static void stopContainer() {
        mqttBroker.stop();
    }

    @Test
    @Timeout(30)
    public void publishesWrappedProtobufToPulsarPublisher() throws Exception {
        String brokerUri = "tcp://" + mqttBroker.getHost() + ":" + mqttBroker.getFirstMappedPort();

        MqttProperties mqttProps = new MqttProperties(brokerUri, "test/#", 1, "it-" + UUID.randomUUID(), true, 10_000,
                30, 10, null, null);

        IntegrationConfiguration cfg = new IntegrationConfiguration();
        var factory = cfg.mqttClientFactory(mqttProps);
        var adapter = cfg.mqttInboundAdapter(mqttProps, factory);
        adapter.setCompletionTimeout(1000);
        adapter.setDisconnectCompletionTimeout(1000);

        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);
        RawMessageMapper mapper = new RawMessageMapper();
        MqttToPulsarMessageHandler handler = new MqttToPulsarMessageHandler(mapper, publisher, shutdown);

        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        CountDownLatch latch = new CountDownLatch(1);

        doAnswer(invocation -> {
            byte[] pulsarPayload = invocation.getArgument(0, byte[].class);
            Mqtt.RawMessage msg = Mqtt.RawMessage.parseFrom(pulsarPayload);
            assertEquals("test/a", msg.getTopic());
            assertArrayEquals(payload, msg.getPayload().toByteArray());
            latch.countDown();
            return null;
        }).when(publisher).publish(any(byte[].class), anyLong(), anyString(), anyInt());

        DirectChannel channel = new DirectChannel();
        channel.subscribe(handler::handleMessage);
        adapter.setOutputChannel(channel);
        adapter.start();

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);

        MqttClient client = new MqttClient(brokerUri, MqttClient.generateClientId());
        client.connect(options);
        client.publish("test/a", new MqttMessage(payload));

        assertTrue(latch.await(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS));
        verify(publisher).publish(any(byte[].class), anyLong(), anyString(), anyInt());

        client.disconnect();
        client.close(true);

        adapter.stop();
        adapter.destroy();
    }
}
