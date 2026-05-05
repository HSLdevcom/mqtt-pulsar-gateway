package fi.hsl.pulsar.mqtt;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.pulsar.mqtt.config.IntegrationConfiguration;
import fi.hsl.pulsar.mqtt.config.MqttProperties;
import fi.hsl.pulsar.mqtt.config.PulsarProperties;
import fi.hsl.pulsar.mqtt.service.FailFastShutdown;
import fi.hsl.pulsar.mqtt.service.MqttToPulsarMessageHandler;
import fi.hsl.pulsar.mqtt.service.PulsarPublisher;
import fi.hsl.pulsar.mqtt.service.RawMessageMapper;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.integration.channel.DirectChannel;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.pulsar.PulsarContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class MqttToPulsarFlowIT {

    private static final int MQTT_PORT = 1883;

    private static final GenericContainer<?> mqttBroker = new GenericContainer<>(
            DockerImageName.parse("eclipse-mosquitto:2.1.2-alpine"))
            .withExposedPorts(MQTT_PORT)
            .withCopyToContainer(Transferable.of("listener 1883 0.0.0.0\nallow_anonymous true\n"),
                    "/mosquitto/config/mosquitto.conf")
            .withCreateContainerCmdModifier(cmd -> bindToLocalhost(cmd, MQTT_PORT));

    private static final PulsarContainer pulsarBroker = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:4.2.0")).withEnv("PULSAR_PREFIX_advertisedAddress", "localhost")
            .withCreateContainerCmdModifier(
                    cmd -> bindToLocalhost(cmd, PulsarContainer.BROKER_PORT, PulsarContainer.BROKER_HTTP_PORT));

    @BeforeAll
    static void startContainers() {
        mqttBroker.start();
        pulsarBroker.start();
    }

    @AfterAll
    static void stopContainers() {
        mqttBroker.stop();
        pulsarBroker.stop();
    }

    @Test
    @Timeout(120)
    public void endToEndMqttToPulsar() throws Exception {
        String mqttBrokerUri = "tcp://" + mqttBroker.getHost() + ":" + mqttBroker.getFirstMappedPort();
        String pulsarHost = pulsarBroker.getHost();
        int pulsarPort = pulsarBroker.getMappedPort(PulsarContainer.BROKER_PORT);
        String pulsarServiceUrl = "pulsar://" + pulsarHost + ":" + pulsarPort;
        String pulsarTopic = "persistent://public/default/it-" + UUID.randomUUID();

        // Wire up the MQTT inbound adapter.
        MqttProperties mqttProps = new MqttProperties(mqttBrokerUri, "test/#", 1, "it-" + UUID.randomUUID(), true,
                10_000, 30, 10, null, null);

        IntegrationConfiguration cfg = new IntegrationConfiguration();
        var factory = cfg.mqttClientFactory(mqttProps);
        var adapter = cfg.mqttInboundAdapter(mqttProps, factory);
        adapter.setCompletionTimeout(5000);
        adapter.setDisconnectCompletionTimeout(5000);

        // Create a real PulsarPublisher connected to the Pulsar container.
        PulsarProperties pulsarProps = new PulsarProperties(pulsarHost, pulsarPort, pulsarTopic, 20, 10_000);
        PulsarPublisher publisher = new PulsarPublisher(pulsarProps);

        FailFastShutdown shutdown = mock(FailFastShutdown.class);
        RawMessageMapper mapper = new RawMessageMapper();
        MqttToPulsarMessageHandler handler = new MqttToPulsarMessageHandler(mapper, publisher, shutdown);

        // Create a Pulsar consumer BEFORE publishing so the subscription captures the message.
        PulsarClient consumerClient = PulsarClient.builder().serviceUrl(pulsarServiceUrl).build();
        Consumer<byte[]> consumer = consumerClient.newConsumer(Schema.BYTES).topic(pulsarTopic)
                .subscriptionName("it-verify").subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        // Use a latch to wait for the handler to finish processing.
        CountDownLatch handlerDone = new CountDownLatch(1);
        DirectChannel channel = new DirectChannel();
        channel.subscribe(message -> {
            handler.handleMessage(message);
            handlerDone.countDown();
        });
        adapter.setOutputChannel(channel);
        adapter.start();

        // Publish an MQTT message.
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);

        MqttClient mqttClient = new MqttClient(mqttBrokerUri, MqttClient.generateClientId());
        mqttClient.connect(options);
        mqttClient.publish("test/a", new MqttMessage(payload));

        // Wait for the handler to process the MQTT message.
        assertTrue(handlerDone.await(30, TimeUnit.SECONDS), "Handler did not process the MQTT message in time");

        // Consume the message from Pulsar and verify.
        Message<byte[]> pulsarMsg = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(pulsarMsg, "No message received from Pulsar");

        Mqtt.RawMessage rawMsg = Mqtt.RawMessage.parseFrom(pulsarMsg.getValue());
        assertEquals("test/a", rawMsg.getTopic());
        assertArrayEquals(payload, rawMsg.getPayload().toByteArray());

        // Verify Pulsar message properties.
        assertTrue(pulsarMsg.getEventTime() > 0, "Event time should be set");
        assertEquals("mqtt-raw", pulsarMsg.getProperty("protobuf-schema"));
        assertNotNull(pulsarMsg.getProperty("schema-version"));
        assertNotNull(pulsarMsg.getProperty("source-ts"));

        // Clean up.
        consumer.close();
        consumerClient.close();
        mqttClient.disconnect();
        mqttClient.close(true);
        publisher.close();
        adapter.stop();
        adapter.destroy();
    }

    /** Bind the given container ports to 127.0.0.1 so they are not reachable from the network. */
    private static void bindToLocalhost(CreateContainerCmd cmd, int... ports) {
        var bindings = new Ports();
        for (int port : ports) {
            bindings.bind(ExposedPort.tcp(port), Ports.Binding.bindIp("127.0.0.1"));
        }
        cmd.getHostConfig().withPortBindings(bindings);
    }
}
