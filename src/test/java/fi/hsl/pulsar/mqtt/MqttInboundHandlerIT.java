package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SpringBootTest(classes = {MqttConfiguration.class, MqttInboundHandler.class,
        MqttInboundHandlerIT.TestBeans.class}, properties = {"spring.main.web-application-type=none",
                "spring.main.allow-bean-definition-overriding=true"})
class MqttInboundHandlerIT {

    @Container
    static GenericContainer<?> mqttBroker = new GenericContainer<>(DockerImageName.parse("hivemq/hivemq4"))
            .withExposedPorts(1883);

    static final AtomicInteger counter = new AtomicInteger();
    static volatile CountDownLatch latch;

    @BeforeEach
    void reset() {
        counter.set(0);
        latch = new CountDownLatch(5);
    }

    @Test
    @Timeout(30)
    void consumes_messages_from_broker_and_invokes_IMqttMessageHandler() throws Exception {
        String brokerUri = "tcp://" + mqttBroker.getHost() + ":" + mqttBroker.getMappedPort(1883);

        MqttClient publisher = new MqttClient(brokerUri, MqttClient.generateClientId(), new MemoryPersistence());
        publisher.connect();

        for (int i = 0; i < 5; i++) {
            publisher.publish("test", String.valueOf(i).getBytes(StandardCharsets.UTF_8), 1, false);
        }

        publisher.disconnect();
        publisher.close(true);

        boolean allReceived = latch.await(15, TimeUnit.SECONDS);
        assertTrue(allReceived, "Did not receive all messages in time");
        assertEquals(5, counter.get());
    }

    @TestConfiguration
    static class TestBeans {

        @Bean
        public Config config() {
            String brokerUri = "tcp://" + mqttBroker.getHost() + ":" + mqttBroker.getMappedPort(1883);

            return ConfigFactory.parseString("""
                    mqtt-broker {
                      host = "%s"
                      topic = "#"
                      qos = 1

                      clientId = "test_client"
                      addRandomnessToClientId = true

                      manualAck = true
                      cleanSession = false
                      maxInflight = 10000
                      keepAliveInterval = 30
                      completionTimeout = 10000

                      credentials {
                        required = false
                        username = ""
                        password = ""
                      }

                      retry {
                        maxRetries = 3
                        backOffInitialInterval = 100
                        backOffMultiplier = 2.0
                        backOffMaxInterval = 1000
                      }
                    }
                    """.formatted(brokerUri));
        }

        @Bean(name = "messageHandler")
        public IMqttMessageHandler testMessageHandler() {
            return (_, _) -> {
                counter.incrementAndGet();
                latch.countDown();
                return CompletableFuture.completedFuture(null);
            };
        }
    }
}
