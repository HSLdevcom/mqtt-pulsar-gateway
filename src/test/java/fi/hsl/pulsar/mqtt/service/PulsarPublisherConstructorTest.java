package fi.hsl.pulsar.mqtt.service;

import fi.hsl.pulsar.mqtt.config.PulsarProperties;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PulsarPublisherConstructorTest {

    @Test
    @SuppressWarnings("unchecked")
    public void connectBuildsClientAndProducer() throws Exception {
        PulsarProperties props = new PulsarProperties("x", 6650, "mqtt-raw", 7, 42);
        FailFastShutdown failFastShutdown = mock(FailFastShutdown.class);

        PulsarClient client = mock(PulsarClient.class);
        Producer<byte[]> producer = mock(Producer.class);

        ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
        when(client.newProducer(Schema.BYTES)).thenReturn(producerBuilder);
        when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
        when(producerBuilder.sendTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(producerBuilder);
        when(producerBuilder.maxPendingMessages(anyInt())).thenReturn(producerBuilder);
        when(producerBuilder.blockIfQueueFull(true)).thenReturn(producerBuilder);
        when(producerBuilder.create()).thenReturn(producer);

        ClientBuilder clientBuilder = mock(ClientBuilder.class);
        when(clientBuilder.serviceUrl(anyString())).thenReturn(clientBuilder);
        when(clientBuilder.connectionTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(clientBuilder);
        when(clientBuilder.operationTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(clientBuilder);
        when(clientBuilder.build()).thenReturn(client);

        try (MockedStatic<PulsarClient> pulsarClientStatic = mockStatic(PulsarClient.class)) {
            pulsarClientStatic.when(PulsarClient::builder).thenReturn(clientBuilder);

            PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown);
            publisher.connect();

            verify(clientBuilder).serviceUrl("pulsar://x:6650");
            verify(clientBuilder).connectionTimeout(10, TimeUnit.SECONDS);
            verify(clientBuilder).operationTimeout(30, TimeUnit.SECONDS);
            verify(client).newProducer(Schema.BYTES);
            verify(producerBuilder).topic("mqtt-raw");
            verify(producerBuilder).sendTimeout(7, TimeUnit.SECONDS);
            verify(producerBuilder).maxPendingMessages(42);
            verify(producerBuilder).blockIfQueueFull(true);
            assertTrue(publisher.isRunning());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void connectClosesClientAndCallsFailFastWhenProducerCreationFails() throws Exception {
        PulsarProperties props = new PulsarProperties("x", 6650, "mqtt-raw", 7, 42);
        FailFastShutdown failFastShutdown = mock(FailFastShutdown.class);

        PulsarClient client = mock(PulsarClient.class);
        PulsarClientException producerError = new PulsarClientException("producer creation failed");

        ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
        when(client.newProducer(Schema.BYTES)).thenReturn(producerBuilder);
        when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
        when(producerBuilder.sendTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(producerBuilder);
        when(producerBuilder.maxPendingMessages(anyInt())).thenReturn(producerBuilder);
        when(producerBuilder.blockIfQueueFull(true)).thenReturn(producerBuilder);
        when(producerBuilder.create()).thenThrow(producerError);

        ClientBuilder clientBuilder = mock(ClientBuilder.class);
        when(clientBuilder.serviceUrl(anyString())).thenReturn(clientBuilder);
        when(clientBuilder.connectionTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(clientBuilder);
        when(clientBuilder.operationTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(clientBuilder);
        when(clientBuilder.build()).thenReturn(client);

        try (MockedStatic<PulsarClient> pulsarClientStatic = mockStatic(PulsarClient.class)) {
            pulsarClientStatic.when(PulsarClient::builder).thenReturn(clientBuilder);

            PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown);
            publisher.connect();

            verify(client).close();
            verify(failFastShutdown).exitWithFailure(producerError);
        }
    }
}
