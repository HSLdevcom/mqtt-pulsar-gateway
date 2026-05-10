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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PulsarPublisherConstructorTest {

    @SuppressWarnings("unchecked")
    private static ProducerBuilder<byte[]> mockProducerBuilder(PulsarClient client) throws PulsarClientException {
        ProducerBuilder<byte[]> pb = mock(ProducerBuilder.class);
        when(client.newProducer(Schema.BYTES)).thenReturn(pb);
        when(pb.topic(anyString())).thenReturn(pb);
        when(pb.sendTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(pb);
        when(pb.maxPendingMessages(anyInt())).thenReturn(pb);
        when(pb.blockIfQueueFull(true)).thenReturn(pb);
        return pb;
    }

    private static ClientBuilder mockClientBuilder(PulsarClient... clients) throws PulsarClientException {
        ClientBuilder cb = mock(ClientBuilder.class);
        when(cb.serviceUrl(anyString())).thenReturn(cb);
        when(cb.connectionTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(cb);
        when(cb.operationTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(cb);
        if (clients.length == 1) {
            when(cb.build()).thenReturn(clients[0]);
        } else {
            PulsarClient first = clients[0];
            PulsarClient[] rest = new PulsarClient[clients.length - 1];
            System.arraycopy(clients, 1, rest, 0, rest.length);
            when(cb.build()).thenReturn(first, rest);
        }
        return cb;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void connectBuildsClientAndProducer() throws Exception {
        PulsarProperties props = new PulsarProperties("x", 6650, "mqtt-raw", 7, 42);
        FailFastShutdown failFastShutdown = mock(FailFastShutdown.class);

        PulsarClient client = mock(PulsarClient.class);
        Producer<byte[]> producer = mock(Producer.class);

        ProducerBuilder<byte[]> producerBuilder = mockProducerBuilder(client);
        when(producerBuilder.create()).thenReturn(producer);

        ClientBuilder clientBuilder = mockClientBuilder(client);

        try (MockedStatic<PulsarClient> pulsarClientStatic = mockStatic(PulsarClient.class)) {
            pulsarClientStatic.when(PulsarClient::builder).thenReturn(clientBuilder);

            PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown, 0);
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

        ProducerBuilder<byte[]> producerBuilder = mockProducerBuilder(client);
        when(producerBuilder.create()).thenThrow(producerError);

        ClientBuilder clientBuilder = mockClientBuilder(client);

        try (MockedStatic<PulsarClient> pulsarClientStatic = mockStatic(PulsarClient.class)) {
            pulsarClientStatic.when(PulsarClient::builder).thenReturn(clientBuilder);

            PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown, 0);
            publisher.connect();

            verify(client).close();
            verify(failFastShutdown).exitWithFailure(producerError);
        }
    }

    @Test
    public void connectCallsFailFastWithoutClosingClientWhenBuildFails() throws Exception {
        PulsarProperties props = new PulsarProperties("x", 6650, "mqtt-raw", 7, 42);
        FailFastShutdown failFastShutdown = mock(FailFastShutdown.class);

        PulsarClientException buildError = new PulsarClientException("build failed");

        ClientBuilder clientBuilder = mock(ClientBuilder.class);
        when(clientBuilder.serviceUrl(anyString())).thenReturn(clientBuilder);
        when(clientBuilder.connectionTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(clientBuilder);
        when(clientBuilder.operationTimeout(anyInt(), eq(TimeUnit.SECONDS))).thenReturn(clientBuilder);
        when(clientBuilder.build()).thenThrow(buildError);

        try (MockedStatic<PulsarClient> pulsarClientStatic = mockStatic(PulsarClient.class)) {
            pulsarClientStatic.when(PulsarClient::builder).thenReturn(clientBuilder);

            PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown, 0);
            publisher.connect();

            verify(failFastShutdown).exitWithFailure(buildError);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void connectSuppressesCloseExceptionWhenBothCreateAndCloseFail() throws Exception {
        PulsarProperties props = new PulsarProperties("x", 6650, "mqtt-raw", 7, 42);
        FailFastShutdown failFastShutdown = mock(FailFastShutdown.class);

        PulsarClient client = mock(PulsarClient.class);
        PulsarClientException producerError = new PulsarClientException("producer creation failed");
        PulsarClientException closeError = new PulsarClientException("close failed");

        ProducerBuilder<byte[]> producerBuilder = mockProducerBuilder(client);
        when(producerBuilder.create()).thenThrow(producerError);
        doThrow(closeError).when(client).close();

        ClientBuilder clientBuilder = mockClientBuilder(client);

        try (MockedStatic<PulsarClient> pulsarClientStatic = mockStatic(PulsarClient.class)) {
            pulsarClientStatic.when(PulsarClient::builder).thenReturn(clientBuilder);

            PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown, 0);
            publisher.connect();

            verify(client).close();
            verify(failFastShutdown).exitWithFailure(producerError);
            assertSame(closeError, producerError.getSuppressed()[0]);
        }
    }

    @Test
    public void publishReturnsPendingFutureWhenCalledBeforeConnect() {
        PulsarProperties props = new PulsarProperties("x", 6650, "mqtt-raw", 7, 42);
        FailFastShutdown failFastShutdown = mock(FailFastShutdown.class);
        // Producer not yet connected — producerReady never completed.
        PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown, 0);
        assertFalse(publisher.publish("p".getBytes(), 1L, "mqtt-raw", 1).isDone());
    }

    @Test
    public void twoArgConstructorSetsNotRunning() {
        PulsarProperties props = new PulsarProperties("x", 6650, "mqtt-raw", 7, 42);
        FailFastShutdown failFastShutdown = mock(FailFastShutdown.class);
        PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown);
        assertFalse(publisher.isRunning());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void connectCallsFailFastWhenInterruptedDuringRetrySleep() throws Exception {
        PulsarProperties props = new PulsarProperties("x", 6650, "mqtt-raw", 7, 42);
        FailFastShutdown failFastShutdown = mock(FailFastShutdown.class);

        PulsarClient client = mock(PulsarClient.class);
        ProducerBuilder<byte[]> producerBuilder = mockProducerBuilder(client);
        when(producerBuilder.create()).thenThrow(new PulsarClientException("always fails"));

        ClientBuilder clientBuilder = mockClientBuilder(client);

        Thread testThread = Thread.currentThread();
        // Interrupt the test thread shortly after connect() enters the retry sleep.
        Thread interrupter = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
            }
            testThread.interrupt();
        });
        interrupter.setDaemon(true);
        interrupter.start();

        try (MockedStatic<PulsarClient> pulsarClientStatic = mockStatic(PulsarClient.class)) {
            pulsarClientStatic.when(PulsarClient::builder).thenReturn(clientBuilder);
            // retryTimeoutMs=60s keeps us inside the retry loop; retryDelayMs=10s so the sleep is
            // long enough for the interrupter to fire.
            PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown, 60_000, 10_000);
            publisher.connect();

            verify(failFastShutdown).exitWithFailure(any(InterruptedException.class));
            assertFalse(publisher.isRunning());
        } finally {
            // Clear the interrupt flag so it doesn't bleed into subsequent tests.
            Thread.interrupted();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void connectSucceedsAfterTransientFailure() throws Exception {
        PulsarProperties props = new PulsarProperties("x", 6650, "mqtt-raw", 7, 42);
        FailFastShutdown failFastShutdown = mock(FailFastShutdown.class);

        PulsarClient failingClient = mock(PulsarClient.class);
        PulsarClient goodClient = mock(PulsarClient.class);
        Producer<byte[]> producer = mock(Producer.class);

        ProducerBuilder<byte[]> failingBuilder = mockProducerBuilder(failingClient);
        when(failingBuilder.create()).thenThrow(new PulsarClientException("transient"));

        ProducerBuilder<byte[]> goodBuilder = mockProducerBuilder(goodClient);
        when(goodBuilder.create()).thenReturn(producer);

        // build() returns failingClient on first call, goodClient on second
        ClientBuilder clientBuilder = mockClientBuilder(failingClient, goodClient);

        try (MockedStatic<PulsarClient> pulsarClientStatic = mockStatic(PulsarClient.class)) {
            pulsarClientStatic.when(PulsarClient::builder).thenReturn(clientBuilder);

            // retryTimeoutMs=5s, retryDelayMs=1ms so the retry loop runs without sleeping
            PulsarPublisher publisher = new PulsarPublisher(props, failFastShutdown, 5_000, 1);
            publisher.connect();

            verify(failingClient).close();
            verify(failFastShutdown, never()).exitWithFailure(any());
            assertTrue(publisher.isRunning());
        }
    }
}
