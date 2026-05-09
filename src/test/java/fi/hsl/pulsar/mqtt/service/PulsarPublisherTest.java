package fi.hsl.pulsar.mqtt.service;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PulsarPublisherTest {

    @SuppressWarnings("unchecked")
    private static TypedMessageBuilder<byte[]> mockBuilder(Producer<byte[]> producer) {
        TypedMessageBuilder<byte[]> builder = mock(TypedMessageBuilder.class);
        when(producer.newMessage()).thenReturn(builder);
        when(builder.eventTime(anyLong())).thenReturn(builder);
        when(builder.value(any(byte[].class))).thenReturn(builder);
        when(builder.properties(anyMap())).thenReturn(builder);
        return builder;
    }

    @Test
    public void publishSetsExpectedPropertiesAndReturnsFutureFromSendAsync() throws Exception {
        @SuppressWarnings("unchecked")
        Producer<byte[]> producer = mock(Producer.class);
        TypedMessageBuilder<byte[]> builder = mockBuilder(producer);

        MessageId messageId = mock(MessageId.class);
        CompletableFuture<MessageId> sendFuture = CompletableFuture.completedFuture(messageId);
        when(builder.sendAsync()).thenReturn(sendFuture);

        PulsarClient client = mock(PulsarClient.class);

        PulsarPublisher publisher = new PulsarPublisher(client, producer);

        byte[] payload = "p".getBytes();
        CompletableFuture<MessageId> result = publisher.publish(payload, 123L, "mqtt-raw", 1);

        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.captor();
        verify(builder).properties(propsCaptor.capture());
        Map<String, String> props = propsCaptor.getValue();

        assertEquals("123", props.get("source-ts"));
        assertEquals("mqtt-raw", props.get("protobuf-schema"));
        assertEquals("1", props.get("schema-version"));

        verify(builder).sendAsync();
        assertTrue(result.isDone());
        assertSame(messageId, result.get());
    }

    @Test
    public void publishReturnsFailedFutureWhenSendAsyncThrowsSynchronously() {
        @SuppressWarnings("unchecked")
        Producer<byte[]> producer = mock(Producer.class);
        TypedMessageBuilder<byte[]> builder = mockBuilder(producer);

        RuntimeException failure = new RuntimeException("producer closed");
        when(builder.sendAsync()).thenThrow(failure);

        PulsarClient client = mock(PulsarClient.class);
        PulsarPublisher publisher = new PulsarPublisher(client, producer);

        CompletableFuture<MessageId> result = publisher.publish("p".getBytes(), 1L, "mqtt-raw", 1);

        assertTrue(result.isCompletedExceptionally());
        ExecutionException ee = assertThrows(ExecutionException.class, result::get);
        assertSame(failure, ee.getCause());
    }

    @Test
    public void publishReturnsFailedFutureWhenProducerNotYetInitialized() {
        PulsarPublisher publisher = new PulsarPublisher(mock(PulsarClient.class), null);

        CompletableFuture<MessageId> result = publisher.publish("p".getBytes(), 1L, "mqtt-raw", 1);

        assertTrue(result.isCompletedExceptionally());
        ExecutionException ee = assertThrows(ExecutionException.class, result::get);
        assertInstanceOf(IllegalStateException.class, ee.getCause());
    }

    @Test
    public void stopFlushesBeforeClosingProducer() throws Exception {
        @SuppressWarnings("unchecked")
        Producer<byte[]> producer = mock(Producer.class);
        PulsarClient client = mock(PulsarClient.class);

        PulsarPublisher publisher = new PulsarPublisher(client, producer);
        publisher.stop();

        InOrder order = inOrder(producer, client);
        order.verify(producer).flush();
        order.verify(producer).close();
        order.verify(client).close();
    }

    @Test
    public void stopSwallowsErrorsFromFlushAndClose() throws Exception {
        @SuppressWarnings("unchecked")
        Producer<byte[]> producer = mock(Producer.class);
        PulsarClient client = mock(PulsarClient.class);

        doThrow(new RuntimeException("boom")).when(producer).flush();
        doThrow(new RuntimeException("boom")).when(producer).close();
        doThrow(new RuntimeException("boom")).when(client).close();

        PulsarPublisher publisher = new PulsarPublisher(client, producer);
        publisher.stop();
    }
}
