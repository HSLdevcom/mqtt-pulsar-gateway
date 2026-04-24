package fi.hsl.pulsar.mqtt.service;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.SimpleAcknowledgment;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MqttToPulsarMessageHandlerTest {

    private static Message<?> buildMqttMessage(byte[] payload, String topic, SimpleAcknowledgment ack) {
        return MessageBuilder.withPayload(payload).setHeader(MqttHeaders.RECEIVED_TOPIC, topic)
                .setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ack).build();
    }

    @Test
    public void acksOnlyAfterPulsarFutureCompletes() throws Exception {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        when(mapper.toRawMessageBytes(anyString(), any(byte[].class))).thenReturn("mapped".getBytes());
        when(mapper.schemaVersion()).thenReturn(1);

        CompletableFuture<MessageId> pending = new CompletableFuture<>();
        when(publisher.publish(any(byte[].class), anyLong(), anyString(), anyInt())).thenReturn(pending);

        SimpleAcknowledgment ack = mock(SimpleAcknowledgment.class);
        Message<?> msg = buildMqttMessage("hello".getBytes(), "t/1", ack);

        new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg);

        // The future has not completed: ack must not have fired yet.
        verify(ack, never()).acknowledge();
        verify(shutdown, never()).exitWithFailure(any(Throwable.class));

        pending.complete(mock(MessageId.class));

        verify(ack).acknowledge();
        verify(shutdown, never()).exitWithFailure(any(Throwable.class));
    }

    @Test
    public void handleMessageDoesNotThrowWhenFutureCompletesExceptionally() {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        when(mapper.toRawMessageBytes(anyString(), any(byte[].class))).thenReturn("mapped".getBytes());
        when(mapper.schemaVersion()).thenReturn(1);

        PulsarClientException failure = mock(PulsarClientException.class);
        when(publisher.publish(any(byte[].class), anyLong(), anyString(), anyInt()))
                .thenReturn(CompletableFuture.failedFuture(failure));

        SimpleAcknowledgment ack = mock(SimpleAcknowledgment.class);
        Message<?> msg = buildMqttMessage("x".getBytes(), "t/e", ack);

        // No exception bubbles out of handleMessage: failure is signalled via failFastShutdown.
        new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg);

        verify(shutdown).exitWithFailure(same(failure));
        verify(ack, never()).acknowledge();
    }

    @Test
    public void failsFastWhenFutureCompletesWithTimeout() {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        when(mapper.toRawMessageBytes(anyString(), any(byte[].class))).thenReturn("mapped".getBytes());
        when(mapper.schemaVersion()).thenReturn(1);

        PulsarClientException.TimeoutException timeout = mock(PulsarClientException.TimeoutException.class);
        when(publisher.publish(any(byte[].class), anyLong(), anyString(), anyInt()))
                .thenReturn(CompletableFuture.failedFuture(timeout));

        SimpleAcknowledgment ack = mock(SimpleAcknowledgment.class);
        Message<?> msg = buildMqttMessage("x".getBytes(), "t/to", ack);

        new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg);

        verify(shutdown).exitWithFailure(same(timeout));
        verify(ack, never()).acknowledge();
    }

    @Test
    public void unwrapsCompletionExceptionBeforeFailingFast() {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        when(mapper.toRawMessageBytes(anyString(), any(byte[].class))).thenReturn("mapped".getBytes());
        when(mapper.schemaVersion()).thenReturn(1);

        PulsarClientException cause = mock(PulsarClientException.class);
        CompletableFuture<MessageId> future = new CompletableFuture<>();
        future.completeExceptionally(new CompletionException(cause));
        when(publisher.publish(any(byte[].class), anyLong(), anyString(), anyInt())).thenReturn(future);

        SimpleAcknowledgment ack = mock(SimpleAcknowledgment.class);
        Message<?> msg = buildMqttMessage("x".getBytes(), "t/ce", ack);

        new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg);

        verify(shutdown).exitWithFailure(same(cause));
        verify(ack, never()).acknowledge();
    }

    @Test
    public void acksPreserveOrderAcrossInterleavedFutures() {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        when(mapper.toRawMessageBytes(anyString(), any(byte[].class))).thenReturn("mapped".getBytes());
        when(mapper.schemaVersion()).thenReturn(1);

        CompletableFuture<MessageId> f1 = new CompletableFuture<>();
        CompletableFuture<MessageId> f2 = new CompletableFuture<>();
        CompletableFuture<MessageId> f3 = new CompletableFuture<>();
        when(publisher.publish(any(byte[].class), anyLong(), anyString(), anyInt())).thenReturn(f1).thenReturn(f2)
                .thenReturn(f3);

        SimpleAcknowledgment ack1 = mock(SimpleAcknowledgment.class);
        SimpleAcknowledgment ack2 = mock(SimpleAcknowledgment.class);
        SimpleAcknowledgment ack3 = mock(SimpleAcknowledgment.class);

        MqttToPulsarMessageHandler handler = new MqttToPulsarMessageHandler(mapper, publisher, shutdown);
        handler.handleMessage(buildMqttMessage("1".getBytes(), "t/o", ack1));
        handler.handleMessage(buildMqttMessage("2".getBytes(), "t/o", ack2));
        handler.handleMessage(buildMqttMessage("3".getBytes(), "t/o", ack3));

        // Complete in Pulsar-producer order: ordering per producer is the server guarantee.
        f1.complete(mock(MessageId.class));
        f2.complete(mock(MessageId.class));
        f3.complete(mock(MessageId.class));

        InOrder order = inOrder(ack1, ack2, ack3);
        order.verify(ack1).acknowledge();
        order.verify(ack2).acknowledge();
        order.verify(ack3).acknowledge();
    }

    @Test
    public void throwsIfTopicMissing() {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        Message<?> msg = MessageBuilder.withPayload("x".getBytes()).build();
        assertThrows(MessagingException.class,
                () -> new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg));
    }

    @Test
    public void throwsIfAckHeaderMissing() {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        Message<?> msg = MessageBuilder.withPayload("x".getBytes()).setHeader(MqttHeaders.RECEIVED_TOPIC, "t/3")
                .build();
        assertThrows(MessagingException.class,
                () -> new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg));
    }

    @Test
    public void throwsIfPayloadNotBytes() {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        SimpleAcknowledgment ack = mock(SimpleAcknowledgment.class);

        Message<?> msg = MessageBuilder.withPayload("not-bytes").setHeader(MqttHeaders.RECEIVED_TOPIC, "t/4")
                .setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ack).build();
        assertThrows(MessagingException.class,
                () -> new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg));
    }
}
