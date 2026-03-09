package fi.hsl.pulsar.mqtt.service;

import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.SimpleAcknowledgment;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MqttToPulsarMessageHandlerTest {

    @Test
    public void acksOnSuccessfulPublish() throws Exception {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        byte[] mqttPayload = "hello".getBytes();
        byte[] mappedPayload = "mapped".getBytes();
        when(mapper.toRawMessageBytes(anyString(), any(byte[].class))).thenReturn(mappedPayload);
        when(mapper.schemaVersion()).thenReturn(1);

        SimpleAcknowledgment ack = mock(SimpleAcknowledgment.class);

        Message<?> msg = MessageBuilder.withPayload(mqttPayload).setHeader(MqttHeaders.RECEIVED_TOPIC, "t/1")
                .setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ack).build();

        new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg);

        verify(publisher).publish(any(byte[].class), anyLong(), anyString(), anyInt());
        verify(ack).acknowledge();
        verify(shutdown, never()).exitWithFailure(any(Throwable.class));
    }

    @Test
    public void failsFastOnPulsarTimeoutAndDoesNotAck() throws Exception {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        when(mapper.toRawMessageBytes(anyString(), any(byte[].class))).thenReturn("mapped".getBytes());
        when(mapper.schemaVersion()).thenReturn(1);

        PulsarClientException.TimeoutException timeout = mock(PulsarClientException.TimeoutException.class);
        doThrow(timeout).when(publisher).publish(any(byte[].class), anyLong(), anyString(), anyInt());

        SimpleAcknowledgment ack = mock(SimpleAcknowledgment.class);

        Message<?> msg = MessageBuilder.withPayload("x".getBytes()).setHeader(MqttHeaders.RECEIVED_TOPIC, "t/2")
                .setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ack).build();

        assertThrows(MessagingException.class,
                () -> new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg));

        verify(shutdown).exitWithFailure(timeout);
        verify(ack, never()).acknowledge();
    }

    @Test
    public void failsFastOnPulsarErrorAndDoesNotAck() throws Exception {
        RawMessageMapper mapper = mock(RawMessageMapper.class);
        PulsarPublisher publisher = mock(PulsarPublisher.class);
        FailFastShutdown shutdown = mock(FailFastShutdown.class);

        when(mapper.toRawMessageBytes(anyString(), any(byte[].class))).thenReturn("mapped".getBytes());
        when(mapper.schemaVersion()).thenReturn(1);

        PulsarClientException failure = mock(PulsarClientException.class);
        doThrow(failure).when(publisher).publish(any(byte[].class), anyLong(), anyString(), anyInt());

        SimpleAcknowledgment ack = mock(SimpleAcknowledgment.class);

        Message<?> msg = MessageBuilder.withPayload("x".getBytes()).setHeader(MqttHeaders.RECEIVED_TOPIC, "t/5")
                .setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ack).build();

        assertThrows(MessagingException.class,
                () -> new MqttToPulsarMessageHandler(mapper, publisher, shutdown).handleMessage(msg));

        verify(shutdown).exitWithFailure(failure);
        verify(ack, never()).acknowledge();
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
