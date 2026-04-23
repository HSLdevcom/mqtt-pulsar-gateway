package fi.hsl.pulsar.mqtt.service;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PulsarPublisherTest {

    @Test
    public void publishSetsExpectedPropertiesAndSends() throws Exception {
        @SuppressWarnings("unchecked")
        Producer<byte[]> producer = mock(Producer.class);
        @SuppressWarnings("unchecked")
        TypedMessageBuilder<byte[]> builder = mock(TypedMessageBuilder.class);

        when(producer.newMessage()).thenReturn(builder);
        when(builder.eventTime(anyLong())).thenReturn(builder);
        when(builder.value(any(byte[].class))).thenReturn(builder);
        when(builder.properties(anyMap())).thenReturn(builder);

        PulsarClient client = mock(PulsarClient.class);

        PulsarPublisher publisher = new PulsarPublisher(client, producer);

        byte[] payload = "p".getBytes();
        publisher.publish(payload, 123L, "mqtt-raw", 1);

        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.captor();
        verify(builder).properties(propsCaptor.capture());
        Map<String, String> props = propsCaptor.getValue();

        assertEquals("123", props.get("source-ts"));
        assertEquals("mqtt-raw", props.get("protobuf-schema"));
        assertEquals("1", props.get("schema-version"));

        verify(builder).send();
    }

    @Test
    public void closeClosesProducerAndClient() throws Exception {
        @SuppressWarnings("unchecked")
        Producer<byte[]> producer = mock(Producer.class);
        PulsarClient client = mock(PulsarClient.class);

        PulsarPublisher publisher = new PulsarPublisher(client, producer);
        publisher.close();

        verify(producer).close();
        verify(client).close();
    }

    @Test
    public void closeSwallowsCloseErrors() throws Exception {
        @SuppressWarnings("unchecked")
        Producer<byte[]> producer = mock(Producer.class);
        PulsarClient client = mock(PulsarClient.class);

        doThrow(new RuntimeException("boom")).when(producer).close();
        doThrow(new RuntimeException("boom")).when(client).close();

        PulsarPublisher publisher = new PulsarPublisher(client, producer);
        publisher.close();
    }
}
