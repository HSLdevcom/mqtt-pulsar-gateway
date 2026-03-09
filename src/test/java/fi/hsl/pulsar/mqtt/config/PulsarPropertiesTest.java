package fi.hsl.pulsar.mqtt.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PulsarPropertiesTest {

    @Test
    public void gettersAndSettersWork() {
        PulsarProperties props = new PulsarProperties();
        props.setServiceUrl("pulsar://localhost:6650");
        props.setTopic("mqtt-raw");
        props.setSendTimeoutSeconds(9);
        props.setMaxPendingMessages(777);

        assertEquals("pulsar://localhost:6650", props.getServiceUrl());
        assertEquals("mqtt-raw", props.getTopic());
        assertEquals(9, props.getSendTimeoutSeconds());
        assertEquals(777, props.getMaxPendingMessages());
    }
}
