package fi.hsl.pulsar.mqtt.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MqttPropertiesTest {

    @Test
    public void gettersAndSettersWork() {
        MqttProperties props = new MqttProperties();
        props.setBrokerUrl("tcp://localhost:1883");
        props.setTopic("test/#");
        props.setQos(2);
        props.setClientId("cid");
        props.setMaxInflight(123);
        props.setKeepAliveIntervalSeconds(11);
        props.setConnectionTimeoutSeconds(22);
        props.setUsername("u");
        props.setPassword("p");

        assertEquals("tcp://localhost:1883", props.getBrokerUrl());
        assertEquals("test/#", props.getTopic());
        assertEquals(2, props.getQos());
        assertEquals("cid", props.getClientId());
        assertEquals(123, props.getMaxInflight());
        assertEquals(11, props.getKeepAliveIntervalSeconds());
        assertEquals(22, props.getConnectionTimeoutSeconds());
        assertEquals("u", props.getUsername());
        assertEquals("p", props.getPassword());
    }
}
