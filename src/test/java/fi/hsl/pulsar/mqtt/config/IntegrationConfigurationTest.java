package fi.hsl.pulsar.mqtt.config;

import org.junit.jupiter.api.Test;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.inbound.AbstractMqttMessageDrivenChannelAdapter;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntegrationConfigurationTest {

    @Test
    public void mqttClientFactoryUsesCleanSessionTrue() throws Exception {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "#", 1, "test", true, 1234, 12, 13, "user",
                "pw");

        IntegrationConfiguration cfg = new IntegrationConfiguration();
        DefaultMqttPahoClientFactory factory = (DefaultMqttPahoClientFactory) cfg.mqttClientFactory(props);

        MqttConnectOptions options = factory.getConnectionOptions();

        assertNotNull(options);
        assertTrue(options.isCleanSession());
        assertEquals(1234, options.getMaxInflight());
        assertEquals(12, options.getKeepAliveInterval());
        assertEquals(13, options.getConnectionTimeout());
        assertEquals("user", options.getUserName());
        assertEquals("pw", new String(options.getPassword()));
    }

    @Test
    public void mqttInboundAdapterManualAcksEnabled() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "#", 1, "test", true, 10_000, 30, 10, null,
                null);

        IntegrationConfiguration cfg = new IntegrationConfiguration();
        var factory = cfg.mqttClientFactory(props);
        MqttPahoMessageDrivenChannelAdapter adapter = cfg.mqttInboundAdapter(props, factory);

        assertTrue(isManualAcks(adapter));
        assertEquals(1, adapter.getQos()[0]);
    }

    private static boolean isManualAcks(AbstractMqttMessageDrivenChannelAdapter<?, ?> adapter) {
        try {
            Field manualAcks = AbstractMqttMessageDrivenChannelAdapter.class.getDeclaredField("manualAcks");
            manualAcks.setAccessible(true);
            return (boolean) manualAcks.get(adapter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
