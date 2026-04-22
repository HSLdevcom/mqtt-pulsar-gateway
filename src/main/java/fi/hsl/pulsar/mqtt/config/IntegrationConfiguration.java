package fi.hsl.pulsar.mqtt.config;

import fi.hsl.pulsar.mqtt.service.MqttToPulsarMessageHandler;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

@Configuration
@EnableIntegration
public class IntegrationConfiguration {

    private static final Logger log = LoggerFactory.getLogger(IntegrationConfiguration.class);

    @Bean
    public MqttPahoClientFactory mqttClientFactory(MqttProperties mqttProperties) {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();

        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{mqttProperties.brokerUrl()});
        options.setCleanSession(mqttProperties.cleanSession());
        options.setMaxInflight(mqttProperties.maxInflight());
        options.setAutomaticReconnect(true);
        options.setKeepAliveInterval(mqttProperties.keepAliveIntervalSeconds());
        options.setConnectionTimeout(mqttProperties.connectionTimeoutSeconds());

        if (mqttProperties.username() != null && !mqttProperties.username().isBlank()) {
            options.setUserName(mqttProperties.username());
            if (mqttProperties.password() != null) {
                options.setPassword(mqttProperties.password().toCharArray());
            }
        }

        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MqttPahoMessageDrivenChannelAdapter mqttInboundAdapter(MqttProperties mqttProperties,
            MqttPahoClientFactory mqttClientFactory) {
        log.info("Configuring MQTT inbound adapter: brokerUrl={}, topic={}, qos={}, clientId={}, cleanSession={}",
                mqttProperties.brokerUrl(), mqttProperties.topic(), mqttProperties.qos(), mqttProperties.clientId(),
                mqttProperties.cleanSession());

        MqttPahoMessageDrivenChannelAdapter adapter = new MqttPahoMessageDrivenChannelAdapter(mqttProperties.clientId(),
                mqttClientFactory, mqttProperties.topic());
        adapter.setQos(mqttProperties.qos());
        adapter.setManualAcks(true);

        DefaultPahoMessageConverter converter = new DefaultPahoMessageConverter();
        converter.setPayloadAsBytes(true);
        adapter.setConverter(converter);

        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler mqttToPulsarServiceActivator(MqttToPulsarMessageHandler handler) {
        return handler;
    }
}
