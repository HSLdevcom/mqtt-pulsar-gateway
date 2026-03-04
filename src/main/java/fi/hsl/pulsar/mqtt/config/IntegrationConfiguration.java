package fi.hsl.pulsar.mqtt.config;

import fi.hsl.pulsar.mqtt.service.MqttToPulsarMessageHandler;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
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
@IntegrationComponentScan
public class IntegrationConfiguration {

    private static final Logger log = LoggerFactory.getLogger(IntegrationConfiguration.class);

    @Bean
    public MqttPahoClientFactory mqttClientFactory(MqttProperties mqttProperties) {
        if (mqttProperties.getBrokerUrl() == null || mqttProperties.getBrokerUrl().isBlank()) {
            throw new IllegalArgumentException("mqtt.brokerUrl is required");
        }
        if (mqttProperties.getTopic() == null || mqttProperties.getTopic().isBlank()) {
            throw new IllegalArgumentException("mqtt.topic is required");
        }

        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();

        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{mqttProperties.getBrokerUrl()});
        options.setCleanSession(true);
        options.setMaxInflight(mqttProperties.getMaxInflight());
        options.setAutomaticReconnect(true);
        options.setKeepAliveInterval(mqttProperties.getKeepAliveIntervalSeconds());
        options.setConnectionTimeout(mqttProperties.getConnectionTimeoutSeconds());

        if (mqttProperties.getUsername() != null && !mqttProperties.getUsername().isBlank()) {
            options.setUserName(mqttProperties.getUsername());
            if (mqttProperties.getPassword() != null) {
                options.setPassword(mqttProperties.getPassword().toCharArray());
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
        log.info("Configuring MQTT inbound adapter: brokerUrl={}, topic={}, qos={}, clientId={}, cleanSession=true",
                mqttProperties.getBrokerUrl(), mqttProperties.getTopic(), mqttProperties.getQos(),
                mqttProperties.getClientId());

        MqttPahoMessageDrivenChannelAdapter adapter = new MqttPahoMessageDrivenChannelAdapter(
                mqttProperties.getClientId(), mqttClientFactory, mqttProperties.getTopic());
        adapter.setQos(mqttProperties.getQos());
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
