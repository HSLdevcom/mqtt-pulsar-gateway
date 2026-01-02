package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.pulsar.PulsarApplication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.handler.advice.RequestHandlerRetryAdvice;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

@Configuration
@EnableIntegration
public class MqttConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MqttConfiguration.class);

    @Bean
    public MessageChannel mqttInputChannel(Executor mqttExecutor) {
        return new ExecutorChannel(mqttExecutor);
    }

    @Bean
    public Executor mqttExecutor(Config config) {
        return Executors.newFixedThreadPool(config.getInt("mqtt-broker.threadPool"));
    }

    @Bean
    public MqttConnectOptions mqttConnectOptions(Config config) throws Exception {
        MqttConnectOptions connectOptions = new MqttConnectOptions();

        connectOptions.setAutomaticReconnect(true);
        connectOptions.setCleanSession(config.getBoolean("mqtt-broker.cleanSession"));
        connectOptions.setMaxInflight(config.getInt("mqtt-broker.maxInflight"));
        connectOptions.setKeepAliveInterval(config.getInt("mqtt-broker.keepAliveInterval"));

        final var mqttCredentials = Credentials.readMqttCredentials(config);

        mqttCredentials.ifPresent(credentials -> {
            connectOptions.setUserName(credentials.username);
            connectOptions.setPassword(credentials.password.toCharArray());
        });

        connectOptions.setConnectionTimeout(10);

        return connectOptions;
    }


    @Bean
    public IMqttMessageHandler messageHandler(Config config,  PulsarApplication pulsarApplication) {
        return new MessageProcessor(config, pulsarApplication);
    }

    @Bean
    public MqttConnector mqttConnector(Config config, MqttConnectOptions connectOptions, IMqttMessageHandler messageHandler) {
        return new MqttConnector(config, connectOptions, messageHandler);
    }

    @Bean
    public MqttPahoClientFactory mqttPahoClientFactory(MqttConnectOptions mqttConnectOptions) {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        factory.setConnectionOptions(mqttConnectOptions);
        return factory;
    }

    @Bean
    public MqttPahoMessageDrivenChannelAdapter mqttInboundAdapter(Config config, Executor mqttExecutor, MqttPahoClientFactory mqttPahoClientFactory) {
        String clientId = createClientId(config);

        final String broker =  config.getString("mqtt-broker.host");
        final String topic = config.getString("mqtt-broker.topic");

        MqttPahoMessageDrivenChannelAdapter adapter = new MqttPahoMessageDrivenChannelAdapter(
                broker,
                clientId,
                mqttPahoClientFactory,
                topic
        );

        adapter.setQos(config.getInt("mqtt-broker.qos"));
        adapter.setManualAcks(config.getBoolean("mqtt-broker.manualAck"));
        adapter.setOutputChannel(mqttInputChannel(mqttExecutor));
        adapter.setCompletionTimeout(config.getInt("mqtt-broker.completionTimeout"));

        return adapter;
    }

    @Bean
    public RequestHandlerRetryAdvice mqttRetryAdvice(Config config) {
        RequestHandlerRetryAdvice advice = new RequestHandlerRetryAdvice();

        ExponentialBackOff exponentialBackoff = new ExponentialBackOff(
                config.getLong("mqtt-broker.retry.backOffInitialInterval"),
                config.getDouble("mqtt-broker.retry.backOffMultiplier")
        );
        exponentialBackoff.setMaxInterval(config.getLong("mqtt-broker.retry.backOffMaxInterval"));

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxRetries(config.getInt("mqtt-broker.retry.maxRetries"))
                .includes(List.of(TimeoutException.class, PulsarClientException.class))
                .build();

        advice.setBackOff(exponentialBackoff);
        advice.setRetryPolicy(retryPolicy);

        return advice;
    }

    private static String createClientId(Config config) {
        String clientId = config.getString("mqtt-broker.clientId");
        if (config.getBoolean("mqtt-broker.addRandomnessToClientId")) { // This prevents persistent delivery of messages
            log.info("Creating random client ID for MQTT subscription");
            clientId += "-" + UUID.randomUUID().toString().substring(0, 8);
        }
        log.info("Created client ID for MQTT subscription: {}", clientId);
        return clientId;
    }

}
