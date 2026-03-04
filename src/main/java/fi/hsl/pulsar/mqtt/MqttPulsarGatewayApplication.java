package fi.hsl.pulsar.mqtt;

import fi.hsl.pulsar.mqtt.config.MqttProperties;
import fi.hsl.pulsar.mqtt.config.PulsarProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({MqttProperties.class, PulsarProperties.class})
public class MqttPulsarGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(MqttPulsarGatewayApplication.class, args);
    }
}
