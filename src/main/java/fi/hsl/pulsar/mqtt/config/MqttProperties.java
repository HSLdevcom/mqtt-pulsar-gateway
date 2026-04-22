package fi.hsl.pulsar.mqtt.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "mqtt")
public record MqttProperties(@NotBlank String brokerUrl, @NotBlank String topic, @Min(0) @Max(2) int qos,
        @NotBlank String clientId, boolean cleanSession, @Positive int maxInflight,
        @Positive int keepAliveIntervalSeconds, @Positive int connectionTimeoutSeconds, String username,
        String password) {
}
