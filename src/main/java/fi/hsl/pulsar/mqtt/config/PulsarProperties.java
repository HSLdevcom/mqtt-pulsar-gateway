package fi.hsl.pulsar.mqtt.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "pulsar")
public record PulsarProperties(@NotBlank String host, @Positive int port, @NotBlank String topic,
        @Positive int sendTimeoutSeconds, @Positive int maxPendingMessages) {
}
