package fi.hsl.pulsar.mqtt.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.Optional;

@Validated
@ConfigurationProperties(prefix = "mqtt")
public record MqttProperties(@NotBlank String brokerUrl, @NotBlank String topic, @Min(0) @Max(2) int qos,
        @NotBlank String clientId, boolean cleanSession, @Positive int maxInflight,
        @Positive int keepAliveIntervalSeconds, @Positive int connectionTimeoutSeconds, String username,
        String password) {

    public record Credentials(String username, String password) {
    }

    public Optional<Credentials> credentials() {
        if (username != null && !username.isBlank() && password != null && !password.isBlank()) {
            return Optional.of(new Credentials(username, password));
        }
        return Optional.empty();
    }

    @AssertTrue(message = "username and password must both be provided or both be absent")
    public boolean isCredentialsComplete() {
        boolean hasUsername = username != null && !username.isBlank();
        boolean hasPassword = password != null && !password.isBlank();
        return hasUsername == hasPassword;
    }
}
