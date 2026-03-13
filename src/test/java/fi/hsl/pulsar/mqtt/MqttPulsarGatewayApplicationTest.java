package fi.hsl.pulsar.mqtt;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

public class MqttPulsarGatewayApplicationTest {

    @Test
    public void mainInvokesSpringApplicationRun() {
        try (MockedStatic<SpringApplication> springApplication = mockStatic(SpringApplication.class)) {
            springApplication
                    .when(() -> SpringApplication.run(eq(MqttPulsarGatewayApplication.class), any(String[].class)))
                    .thenReturn(null);

            MqttPulsarGatewayApplication.main(new String[]{"--test=true"});

            springApplication
                    .verify(() -> SpringApplication.run(eq(MqttPulsarGatewayApplication.class), any(String[].class)));
        }
    }
}
