package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.health.HealthServer;
import fi.hsl.common.pulsar.PulsarApplication;
import org.apache.pulsar.client.api.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConfiguration {

    @Bean
    public Config typesafeConfig() {
        return ConfigParser.createConfig();
    }

    @Bean(destroyMethod = "close")
    public PulsarApplication pulsarApplication(Config config) throws Exception {
        return PulsarApplication.newInstance(config);
    }

    @Bean
    public Producer<byte[]> pulsarProducer(PulsarApplication app) {
        return app.getContext().getSingleProducer();
    }

    @Bean
    public HealthServer healthServer(PulsarApplication pulsarApplication) {
        final var pulsarAppContext = pulsarApplication.getContext();
        return pulsarAppContext.getHealthServer();
    }
}
