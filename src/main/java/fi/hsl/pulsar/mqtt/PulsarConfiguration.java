package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConfiguration {

    @Bean
    public Config typesafeConfig() {
        return ConfigParser.createConfig();
    }

    @Bean
    public PulsarApplication pulsarApplication(Config config) throws Exception {
        return PulsarApplication.newInstance(config);
    }

}
