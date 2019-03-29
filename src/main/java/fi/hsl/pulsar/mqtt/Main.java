package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;
import java.util.Scanner;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Launching MQTT-Pulsar-Gateway");

        MqttConnector connector = null;
        PulsarApplication app = null;
        try {
            String sourceType = ConfigUtils.getEnvOrThrow("SOURCE");
            Config config = getConfig(sourceType);
            Optional<Credentials> credentials = Credentials.readMqttCredentials(config);

            log.info("Configurations read, connecting.");

            app = PulsarApplication.newInstance(config);
            connector = new MqttConnector(config, credentials);

            IMapperFactory factory = getMapperFactory(sourceType);
            MessageProcessor processor = new MessageProcessor(config, app, connector, factory);
            //Let's subscribe to connector before connecting so we'll get all the events.
            connector.subscribe(processor);

            connector.connect();

            log.info("Connections established, let's process some messages");
        }
        catch (Exception e) {
            log.error("Exception at main", e);
            if (app != null) {
                app.close();
            }
            if (connector != null) {
                connector.close();
            }
        }
    }

    private static Config getConfig(final String source) {
        switch (source) {
            case "hfp": return ConfigParser.createConfig("hfp.conf");
            case "metro-schedule": return ConfigParser.createConfig("metro-schedule.conf");
            default: throw new IllegalArgumentException(String.format("Failed to get Config specified by env var SOURCE=%s.", source));
        }
    }

    private static IMapperFactory getMapperFactory(final String source) {
        switch (source) {
            case "hfp": return new RawMessageFactory();
            case "metro-schedule": return new MetroScheduleFactory();
            default: throw new IllegalArgumentException(String.format("Failed to get IMapperFactory specified by env var SOURCE=%s.", source));
        }
    }
}
