package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.health.HealthServer;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;
import java.util.Scanner;
import java.util.function.BooleanSupplier;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Launching MQTT-Pulsar-Gateway");

        MqttConnector connector = null;
        PulsarApplication app = null;
        try {
            Config config = ConfigParser.createConfig();
            Optional<Credentials> credentials = Credentials.readMqttCredentials(config);

            log.info("Configurations read, connecting.");

            app = PulsarApplication.newInstance(config);
            PulsarApplicationContext context = app.getContext();
            connector = new MqttConnector(config, credentials);

            MessageProcessor processor = new MessageProcessor(config, app, connector);
            //Let's subscribe to connector before connecting so we'll get all the events.
            connector.subscribe(processor);

            connector.connect();

            HealthServer healthServer = context.getHealthServer();
            final BooleanSupplier mqttHealthCheck = () -> {
                if (processor != null) {
                    return processor.isMqttConnected();
                }
                return false;
            };

            if (healthServer != null) {
                healthServer.addCheck(mqttHealthCheck);
            }

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
}
