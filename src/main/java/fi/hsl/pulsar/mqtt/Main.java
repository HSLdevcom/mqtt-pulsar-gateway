package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.health.HealthServer;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

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

            MessageProcessor processor = new MessageProcessor(config, app);

            connector = new MqttConnector(config, credentials, processor);
            connector.connect();

            HealthServer healthServer = context.getHealthServer();
            if (healthServer != null) {
                healthServer.addCheck(connector::isMqttConnected);
                healthServer.addCheck(processor::isLastMsgSendIntervalHealthy);
            }

            log.info("Connections established, let's process some messages");
            processor.processMessages();
        } catch (Exception e) {
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
