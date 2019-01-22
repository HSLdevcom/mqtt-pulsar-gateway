package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Scanner;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    static class Credentials {
        String username;
        String password;
        public Credentials(String user, String pw) {
            username = user;
            password = pw;
        }
    }

    private static Credentials readMqttCredentials(Config config) {
        String username = "";
        String password = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String usernamePath = config.getString("mqtt-broker.usernameFilepath");
            log.debug("Reading username from " + usernamePath);
            username = new Scanner(new File(usernamePath)).useDelimiter("\\Z").next();

            final String passwordPath = config.getString("mqtt-broker.passwordFilepath");
            log.debug("Reading password from " + passwordPath);
            password = new Scanner(new File(passwordPath)).useDelimiter("\\Z").next();

        } catch (Exception e) {
            log.error("Failed to read secret files", e);
        }

        return new Credentials(username, password);
    }


    public static void main(String[] args) {
        log.info("Launching MQTT-Pulsar-Gateway");

        Config config = ConfigParser.createConfig();
        Credentials credentials = readMqttCredentials(config);

        log.info("Configurations read, connecting.");
        MqttConnector connector = null;
        PulsarApplication app = null;
        try {
            app = PulsarApplication.newInstance(config);
            connector = MqttConnector.newInstance(config, credentials.username, credentials.password);

            MessageProcessor.newInstance(connector, app);
            log.info("Starting to process messages");
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
