package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class Credentials {
    private static final Logger log = LoggerFactory.getLogger(Credentials.class);

    public final String username;
    public final String password;

    public Credentials(String user, String pw) {
        username = user;
        password = pw;
    }

    public static Optional<Credentials> readMqttCredentials(Config config) throws Exception {
        try {
            if (!config.getBoolean("mqtt-broker.credentials.required")) {
                log.info("Login credentials not required");
                return Optional.empty();
            }
            else {
                //Default path is what works with Docker out-of-the-box. Override with a local file if needed
                final String usernamePath = config.getString("mqtt-broker.credentials.usernameFilepath");
                log.debug("Reading username from " + usernamePath);
                final String username = Files.readString(Path.of(usernamePath), StandardCharsets.UTF_8);

                final String passwordPath = config.getString("mqtt-broker.credentials.passwordFilepath");
                log.debug("Reading password from " + passwordPath);
                final String password = Files.readString(Path.of(passwordPath), StandardCharsets.UTF_8);

                log.info("Login credentials read from files successfully");
                return Optional.of(new Credentials(username, password));
            }
        } catch (Exception e) {
            log.error("Failed to read login credentials from secret files", e);
            throw e;
        }
    }


}
