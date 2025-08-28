package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      } else {
        String username = System.getenv("MQTT_BROKER_USERNAME");
        String password = System.getenv("MQTT_BROKER_PASSWORD");

        if (username == null || username.isEmpty() || password == null || password.isEmpty()) {
          log.error("Invalid login credentials");
          throw new IllegalArgumentException("Invalid MQTT login credentials");
        }

        log.info("Login credentials read from environment variables successfully");
        return Optional.of(new Credentials(username, password));
      }
    } catch (Exception e) {
      log.error("Failed to read login credentials from environment variables", e);
      throw e;
    }
  }
}
