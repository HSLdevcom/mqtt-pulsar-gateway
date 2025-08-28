package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import java.util.Optional;
import java.util.UUID;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttConnector implements MqttCallbackExtended {
  private static final Logger log = LoggerFactory.getLogger(MqttConnector.class);

  private final String mqttTopic;
  private final int qos;
  private final String clientId;
  private final String broker;
  private final boolean manualAck;

  private final IMqttMessageHandler messageHandler;

  private final MqttConnectOptions connectOptions;
  private MqttAsyncClient mqttClient;

  public MqttConnector(
      Config config, Optional<Credentials> maybeCredentials, IMqttMessageHandler messageHandler) {
    mqttTopic = config.getString("mqtt-broker.topic");
    qos = config.getInt("mqtt-broker.qos");
    clientId = createClientId(config);
    broker = config.getString("mqtt-broker.host");
    manualAck = config.getBoolean("mqtt-broker.manualAck");

    final int maxInFlight = config.getInt("mqtt-broker.maxInflight");
    final boolean cleanSession = config.getBoolean("mqtt-broker.cleanSession");
    final int keepAliveInterval = config.getInt("mqtt-broker.keepAliveInterval");

    this.messageHandler = messageHandler;

    connectOptions = new MqttConnectOptions();
    connectOptions.setCleanSession(
        cleanSession); // This should be false for persistent subscription
    connectOptions.setMaxInflight(maxInFlight);
    connectOptions.setAutomaticReconnect(true);
    connectOptions.setKeepAliveInterval(keepAliveInterval);

    maybeCredentials.ifPresent(
        credentials -> {
          connectOptions.setUserName(credentials.username);
          connectOptions.setPassword(credentials.password.toCharArray());
        });
    connectOptions.setConnectionTimeout(10);
  }

  private static String createClientId(Config config) {
    String clientId = config.getString("mqtt-broker.clientId");
    if (config.getBoolean(
        "mqtt-broker.addRandomnessToClientId")) { // This prevents persistent delivery of messages
      log.info("Creating random client ID for MQTT subscription");
      clientId += "-" + UUID.randomUUID().toString().substring(0, 8);
    }
    log.info("Created client ID for MQTT subscription: {}", clientId);
    return clientId;
  }

  public void connect() throws Exception {
    MqttAsyncClient client = null;
    try {
      // Let's use memory persistance to optimize throughput
      client = new MqttAsyncClient(broker, clientId, new MemoryPersistence());
      client.setManualAcks(manualAck);
      client.setCallback(
          this); // Let's add the callback before connecting so we won't lose any messages

      log.info("Connecting to MQTT broker at {}", broker);

      mqttClient = client;

      IMqttToken token = client.connect(connectOptions);
      token.waitForCompletion();

      log.info("Connection to MQTT completed: {}", token.isComplete());
      if (token.getException() != null) {
        throw token.getException();
      }
    } catch (Exception e) {
      log.error("Error connecting to MQTT broker", e);
      if (client != null) {
        // Paho doesn't close the connection threads unless we force-close it.
        client.close(true);
        mqttClient = null;
      }
      throw e;
    }
  }

  @Override
  public void connectComplete(boolean reconnect, String brokerUri) {
    log.info("Connected to MQTT broker at {}, reconnect: {}", brokerUri, reconnect);

    try {
      log.info("Subscribing to topic {} with QoS {}", mqttTopic, qos);
      mqttClient.subscribe(mqttTopic, qos);
    } catch (MqttException e) {
      log.error("Error subscribing to MQTT broker", e);
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void connectionLost(Throwable cause) {
    log.error("Connection lost to MQTT broker at {}, reconnecting...", broker, cause);
    mqttClient.getDebug().dumpClientDebug();
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) {
    messageHandler
        .handleMessage(topic, message)
        .whenComplete(
            (res, err) -> {
              if (err != null) {
                log.error("Failed to handle message, exiting application...");
                close();
              } else if (manualAck) {
                try {
                  mqttClient.messageArrivedComplete(message.getId(), message.getQos());
                } catch (MqttException e) {
                  log.error("Failed acking message", e);
                  throw new RuntimeException(e);
                }
              }
            });
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {}

  public void close() {
    if (mqttClient == null) {
      log.warn("Cannot close MQTT connection since it's null");
      return;
    }

    try {
      log.info("Closing MqttConnector resources");
      // Paho doesn't close the connection threads unless we first disconnect and then force-close
      // it.
      mqttClient.disconnectForcibly(1000L, 1000L);
      mqttClient.close(true);
      mqttClient = null;
    } catch (Exception e) {
      log.error("Failed to close MQTT client connection", e);
    }
  }

  public boolean isMqttConnected() {
    if (mqttClient != null) {
      final boolean mqttConnected = mqttClient.isConnected();
      if (!mqttConnected) {
        log.error("Health check: MQTT is not connected");
      }
      return mqttConnected;
    }
    return false;
  }
}
