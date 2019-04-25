package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Optional;

public class MqttConnector implements MqttCallback {
    private static final Logger log = LoggerFactory.getLogger(MqttConnector.class);

    private final String mqttTopic;
    private final int qos;
    private final String clientId;
    private final String broker;

    private final MqttConnectOptions connectOptions;
    private final LinkedList<IMqttMessageHandler> handlers = new LinkedList<>();

    private MqttClient mqttClient;

    public MqttConnector(Config config, Optional<Credentials> maybeCredentials) {
        mqttTopic = config.getString("mqtt-broker.topic");
        qos = config.getInt("mqtt-broker.qos");
        clientId = config.getString("mqtt-broker.clientId");
        broker = config.getString("mqtt-broker.host");

        final int maxInFlight = config.getInt("mqtt-broker.maxInflight");
        final boolean cleanSession = config.getBoolean("mqtt-broker.cleanSession");

        connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(cleanSession);
        connectOptions.setMaxInflight(maxInFlight);
        connectOptions.setAutomaticReconnect(false); //Let's abort on connection errors

        maybeCredentials.ifPresent(credentials -> {
            connectOptions.setUserName(credentials.username);
            connectOptions.setPassword(credentials.password.toCharArray());
        });
        connectOptions.setConnectionTimeout(10);
    }

    public void subscribe(IMqttMessageHandler handler) {
        //let's not subscribe to the actual client. we have our own observables here
        //since we want to provide the disconnected-event via the same interface.
        log.info("Adding subscription");
        handlers.add(handler);
    }

    public void connect() throws Exception {
        MqttClient client = null;
        try {
            //Let's use memory persistance to optimize throughput.
            MemoryPersistence memoryPersistence = new MemoryPersistence();

            client = new MqttClient(broker, clientId, memoryPersistence);
            client.setCallback(this); //Let's add the callback before connecting so we won't lose any messages

            log.info(String.format("Connecting to mqtt broker %s", broker));
            IMqttToken token = client.connectWithResult(connectOptions);

            log.info("Connection to MQTT completed? {}", token.isComplete());
            if (token.getException() != null) {
                throw token.getException();
            }
        }
        catch (Exception e) {
            log.error("Error connecting to MQTT broker", e);
            if (client != null) {
                //Paho doesn't close the connection threads unless we force-close it.
                client.close(true);
            }
            throw e;
        }

        try {
            log.info("Subscribing to topic {} with QoS {}", mqttTopic, qos);
            mqttClient = client;
            mqttClient.subscribe(mqttTopic, qos);
        }
        catch (Exception e) {
            log.error("Error subscribing to MQTT broker", e);
            close();
            throw e;
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.error("Connection to mqtt broker lost, notifying clients", cause);
        for (IMqttMessageHandler handler: handlers) {
            handler.connectionLost(cause);
        }
        close();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        for (IMqttMessageHandler handler: handlers) {
            handler.handleMessage(topic, message);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {}

    public void close() {
        if (mqttClient == null) {
            log.warn("Cannot close mqtt connection since it's null");
            return;
        }
        try {
            log.info("Closing MqttConnector resources");
            //Paho doesn't close the connection threads unless we first disconnect and then force-close it.
            mqttClient.disconnectForcibly(5000L);
            mqttClient.close(true);
            mqttClient = null;
        }
        catch (Exception e) {
            log.error("Failed to close MQTT client connection", e);
        }
    }
}
