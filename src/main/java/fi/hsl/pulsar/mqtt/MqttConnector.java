package fi.hsl.pulsar.mqtt;

import com.typesafe.config.Config;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public class MqttConnector implements MqttCallback {
    private static final Logger log = LoggerFactory.getLogger(MqttConnector.class);

    private MqttClient mqttClient;
    private String mqttTopic;
    private int qos;

    private final LinkedList<IMqttMessageHandler> handlers = new LinkedList<>();

    MqttConnector(MqttClient client, String topic, int QoS) {
        this.mqttClient = client;
        this.mqttTopic = topic;
        this.qos = QoS;
    }

    public static MqttConnector newInstance(Config config, String username, String password) throws Exception {
        MqttClient mqttClient = null;
        MqttConnector connector = null;
        try {
            final String clientId = config.getString("mqtt-broker.clientId");
            final String broker = config.getString("mqtt-broker.host");
            final int maxInFlight = config.getInt("mqtt-broker.maxInflight");
            final String topic = config.getString("mqtt-broker.topic");
            final int QoS = config.getInt("mqtt-broker.qos");

            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(false); //WHY FALSE? WHY NOT TRUE?
            connectOptions.setMaxInflight(maxInFlight);
            connectOptions.setAutomaticReconnect(false); //Let's abort on connection errors

            connectOptions.setUserName(username);
            connectOptions.setPassword(password.toCharArray());
            connectOptions.setConnectionTimeout(10);

            //Let's use memory persistance to optimize throughput.
            MemoryPersistence memoryPersistence = new MemoryPersistence();

            mqttClient = new MqttClient(broker, clientId, memoryPersistence);

            connector = new MqttConnector(mqttClient, topic, QoS);
            mqttClient.setCallback(connector); //Let's add the callback before connecting so we won't lose any messages

            log.info(String.format("Connecting to mqtt broker %s", broker));
            IMqttToken token = mqttClient.connectWithResult(connectOptions);

            log.info("Connection to MQTT completed? {}", token.isComplete());
            if (token.getException() != null) {
                throw token.getException();
            }
        }
        catch (Exception e) {
            log.error("Error connecting to MQTT", e);
            if (mqttClient != null) {
                //Paho doesn't close the connection threads unless we force-close it.
                mqttClient.close(true);
            }
            throw e;
        }
        return connector;
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
        //Paho never calls this MqttCallback-method which is weird.
        //Message events are received after subscribing to the client
        //log.info("HERE: " + topic);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {}

    public void subscribe(final IMqttMessageHandler handler) throws Exception {

        mqttClient.subscribe(mqttTopic, qos, (String topic, MqttMessage message) -> {
            handler.handleMessage(topic, message);
        });
        handlers.add(handler);
    }

    public void close() {
        try {
            log.info("Closing MqttConnector resources");
            //Paho doesn't close the connection threads unless we first disconnect and then force-close it.
            mqttClient.disconnectForcibly(5000L);
            mqttClient.close(true);
        }
        catch (Exception e) {
            log.error("Failed to close MQTT client connection", e);
        }
    }
}
