## mqtt-pulsar-gateway [![Test and create Docker image](https://github.com/HSLdevcom/mqtt-pulsar-gateway/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/mqtt-pulsar-gateway/actions/workflows/test-and-build.yml)

Application for reading data from MQTT topic and feeding it into Pulsar topic. This application doesn't care about the message payload, it just transfers the bytes.

This project is part of [transitdata pipeline](https://github.com/HSLdevcom/transitdata), but it can also be used in other applications.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- `mvn compile`
- `mvn package`

### Docker image

- Run [this script](build-image.sh) to build the Docker image

## Running

### Dependencies

* Pulsar
  * You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container
* Connection to an MQTT broker

### Environment variables

#### MQTT

* `MQTT_CREDENTIALS_REQUIRED`: whether the broker needs credentials
* `FILEPATH_USERNAME_SECRET`: path to the file containing the username, default is `/run/secrets/mqtt_broker_username`
* `FILEPATH_PASSWORD_SECRET`: path to the file containing the password, default is `/run/secrets/mqtt_broker_password`
* `MQTT_BROKER_HOST`: MQTT broker URL
* `MQTT_TOPIC`: MQTT topic to subscribe
* `MQTT_QOS`: MQTT QoS
* `MQTT_MAX_INFLIGHT`: maximum MQTT messages inflight
* `MQTT_CLEAN_SESSION`: whether to open a clean MQTT session, i.e. if true, unacked messages are not redelivered
* `MQTT_CLIENT_ID`: client ID to use when subscribing to the MQTT topic. Remember to use unique client ID if there is more than one subscription
* `MQTT_ADD_RANDOM_TO_CLIENT_ID`: whether to append random string to the client ID
* `MQTT_KEEP_ALIVE_INTERVAL`: interval in seconds for MQTT keep-alive 
* `MQTT_MANUAL_ACK`: whether to ack messages to the MQTT broker only after they have been successfully sent to Pulsar. Setting this to true might cause performance problems with high message rate

#### Other

* `IN_FLIGHT_ALERT_THRESHOLD`: send log message if the inflight message count is higher than this
* `MAX_MESSAGES_PER_SECOND`: maximum amount messages per second to be sent to Pulsar. If negative, no limit
* `MSG_MONITORING_INTERVAL`: send log message after this amount of messages has been processed
* `UNHEALTHY_MSG_SEND_INTERVAL_SECS`: consider the service unhealthy if no message has been sent to Pulsar in this period. If negative, no healthcheck for last sent message
