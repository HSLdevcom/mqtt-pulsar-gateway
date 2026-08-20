# mqtt-pulsar-gateway

Subscribe to an MQTT broker, wrap each message in a Protobuf `RawMessage` envelope and publish it to Apache Pulsar.
The gateway does not inspect the payload; it transfers bytes.
MQTT messages are acknowledged only after the corresponding Pulsar send succeeds.
If a Pulsar send fails, the service shuts down immediately to avoid silently dropping messages.

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) for the MQTT Protobuf schema definition, consumed as a Maven dependency from GitHub Packages.

## Development

Authentication to GitHub Packages is required for fetching dependencies.
Configure a [personal access token](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry) with `read:packages` scope in `.mvn/settings.xml`.

Build and run all tests (unit and integration):

```sh
./mvnw verify
```

Integration tests use [Testcontainers](https://testcontainers.com/) to spin up MQTT and Pulsar brokers, so a running Docker daemon is required.

Run only unit tests (no Docker needed):

```sh
./mvnw test
```

## Docker

The multi-stage Dockerfile builds, tests and packages the application.
It requires a GitHub token as a Docker build secret for accessing GitHub Packages:

```sh
docker build --secret id=github_token,src=<path-to-token-file> -t hsldevcom/mqtt-pulsar-gateway .
```

## Configuration

All configuration is via environment variables.
Spring Boot binds them to the properties below.

The application exposes Spring Boot Actuator endpoints on the default port (8080):
`/actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness` and `/actuator/info`.
Configure the Kubernetes liveness probe to hit `/actuator/health/liveness` and the readiness probe to hit `/actuator/health/readiness`.

The application emits structured logs in Logstash JSON format to stdout.

| Environment variable              | Required | Default | Description                                                                                                                                                                                                                                                                       |
| --------------------------------- | -------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DEBUG_ENABLED`                   | No       | `false` | Enable JDWP remote debug on port 5005.                                                                                                                                                                                                                                            |
| `MQTT_BROKER_HOST`                | Yes      |         | MQTT broker URI, e.g. `tcp://broker:1883` or `ssl://broker:8883`.                                                                                                                                                                                                                 |
| `MQTT_BROKER_PASSWORD`            | No       |         | MQTT password. Pair with `MQTT_BROKER_USERNAME` or leave both unset. The gateway connects without credentials when blank.                                                                                                                                                         |
| `MQTT_BROKER_USERNAME`            | No       |         | MQTT username. Pair with `MQTT_BROKER_PASSWORD` or leave both unset. The gateway connects without credentials when blank.                                                                                                                                                         |
| `MQTT_CLEAN_SESSION`              | No       | `true`  | Whether to start a clean MQTT session. When multiple replicas run and downstream deduplication relies on a limited cache, `true` prevents a reconnecting replica from draining stale messages that would fall outside the deduplication window. Other replicas cover for the gap. |
| `MQTT_CLIENT_ID`                  | Yes      |         | MQTT client ID. Must be unique per broker connection.                                                                                                                                                                                                                             |
| `MQTT_CONNECTION_TIMEOUT_SECONDS` | No       | `10`    | MQTT connection timeout in seconds.                                                                                                                                                                                                                                               |
| `MQTT_KEEP_ALIVE_INTERVAL`        | No       | `30`    | MQTT keep-alive interval in seconds.                                                                                                                                                                                                                                              |
| `MQTT_MAX_INFLIGHT`               | No       | `10000` | Maximum number of in-flight MQTT messages.                                                                                                                                                                                                                                        |
| `MQTT_QOS`                        | No       | `2`     | MQTT QoS level (0, 1 or 2). The broker delivers at min(publisher QoS, subscriber QoS), so `2` preserves the guarantees chosen by each publisher.                                                                                                                                  |
| `MQTT_TOPIC`                      | Yes      |         | MQTT topic filter to subscribe to.                                                                                                                                                                                                                                                |
| `PULSAR_HOST`                     | Yes      |         | Pulsar broker host name.                                                                                                                                                                                                                                                          |
| `PULSAR_PORT`                     | No       | `6650`  | Pulsar broker port.                                                                                                                                                                                                                                                               |
| `PULSAR_PRODUCER_QUEUE_SIZE`      | No       | `10000` | Maximum number of pending Pulsar messages. Blocks when full.                                                                                                                                                                                                                      |
| `PULSAR_PRODUCER_TOPIC`           | Yes      |         | Pulsar topic to publish to.                                                                                                                                                                                                                                                       |
| `PULSAR_SEND_TIMEOUT_SECONDS`     | No       | `20`    | Pulsar send timeout in seconds.                                                                                                                                                                                                                                                   |
