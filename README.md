[![Build Status](https://travis-ci.org/HSLdevcom/mqtt-pulsar-gateway.svg?branch=master)](https://travis-ci.org/HSLdevcom/mqtt-pulsar-gateway)

## Description

Application for reading data from MQTT topic and feeding it into Pulsar topic. 
This application doesn't care about the payload, it just transfers the bytes.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

Either use released versions from public maven repository or build your own and install to local maven repository:
  - ```cd transitdata-common && mvn install```  

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container
- Connection to an external MQTT server.
  - Configure username and password via files
    - Set filepath for username via env variable FILEPATH_USERNAME_SECRET, default is `/run/secrets/mqtt_broker_username`
    - Set filepath for password via env variable FILEPATH_PASSWORD_SECRET, default is `/run/secrets/mqtt_broker_password`
  - Mandatory: Set mqtt-topic via env variable MQTT_TOPIC
  - Remember to use a unique MQTT client-id's if you have multiple instances connected to a single broker.

All other configuration options are configured in the [config file](src/main/resources/environment.conf)
which can also be configured externally via env variable CONFIG_PATH

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   
