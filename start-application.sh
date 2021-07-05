#!/bin/bash

if [[ "${DEBUG_ENABLED}" = true ]]; then
  java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -jar /usr/app/mqtt-pulsar-gateway.jar
else
  java -jar /usr/app/mqtt-pulsar-gateway.jar
fi