#!/bin/bash

set -Eeuo pipefail

JAVA_OPTS=(
  -XX:InitialRAMPercentage=10.0
  -XX:MaxRAMPercentage=95.0
)

if [[ "${DEBUG_ENABLED:-false}" == "true" ]]; then
  JAVA_OPTS+=(
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
  )
fi

exec java "${JAVA_OPTS[@]}" -jar app.jar
