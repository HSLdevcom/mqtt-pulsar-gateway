# syntax=docker/dockerfile:1
# check=error=true

# ============================
# Build stage
# ============================
FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jdk AS build
WORKDIR /usr/app

ARG GITHUB_ACTOR=github-actions

COPY mvnw pom.xml ./
COPY .mvn .mvn

COPY .mvn/settings.xml /root/.m2/settings.xml

COPY src src

RUN --mount=type=secret,id=github_token \
    export GITHUB_TOKEN="$(cat /run/secrets/github_token)" && \
    export GITHUB_ACTOR="$GITHUB_ACTOR" && \
    ./mvnw -B package -DskipTests

# ============================
# Runtime stage
# ============================
FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jre
COPY --from=build /usr/app/target/mqtt-pulsar-gateway.jar app.jar
COPY entrypoint.sh .
