# syntax=docker/dockerfile:1.6

# ============================
# Test stage
# ============================
FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jdk AS test

WORKDIR /usr/app

COPY mvnw pom.xml ./
COPY .mvn .mvn

COPY .mvn/settings.xml /root/.m2/settings.xml

RUN --mount=type=secret,id=github_token \
    export GITHUB_TOKEN="$(cat /run/secrets/github_token)" && \
    ./mvnw -B -q dependency:go-offline

COPY src src

RUN --mount=type=secret,id=github_token \
    export GITHUB_TOKEN="$(cat /run/secrets/github_token)" && \
    ./mvnw -B test

# ============================
# Build stage
# ============================
FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jdk AS build

WORKDIR /usr/app

COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY .mvn/settings.xml /root/.m2/settings.xml
COPY src src

RUN --mount=type=secret,id=github_token \
    export GITHUB_TOKEN="$(cat /run/secrets/github_token)" && \
    ./mvnw -B package -DskipTests

# ============================
# Runtime stage
# ============================
FROM hsldevcom/infodevops-docker-base-images:1.0.2-25-java-jre

WORKDIR /usr/app

COPY --from=build /usr/app/target/mqtt-pulsar-gateway-jar-with-dependencies.jar mqtt-pulsar-gateway.jar

COPY start-application.sh /
RUN chmod +x /start-application.sh

CMD ["/start-application.sh"]