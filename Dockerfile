# ============================
# Test stage
# ============================
FROM hsldevcom/infodevops-docker-base-images:1.0.1-25-java AS test

WORKDIR /usr/app

COPY mvnw pom.xml ./
COPY .mvn .mvn

RUN ./mvnw -B -q dependency:go-offline

COPY src src

RUN ./mvnw -B test

# ============================
# Build stage
# ============================
FROM hsldevcom/infodevops-docker-base-images:1.0.1-25-java AS build

WORKDIR /usr/app

COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY src src

RUN ./mvnw -B package -DskipTests

# ============================
# Runtime stage
# ============================
FROM hsldevcom/infodevops-docker-base-images:1.0.1-25-java

WORKDIR /usr/app

COPY --from=build /usr/app/target/mqtt-pulsar-gateway-jar-with-dependencies.jar mqtt-pulsar-gateway.jar

COPY start-application.sh /
RUN chmod +x /start-application.sh

CMD ["/start-application.sh"]