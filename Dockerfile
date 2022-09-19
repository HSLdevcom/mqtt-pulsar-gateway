FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk add --no-cache curl

ADD target/mqtt-pulsar-gateway-jar-with-dependencies.jar /usr/app/mqtt-pulsar-gateway.jar

COPY start-application.sh /
RUN chmod +x /start-application.sh

CMD ["/start-application.sh"]