FROM hsldevcom/infodevops-docker-base-images:1.0.1-25-java

ADD target/mqtt-pulsar-gateway-jar-with-dependencies.jar /usr/app/mqtt-pulsar-gateway.jar

COPY start-application.sh /
RUN chmod +x /start-application.sh

CMD ["/start-application.sh"]