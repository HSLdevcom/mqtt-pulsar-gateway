#This is a two-stage build image for a java project.
#The actual build step happens in the maven container, and the final jar is
#deployed to a more lightweight container. Maven should not download all the
#dependencies if pom.xml is not modified. Modifying only the source code should
#trigger only the compile step

#The build container
FROM maven:3.5.3-jdk-8-slim as BUILD

RUN mkdir -p /usr/src/app

#Copy pom.xml file and download dependencies. This stage will be cached if the pom.xml file is not changed
COPY pom.xml /usr/src/app

RUN mvn -f /usr/src/app/pom.xml dependency:resolve-plugins dependency:resolve clean package

#Run tests
RUN mvn -f /usr/src/app/pom.xml test

#Build the project
COPY src /usr/src/app/src
RUN mvn -f /usr/src/app/pom.xml clean package

#The container that actually runs our application.
#TODO: switch to Alpine when it becomes available
FROM openjdk:8-jre-slim

#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl

#This container can access the build artifacts inside the BUILD container.
#Everything that is not copied is discarded
COPY --from=BUILD /usr/src/app/target/mqtt-pulsar-gateway-jar-with-dependencies.jar /usr/app/mqtt-pulsar-gateway.jar

ENTRYPOINT ["java", "-jar", "/usr/app/mqtt-pulsar-gateway.jar"]
