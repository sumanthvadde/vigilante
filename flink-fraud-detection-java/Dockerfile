ARG MAVEN_VERSION=3-eclipse-temurin-11
ARG FLINK_TAG=1.17-java11

FROM maven:${MAVEN_VERSION} AS build
WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN mvn -f pom.xml clean package


FROM flink:${FLINK_TAG} as runtime

COPY --from=build /app/target/frauddetection-0.1.jar /opt/flink/usrlib/frauddetection.jar