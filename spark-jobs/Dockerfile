FROM maven:3.9.11-eclipse-temurin-21-alpine AS deps

WORKDIR /build

COPY pom.xml .

RUN --mount=type=cache,target=/root/.m2 \
    mvn package

FROM apache/spark:4.0.3-java21-python3

WORKDIR /opt/app
USER root

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY --from=deps /build/target/jars/*.jar $SPARK_HOME/jars/

COPY raw_ingestion.py .

WORKDIR /opt/spark/work-dir
USER spark
