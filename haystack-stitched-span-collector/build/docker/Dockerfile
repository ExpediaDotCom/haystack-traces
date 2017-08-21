FROM openjdk:8-jre
MAINTAINER Haystack <haystack@expedia.com>

ENV APP_NAME haystack-stitched-span-collector
ENV APP_HOME /app/bin

RUN mkdir -p ${APP_HOME}

COPY target/${APP_NAME}.jar ${APP_HOME}/
COPY build/docker/start-app.sh ${APP_HOME}/

RUN mkdir -p /schemas/cassandra /schemas/elasticsearch
COPY schemas/cassandra/docker.cql /schemas/cassandra/.
COPY schemas/elasticsearch/template.json /schemas/elasticsearch/.

RUN chmod +x ${APP_HOME}/start-app.sh

WORKDIR ${APP_HOME}

EXPOSE 5005

ENTRYPOINT ["./start-app.sh"]
