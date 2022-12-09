FROM openjdk:8-jre

ENV FLINK_FETCHER_HOME=/opt/flink-fetcher-libs
ENV PATH=$FLINK_FETCHER_HOME/bin:$PATH

COPY docker-entrypoint.sh /
COPY flink-kubernetes-fetcher-1.15.tar ${FLINK_FETCHER_HOME}/
RUN set -ex; \
  \
  tar -xf ${FLINK_FETCHER_HOME}/flink-kubernetes-fetcher-1.15.tar --strip-components=1 -C $FLINK_FETCHER_HOME; \
  rm ${FLINK_FETCHER_HOME}/flink-kubernetes-fetcher-1.15.tar; \
  chmod -R 755 ${FLINK_FETCHER_HOME}; \
  chmod 755 /docker-entrypoint.sh;

ENTRYPOINT ["/docker-entrypoint.sh"]
