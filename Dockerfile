FROM hub.bilibili.co/aphrodite/deploy-cxx:mm

# Install dependencies
RUN set -ex; \
  apt-get update; \
  apt-get -y install gpg libsnappy1v5 gettext-base libjemalloc-dev; \
  rm -rf /var/lib/apt/lists/*

# Grab gosu for easy step-down from root
ENV GOSU_VERSION 1.11
RUN set -ex; \
  wget -nv -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
  wget -nv -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
  export GNUPGHOME="$(mktemp -d)"; \
  for server in ha.pool.sks-keyservers.net $(shuf -e \
                          hkp://p80.pool.sks-keyservers.net:80 \
                          keyserver.ubuntu.com \
                          hkp://keyserver.ubuntu.com:80 \
                          pgp.mit.edu) ; do \
      gpg --batch --keyserver "$server" --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 && break || : ; \
  done && \
  gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
  gpgconf --kill all; \
  rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
  chmod +x /usr/local/bin/gosu; \
  gosu nobody true

ENV JAVA_HOME /opt/java
ENV PATH $JAVA_HOME/bin:$PATH

RUN curl http://10.70.82.16:8000/hadoop/jdk1.8.0.tar.gz -o /var/tmp/jdk1.8.0.tar.gz \
    && tar zxf /var/tmp/jdk1.8.0.tar.gz -C /opt/ \
    && ln -sfn /opt/jdk1.8.0 /opt/java

ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH

RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink

WORKDIR $FLINK_HOME

COPY flink-dist/target/flink-1.15.1-SNAPSHOT-bin/flink-1.15.tar .

RUN set -ex; \
  tar -xf flink-1.15.tar --strip-components=1; \
  rm flink-1.15.tar; \
  chown -R flink:flink .;

ENV HADOOP_HOME=/opt/hadoop-2.8.4

RUN set -ex; \
  wget -P $HADOOP_HOME/ http://bazel-cabin.bilibili.co/bilibili/datacenter/online/flink/hadoop-2.8.4.tar.gz; \
  tar -xf $HADOOP_HOME/hadoop-2.8.4.tar.gz --strip-components=1 -C $HADOOP_HOME; \
  rm $HADOOP_HOME/hadoop-2.8.4.tar.gz; \
  rm -rf $HADOOP_HOME/etc; \
  wget http://bazel-cabin.bilibili.co/bilibili/datacenter/online/flink/flink-logagent-app.conf; \
  chown -R flink:flink $HADOOP_HOME;

ENV PATH=$HADOOP_HOME/bin:$PATH
ENV HADOOP_CLASSPATH=/opt/hadoop/conf:/opt/hadoop-2.8.4/share/hadoop/common/lib/*:/opt/hadoop-2.8.4/share/hadoop/common/*:/opt/hadoop-2.8.4/share/hadoop/hdfs:/opt/hadoop-2.8.4/share/hadoop/hdfs/lib/*:/opt/hadoop-2.8.4/share/hadoop/hdfs/*:/opt/hadoop-2.8.4/share/hadoop/yarn/lib/*:/opt/hadoop-2.8.4/share/hadoop/yarn/*:/opt/hadoop-2.8.4/share/hadoop/mapreduce/lib/*:/opt/hadoop-2.8.4/share/hadoop/mapreduce/*

RUN set -ex; \
  wget -P /opt/ http://bazel-cabin.bilibili.co/flink_k8s_entrypoint/docker-entrypoint.sh; \
  mv /opt/docker-entrypoint.sh /docker-entrypoint.sh; \
  chmod 777 /docker-entrypoint.sh;

ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081
