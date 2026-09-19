FROM flink:2.1.3-scala_2.12

USER root

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev wget && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir apache-flink==2.1.3

RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

USER flink
