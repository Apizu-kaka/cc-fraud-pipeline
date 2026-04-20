FROM flink:1.18.1

# Install Python and PyFlink
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    python3 -m pip install \
        apache-flink==1.18.1 \
        kafka-python-ng==2.2.2 \
        psycopg2-binary==2.9.9 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Download Kafka SQL connector JAR
RUN mkdir -p /opt/flink/lib && \
    curl -L -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Download JDBC connector JAR
RUN curl -L -o /opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar && \
    curl -L -o /opt/flink/lib/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

WORKDIR /opt/flink/jobs
