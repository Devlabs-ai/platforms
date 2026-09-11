# Spark runtime with Hadoop-AWS on the classpath so Python jobs can read
# s3a:// (MinIO) without Ivy `spark.jars.packages` on every cold start.
#
# Versions match Spark 3.5.3 (Hadoop 3.3.4) and the former packages line
# in spark-platform/api/main.py.
FROM debian:bookworm-slim AS jars

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /jars

# SHA1 from Maven Central (hadoop-aws 3.3.4 / aws-java-sdk-bundle 1.12.262).
ARG HADOOP_AWS_SHA1=a65839fbf1869f81a1632e09f415e586922e4f80
ARG AWS_SDK_SHA1=02deec3a0ad83d13d032b1812421b23d7a961eea

RUN curl -fsSL -o hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && echo "${HADOOP_AWS_SHA1}  hadoop-aws-3.3.4.jar" | sha1sum -c - \
 && curl -fsSL -o aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && echo "${AWS_SDK_SHA1}  aws-java-sdk-bundle-1.12.262.jar" | sha1sum -c -

FROM apache/spark:3.5.3

USER root
COPY --from=jars /jars/*.jar /opt/spark/jars/
# Official apache/spark image runs as uid 185.
USER 185
