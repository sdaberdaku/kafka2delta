FROM quay.io/strimzi/maven-builder:0.42.0 AS download_artifacts
RUN 'curl' '-f' '-L' '--create-dirs' '--output' '/tmp/kafka-connect-avro-converter/9e3ada44/pom.xml' 'https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.7.0/kafka-connect-avro-converter-7.7.0.pom' \
      && 'echo' '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"><profiles><profile><id>download</id><repositories><repository><id>custom-repo</id><url>https://packages.confluent.io/maven/</url></repository></repositories></profile></profiles><activeProfiles><activeProfile>download</activeProfile></activeProfiles></settings>' > '/tmp/9e3ada44.xml' \
      && 'mvn' 'dependency:copy-dependencies' '-s' '/tmp/9e3ada44.xml' '-DoutputDirectory=/tmp/artifacts/kafka-connect-avro-converter/9e3ada44' '-f' '/tmp/kafka-connect-avro-converter/9e3ada44/pom.xml' \
      && 'curl' '-f' '-L' '--create-dirs' '--output' '/tmp/artifacts/kafka-connect-avro-converter/9e3ada44/kafka-connect-avro-converter-7.7.0.jar' 'https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.7.0/kafka-connect-avro-converter-7.7.0.jar'

RUN 'curl' '-f' '-L' '--create-dirs' '--output' '/tmp/kafka-connect-avro-converter/f2d3b934/pom.xml' 'https://packages.confluent.io/maven/io/confluent/common-config/7.7.0/common-config-7.7.0.pom' \
      && 'echo' '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"><profiles><profile><id>download</id><repositories><repository><id>custom-repo</id><url>https://packages.confluent.io/maven/</url></repository></repositories></profile></profiles><activeProfiles><activeProfile>download</activeProfile></activeProfiles></settings>' > '/tmp/f2d3b934.xml' \
      && 'mvn' 'dependency:copy-dependencies' '-s' '/tmp/f2d3b934.xml' '-DoutputDirectory=/tmp/artifacts/kafka-connect-avro-converter/f2d3b934' '-f' '/tmp/kafka-connect-avro-converter/f2d3b934/pom.xml' \
      && 'curl' '-f' '-L' '--create-dirs' '--output' '/tmp/artifacts/kafka-connect-avro-converter/f2d3b934/common-config-7.7.0.jar' 'https://packages.confluent.io/maven/io/confluent/common-config/7.7.0/common-config-7.7.0.jar'

FROM quay.io/strimzi/kafka:0.42.0-kafka-3.7.1

USER root:root

##########
# Connector plugin debezium-postgres-connector
##########
RUN 'mkdir' '-p' '/opt/kafka/plugins/debezium-postgres-connector/8cdbed29' \
      && 'curl' '-f' '-L' '--output' '/opt/kafka/plugins/debezium-postgres-connector/8cdbed29.zip' 'https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.0.Final/debezium-connector-postgres-2.7.0.Final-plugin.zip' \
      && 'echo' '1374d56e14db841c9bd70ed0ec63e282a4db3e5667a70c274b7128a1a5ff72b66332206f393b5da426d9e9390999da3aece1a55b0a865d69c1bcabc5c6115b10 /opt/kafka/plugins/debezium-postgres-connector/8cdbed29.zip' > '/opt/kafka/plugins/debezium-postgres-connector/8cdbed29.zip.sha512' \
      && 'sha512sum' '--check' '/opt/kafka/plugins/debezium-postgres-connector/8cdbed29.zip.sha512' \
      && 'rm' '-f' '/opt/kafka/plugins/debezium-postgres-connector/8cdbed29.zip.sha512' \
      && 'unzip' '/opt/kafka/plugins/debezium-postgres-connector/8cdbed29.zip' '-d' '/opt/kafka/plugins/debezium-postgres-connector/8cdbed29' \
      && 'find' '/opt/kafka/plugins/debezium-postgres-connector/8cdbed29' '-type' 'l' | 'xargs' 'rm' '-f' \
      && 'rm' '-vf' '/opt/kafka/plugins/debezium-postgres-connector/8cdbed29.zip'

##########
# Connector plugin kafka-connect-avro-converter
##########
COPY --from=download_artifacts '/tmp/artifacts/kafka-connect-avro-converter/9e3ada44' '/opt/kafka/plugins/kafka-connect-avro-converter/9e3ada44'

COPY --from=download_artifacts '/tmp/artifacts/kafka-connect-avro-converter/f2d3b934' '/opt/kafka/plugins/kafka-connect-avro-converter/f2d3b934'

USER 1001

