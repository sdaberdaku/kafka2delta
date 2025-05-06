# kafka2delta
This Python package provides the means to stream Change Data Capture data from Kafka (i.e. generated with Debezium) 
into one or more Delta Tables using Spark Structured Streaming.

The package allows fully replicating Relational Database (e.g. Postgres, MySQL) tables by merging row-level change 
events streamed from Kafka topics into Delta Tables. Kafka messages are expected to be compressed following the
Confluent Avro format. Moreover, the Confluent Schema Registry is required for decoding such messages.

## Kafka Message Format (Source)
Kafka messages have a **key** and a **value**, both of which are serialized in Confluent Avro.

**key** contains the Avro-encoded primary key of the given record.
**value** contains the Avro-encoded content of the whole record.

An Avro-encoded message is composed as follows:
- **Magic Byte (1 byte)** → Always `0x00`, indicating Confluent Avro encoding.
- **Schema ID (4 bytes, Big-Endian)** → Identifies the Avro schema in Confluent Schema Registry.
- **Avro Payload (Binary Encoded)** → Actual data serialized in Avro format.

## Setting up local development environment
Create Conda environment and install the requirements with the following code:

```shell
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
source ~/miniconda3/bin/activate

conda create --name kafka2delta python=3.10 -y
conda activate kafka2delta
pip install -r requirements-dev.txt
conda install -c conda-forge openjdk=11   # Install Java Development Kit (JDK)
```

## Installation
The package can be installed with the following command:
```shell
pip install git+ssh://git@github.com/sdaberdaku/kafka2delta.git#egg=kafka2delta
```

Locally, the package can be installed with the following command:
```shell
pip install -e .
```

Build distribution package with the following command:
```shell
python -m build .
```

## Preparing testing environment
To test the `kafka2delta` package, a Kubernetes cluster can be created with `kind` so that the necessary components can 
be installed.

The following documentation will describe how to install a Postgres database and create a Change Data Capture (CDC) 
pipeline with Kafka and Debezium. The `kafka2delta` package can then be used to stream the CDC data from the related 
Kafka Topics into the corresponding Delta Tables.

First of all, install `kubectl`:
```shell
# 1. Download the desired kubectl version
curl -LO https://dl.k8s.io/release/v1.31.0/bin/linux/amd64/kubectl
# 2. Install kubectl
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
```

Now we can install `helm` and `kind`. A convenient way of doing so is by using `brew`:
```shell
brew install helm kind
```
install brew
```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> ~/.profile
eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
```


Now we are ready to create our `kind` cluster. Use the following commands to create the K8s cluster for testing:
```shell
kind create cluster --name kafka2delta
kubectl config use-context kind-kafka2delta
```

As a first step, we should install Telepresence/Traffic Manager. This tool allows us to expose K8s cluster DNS names to
the host machine, simplifying connectivity and allowing us to avoid using port-forwarding.

Install telepresence client with the following commands:
```shell
# 1. Download the latest binary (~95 MB):
sudo curl -fL https://app.getambassador.io/download/tel2oss/releases/download/v2.17.0/telepresence-linux-amd64 -o /usr/local/bin/telepresence
# 2. Make the binary executable:
sudo chmod a+x /usr/local/bin/telepresence
```

Now we can install the Traffic Manager on the `kind` cluster.
```shell
helm repo add datawire https://app.getambassador.io
helm install traffic-manager datawire/telepresence \
  --version 2.17.0 \
  --namespace ambassador \
  --create-namespace \
  --wait \
  --set ambassador-agent.enabled=false
```

We can now connect to the Kind cluster using telepresence:
```shell
telepresence connect --context kind-kafka2delta
```

Once the cluster is up and running, we can install the Postgres database that we will use for our tests:
```shell
# Install Postgres
export PG_CONFIG=$(cat <<EOF
listen_addresses = '*'
wal_level = 'logical'
EOF
)
helm install postgres oci://registry-1.docker.io/bitnamicharts/postgresql \
  --version 15.5.20 \
  --namespace postgres \
  --create-namespace \
  --wait \
  --set auth.enablePostgresUser=true \
  --set auth.postgresPassword=postgres \
  --set primary.networkPolicy.enabled=false \
  --set primary.persistence.enabled=false \
  --set primary.configuration="${PG_CONFIG}"
```

After the installation is complete, we can create the tables that we will use for testing the CDC replication.
`psql` can be installed with the following command:
```shell
sudo apt update && sudo apt install -y postgresql-client
```

```shell
# Create the required tables
psql "postgresql://postgres:postgres@postgres-postgresql.postgres.svc.cluster.local:5432/postgres" -c "
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at DATE NOT NULL
);
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2) NOT NULL,
    created_at DATE NOT NULL    
);
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    created_at DATE NOT NULL    
);
ALTER TABLE public.users REPLICA IDENTITY FULL;
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.products REPLICA IDENTITY FULL;
"
```

Now we can proceed by installing the `Strimzi Operator` that will be used to install and manage Kafka, Debezium, and the
related resources.
```shell
# Install the Strimzi Kafka Operator
helm install strimzi-kafka-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version 0.42.0\
  --namespace strimzi \
  --create-namespace \
  --wait \
  --set replicas=1 \
  --set watchAnyNamespace=true \
  --set generateNetworkPolicy=false
```

Create the `cdc` K8s namespace:
```shell
kubectl create namespace cdc
```

Now we can start setting up Kafka and the related resources:
```shell
# Create Kafka Cluster and Kafka Nodepool CRs
kubectl apply -f ./resources/kafka-cluster.yaml
```
Once the Kafka cluster and the Entity operator are up, we can create the topics.
```shell
# Create Kafka topics (one for each table to replicate)
kubectl apply -f ./resources/kafka-topics.yaml
```

At this point we can install the Confluent Schema Registry:
```shell
helm install schema-registry oci://registry-1.docker.io/bitnamicharts/schema-registry \
  --version 20.0.0 \
  --namespace cdc \
  --create-namespace \
  --timeout 15m \
  --wait \
  --set replicaCount=1 \
  --set avroCompatibilityLevel=none \
  --set networkPolicy.enabled=false \
  --set kafka.enabled=false \
  --set "externalKafka.brokers[0]=PLAINTEXT://cdc-kafka-bootstrap:9092"
```

(Optional) Install the Kafka UI to inspect the created topics and related messages:
```shell
helm repo add kafka-ui https://provectus.github.io/kafka-ui-charts
helm install kafka-ui kafka-ui/kafka-ui \
  --version 0.7.6 \
  --namespace cdc \
  --create-namespace \
  --wait \
  --set replicaCount=1 \
  --set yamlApplicationConfig.kafka.internalTopicPrefix=__ \
  --set "yamlApplicationConfig.kafka.clusters[0].name=cdc" \
  --set "yamlApplicationConfig.kafka.clusters[0].bootstrapServers=cdc-kafka-brokers:9092" \
  --set yamlApplicationConfig.auth.type=disabled \
  --set yamlApplicationConfig.management.health.ldap.enabled=false
```

Before installing the Debezium Connector, we need to build the Kafka Connect Docker image, and make it available to the
Kind cluster. The following command can be used to do so:
```shell
# Build Kafka Connect Docker image with the required dependencies for running Debezium
docker build -t strimzi-debezium-postgres:2.7.0.Final -f ./resources/kafka-connect.Dockerfile .

# Load the image to the Kind cluster
kind load docker-image strimzi-debezium-postgres:2.7.0.Final --name kafka2delta
```

Now we can create the Kafka Connect and Debezium connector configuration:
```shell
# Create Kafka Connect cluster
kubectl apply -f ./resources/kafka-connect.yaml
# Configure Debezium connector
kubectl apply -f ./resources/kafka-connector.yaml
```

Setting up LocalStack

```shell
helm repo add localstack-charts https://localstack.github.io/helm-charts
helm install localstack localstack-charts/localstack \
  --version 0.6.22 \
  --namespace localstack \
  --create-namespace \
  --wait \
  --set replicaCount=1 \
  --set role.create=false \
  --set persistence.enabled=false \
  --set startServices=s3 \
  --set service.type=ClusterIP
```

## Running tests
Test can be run with the following command:
```shell
python -m pytest .
```

The Spark UI can be viewed at [http://localhost:4040](http://localhost:4040).

## Clean up
To clean up the created resources delete the kind cluster:
```shell
kubectl delete -f ./resources/kafka-topics.yaml
telepresence quit
kind delete cluster --name kafka2delta
```