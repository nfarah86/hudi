# Docker Demo

## A Demo using docker containers​
Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer.
The steps have been tested on a Mac laptop

### Prerequisites​
#### Docker Setup: 
- For Mac, Please follow the steps as defined in [https://docs.docker.com/v17.12/docker-for-mac/install/]. 
- For running Spark-SQL queries, please ensure at least 6 GB and 4 CPUs are allocated to Docker (See Docker -> Preferences -> Advanced). Otherwise, spark-SQL queries could be killed because of memory issues.
- kcat : A command-line utility to publish/consume from kafka topics. Use this command to install kcat:
```bash 
brew install kcat
``` 
- /etc/hosts: The demo references many services running in container by the hostname. Add the following settings to /etc/hosts:
``` bash
127.0.0.1 adhoc-1
127.0.0.1 adhoc-2
127.0.0.1 namenode
127.0.0.1 datanode1
127.0.0.1 hiveserver
127.0.0.1 hivemetastore
127.0.0.1 kafkabroker
127.0.0.1 sparkmaster
127.0.0.1 zookeeper
```

- Copy
- Java: Java SE Development Kit 8.
- Maven: A build automation tool for Java projects.
- jq: A lightweight and flexible command-line JSON processor. Use this command to install jq: 
``` bash 
brew install jq
```
#### Note: 
This has not been tested on some environments like Docker on Windows.

### Setting up Docker Cluster​
##### Build Hudi​
The first step is to build hudi. This step builds hudi on default supported scala version - 2.11.
``` bash 
cd <HUDI_WORKSPACE>

mvn clean package -Pintegration-tests -DskipTests
```
### Bringing up Demo Cluster​

The next step is to run the Docker compose script and setup configs for bringing up the cluster. These files are in the Hudi repository which you should already have locally on your machine from the previous steps. 
This should pull the Docker images from Docker hub and set up the Docker cluster.
- Default
- Mac AArch64

``` bash
cd docker
./setup_demo.sh
....
....
....
[+] Running 10/13
⠿ Container zookeeper             Removed                 8.6s
⠿ Container datanode1             Removed                18.3s
⠿ Container trino-worker-1        Removed                50.7s
⠿ Container spark-worker-1        Removed                16.7s
⠿ Container adhoc-2               Removed                16.9s
⠿ Container graphite              Removed                16.9s
⠿ Container kafkabroker           Removed                14.1s
⠿ Container adhoc-1               Removed                14.1s
⠿ Container presto-worker-1       Removed                11.9s
⠿ Container presto-coordinator-1  Removed                34.6s
.......
......

[+] Running 17/17
⠿ adhoc-1 Pulled                                          2.9s
⠿ graphite Pulled                                         2.8s
⠿ spark-worker-1 Pulled                                   3.0s
⠿ kafka Pulled                                            2.9s
⠿ datanode1 Pulled                                        2.9s
⠿ hivemetastore Pulled                                    2.9s
⠿ hiveserver Pulled                                       3.0s
⠿ hive-metastore-postgresql Pulled                        2.8s
⠿ presto-coordinator-1 Pulled                             2.9s
⠿ namenode Pulled                                         2.9s
⠿ trino-worker-1 Pulled                                   2.9s
⠿ sparkmaster Pulled                                      2.9s
⠿ presto-worker-1 Pulled                                  2.9s
⠿ zookeeper Pulled                                        2.8s
⠿ adhoc-2 Pulled                                          2.9s
⠿ historyserver Pulled                                    2.9s
⠿ trino-coordinator-1 Pulled                              2.9s
[+] Running 17/17
⠿ Container zookeeper                  Started           41.0s
⠿ Container kafkabroker                Started           41.7s
⠿ Container graphite                   Started           41.5s
⠿ Container hive-metastore-postgresql  Running            0.0s
⠿ Container namenode                   Running            0.0s
⠿ Container hivemetastore              Running            0.0s
⠿ Container trino-coordinator-1        Runni...           0.0s
⠿ Container presto-coordinator-1       Star...           42.1s
⠿ Container historyserver              Started           41.0s
⠿ Container datanode1                  Started           49.9s
⠿ Container hiveserver                 Running            0.0s
⠿ Container trino-worker-1             Started           42.1s
⠿ Container sparkmaster                Started           41.9s
⠿ Container spark-worker-1             Started           50.2s
⠿ Container adhoc-2                    Started           38.5s
⠿ Container adhoc-1                    Started           38.5s
⠿ Container presto-worker-1            Started           38.4s
Copying spark default config and setting up configs
Copying spark default config and setting up configs
$ docker ps

```

At this point, the Docker cluster will be up and running. The demo cluster brings up the following services
- HDFS Services (NameNode, DataNode)
- Spark Master and Worker
- Hive Services (Metastore, HiveServer2 along with PostgresDB)
- Kafka Broker and a Zookeeper Node (Kafka will be used as upstream source for the demo)
- Containers for Presto setup (Presto coordinator and worker)
- Containers for Trino setup (Trino coordinator and worker)
- Adhoc containers to run Hudi/Hive CLI commands


**Step 1**: Publish the first batch to Kafka:
```bash 
cat mock_data_batch1.json | kcat -b kafkabroker -t user_events -P
```
To check if the new topic shows up, use:
```bash
kcat -b kafkabroker -L -J | jq .
```

**Step 2**: Streaming ingest data from Kafka topic and sync to Hive 
Hop into the adhoc host:
```bash
docker exec -it adhoc-2 /bin/bash
```
Submit the DeltaStreamer-based streaming ingestion command. This keeps running continuously.

```bash
spark-submit \
 --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE \
 --table-type COPY_ON_WRITE \
 --op BULK_INSERT \
 --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
 --target-base-path /user/hive/warehouse/user_events_cow \
 --target-table user_events_cow --props /var/demo/config/kafka-source.properties \
 --continuous \
 --min-sync-interval-seconds 60 \
 --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
 --enable-sync \
 --hoodie-conf hoodie.datasource.hive_sync.jdbcurl=jdbc:hive2://hiveserver:10000 \
 --hoodie-conf hoodie.datasource.hive_sync.username=hive \
 --hoodie-conf hoodie.datasource.hive_sync.password=hive \
 --hoodie-conf hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor \
  --hoodie-conf hoodie.datasource.hive_sync.database=default \
  --hoodie-conf hoodie.datasource.hive_sync.table=user_events_cow \
  --hoodie-conf hoodie.datasource.hive_sync.partition_fields=event_time_date
```

Explore tables from the hdfs web browser. You can use HDFS web-browser to look at the tables:

`http://namenode:50070/explorer.html#/user/hive/warehouse/user_events_cow.`


**Step 3**: Querying data using Hive
Hop into adhoc host
```bash
docker exec -it adhoc-2 /bin/bash
```
Start beeline to query hive:
```bash
beeline -u jdbc:hive2://hiveserver:10000 \
 --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
 --hiveconf hive.stats.autogather=false

# List Tables
0: jdbc:hive2://hiveserver:10000> show tables;

# Look at partitions that were added
0: jdbc:hive2://hiveserver:10000> show partitions user_events_cow;

# Query the table to get all purchased events after certain date
0: jdbc:hive2://hiveserver:10000> select  count(*) from user_events_cow where event_time_date > '2022-11-10' and action_type='purchased';

#  Query the table to get count of users who has a non empty cart  in last one week
0: jdbc:hive2://hiveserver:10000> select count(*) from user_events_cow where event_time_date > '2022-11-10' and has_shopping_cart = true;
```

**Step 4**: Querying data Spark SQL
Hop into adhoc host:
```bash
docker exec -it adhoc-1 /bin/bash
```

Start a spark shell to query:
```bash
$SPARK_INSTALL/bin/spark-shell \
 --jars $HUDI_SPARK_BUNDLE \
 --master local[2] \
 --driver-class-path $HADOOP_CONF_DIR \
 --conf spark.sql.hive.convertMetastoreParquet=false \
 --deploy-mode client \
 --driver-memory 1G \
 --executor-memory 3G \
 --num-executors 1

# List Tables
scala> spark.sql("show tables").show(100, false)

# Query the table to get all purchased events after certain date
scala> spark.sql("select  count(*) from user_events_cow where event_time_date > '2022-11-10' and action_type='purchased'").show(100, false)

#  Query the table to get count of users who has a non empty cart  in last one week
scala> spark.sql("select count(*) from user_events_cow where event_time_date > '2022-11-10' and has_shopping_cart = true").show(100, false)

```