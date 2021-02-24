
# Contributing

## Building core

Only the apache repository is required, which is mirrored in the public spliceengine nexus repository located at 
https://repository.splicemachine.com/nexus/#welcome

```sh
mvn clean install -Pcore -DskipTests
```

```maven
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for spliceengine-parent 3.1.0.2001-SNAPSHOT:
[INFO]
[INFO] spliceengine-parent ................................ SUCCESS [  0.984 s]
[INFO] utilities .......................................... SUCCESS [  4.413 s]
[INFO] db-shared .......................................... SUCCESS [  0.231 s]
[INFO] db-tools-testing ................................... SUCCESS [  0.314 s]
[INFO] db-build ........................................... SUCCESS [  0.096 s]
[INFO] db-engine .......................................... SUCCESS [  5.237 s]
[INFO] splice_encoding .................................... SUCCESS [  0.215 s]
[INFO] splice_protocol .................................... SUCCESS [  6.337 s]
[INFO] splice_timestamp_api ............................... SUCCESS [  0.107 s]
[INFO] splice_access_api .................................. SUCCESS [  0.161 s]
[INFO] splice_si_api ...................................... SUCCESS [  0.158 s]
[INFO] pipeline_api ....................................... SUCCESS [  0.551 s]
[INFO] db-tools-i18n ...................................... SUCCESS [  1.439 s]
[INFO] db-drda ............................................ SUCCESS [  0.640 s]
[INFO] db-client .......................................... SUCCESS [  0.701 s]
[INFO] db-tools-ij ........................................ SUCCESS [  0.297 s]
[INFO] db-testing ......................................... SUCCESS [  0.148 s]
[INFO] splice_machine ..................................... SUCCESS [  1.520 s]
[INFO] sqlj-it-procs ...................................... SUCCESS [  0.159 s]
[INFO] txn-it-procs ....................................... SUCCESS [  0.120 s]
[INFO] splice_aws ......................................... SUCCESS [  4.354 s]
[INFO] hbase_storage ...................................... SUCCESS [  0.690 s]
[INFO] hbase_pipeline ..................................... SUCCESS [  0.223 s]
[INFO] Splicemachine Scala Utils .......................... SUCCESS [  1.489 s]
[INFO] Spark SQL API Access ............................... SUCCESS [  1.845 s]
[INFO] hbase_sql .......................................... SUCCESS [  3.552 s]
[INFO] standalone ......................................... SUCCESS [  0.250 s]
[INFO] Splicemachine Spark Bindings ....................... SUCCESS [ 18.264 s]
[INFO] External Native Spark Data Source .................. SUCCESS [  3.494 s]
[INFO] platform_it ........................................ SUCCESS [ 24.502 s]
[INFO] splice_ck .......................................... SUCCESS [  5.684 s]
[INFO] splice_machine-assembly ............................ SUCCESS [  8.303 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

## Building Spliceengine HDP Profile

Find the hadoop profile that you want to build in the parent pom.xml of spliceeengine. These are platform dependent on dependencies and require pulling down and building other repositories from the corresponding open source repositories. The `hdp3.1.0` profile is the platform and the `hdp_service` profile creates an rpm that can be used to deploy spliceengine.

i.e. `mvn clean install -Pcore,hdp3.1.0,hdp_service -DskipTests`

The corresponding hdp version taken from the profile is `3.1.0.0-78`
You can search in the hortonworks repository for the release tag and then you use that tag to check out and build the needed dependencies 

`git checkout HDP-3.1.0.0-78-tag`

### Build [zookeeper-release](https://github.com/hortonworks/zookeeper-release/)

```sh
ant mvn-install
```

```ant
mvn-install:
     [echo] /tmp/hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper-3.4.6.pom
     [echo] /tmp/hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper-3.4.6.jar
     [echo] /hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper-3.4.6-sources.jar
     [echo] /tmp/hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper-3.4.6-javadoc.jar
     [echo] /tmp/hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper-3.4.6-tests.jar
[artifact:install] [INFO] Installing /tmp/hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper.pom to /home/bklo/.m2/repository/org/apache/zookeeper/zookeeper/3.4.6/zookeeper-3.4.6.pom
[artifact:install] [INFO] Installing /tmp/hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper-3.4.6.jar to /home/bklo/.m2/repository/org/apache/zookeeper/zookeeper/3.4.6/zookeeper-3.4.6.jar
[artifact:install] [INFO] Installing /tmp/hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper-3.4.6-sources.jar to /home/bklo/.m2/repository/org/apache/zookeeper/zookeeper/3.4.6/zookeeper-3.4.6-sources.jar
[artifact:install] [INFO] Installing /tmp/hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper-3.4.6-javadoc.jar to /home/bklo/.m2/repository/org/apache/zookeeper/zookeeper/3.4.6/zookeeper-3.4.6-javadoc.jar
[artifact:install] [INFO] Installing /tmp/hdp/zookeeper-release/build/zookeeper-3.4.6/dist-maven/zookeeper-3.4.6-tests.jar to /home/bklo/.m2/repository/org/apache/zookeeper/zookeeper/3.4.6/zookeeper-3.4.6-tests.jar
BUILD SUCCESSFUL
Total time: 7 seconds
```

Ant doesn't deposit it into your maven repository, so you need to install the file manually

```sh 
mvn install:install-file -DgroupId=org.apache.zookeeper -DartifactId=zookeeper -Dversion=:3.4.6.3.1.0.0-78 -Dpackaging=pom -Dfile=zookeeper-3.4.6.jar -DpomFile=zookeeper.pom 
```

### Build [spark2-release](https://github.com/hortonworks/spark2-release)

```sh
mvn install -Phadoop2 -DskipTests
```

```xml
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Spark Project Parent POM 2.3.2.3.1.0.0-78:
[INFO]
[INFO] Spark Project Parent POM ........................... SUCCESS [  3.145 s]
[INFO] Spark Project Tags ................................. SUCCESS [  3.770 s]
[INFO] Spark Project Sketch ............................... SUCCESS [  4.963 s]
[INFO] Spark Project Local DB ............................. SUCCESS [  2.456 s]
[INFO] Spark Project Networking ........................... SUCCESS [  4.352 s]
[INFO] Spark Project Shuffle Streaming Service ............ SUCCESS [  2.352 s]
[INFO] Spark Project Unsafe ............................... SUCCESS [  6.791 s]
[INFO] Spark Project Launcher ............................. SUCCESS [  3.620 s]
[INFO] Spark Project Core ................................. SUCCESS [02:04 min]
[INFO] Spark Project ML Local Library ..................... SUCCESS [ 27.626 s]
[INFO] Spark Project GraphX ............................... SUCCESS [ 29.436 s]
[INFO] Spark Project Streaming ............................ SUCCESS [ 55.916 s]
[INFO] Spark Project Catalyst ............................. SUCCESS [01:30 min]
[INFO] Spark Project SQL .................................. SUCCESS [02:36 min]
[INFO] Spark Project ML Library ........................... SUCCESS [02:00 min]
[INFO] Spark Project Tools ................................ SUCCESS [  5.203 s]
[INFO] Spark Project Hive ................................. SUCCESS [01:05 min]
[INFO] Spark Project REPL ................................. SUCCESS [ 13.937 s]
[INFO] Spark Project Assembly ............................. SUCCESS [  2.129 s]
[INFO] Spark Integration for Kafka 0.10 ................... SUCCESS [ 24.750 s]
[INFO] Kafka 0.10 Source for Structured Streaming ......... SUCCESS [ 28.534 s]
[INFO] Spark Project Examples ............................. SUCCESS [ 28.665 s]
[INFO] Spark Integration for Kafka 0.10 Assembly .......... SUCCESS [  5.599 s]
[INFO] Standalone Metastore ............................... SUCCESS [ 12.530 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```


### Build [arrow-release](https://github.com/hortonworks/arrow-release)

Inside `arrow-release/java`

```sh
mvn install -DskipTests
```

```xml
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Apache Arrow Java Root POM 0.8.0.3.1.0.0-78:
[INFO]
[INFO] Apache Arrow Java Root POM ......................... SUCCESS [  7.941 s]
[INFO] Arrow Format ....................................... SUCCESS [  4.949 s]
[INFO] Arrow Memory ....................................... SUCCESS [  4.408 s]
[INFO] Arrow Vectors ...................................... SUCCESS [ 11.861 s]
[INFO] Arrow Tools ........................................ SUCCESS [  5.807 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

### Build [orc-release](https://github.com/hortonworks/orc-release)

Inside `orc-release/java`

```sh
mvn install -DskipTests
```

```xml
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Apache ORC 1.5.1:
[INFO]
[INFO] Apache ORC ......................................... SUCCESS [  1.223 s]
[INFO] ORC Shims .......................................... SUCCESS [  1.017 s]
[INFO] ORC Core ........................................... SUCCESS [  2.591 s]
[INFO] ORC MapReduce ...................................... SUCCESS [  0.862 s]
[INFO] ORC Tools .......................................... SUCCESS [  3.082 s]
[INFO] ORC Examples ....................................... SUCCESS [  2.346 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

### Build [tez-release](https://github.com/hortonworks/tez-release)

```sh
mvn install -DskipTests
```

```xml
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for tez 0.9.1.3.1.0.0-78:
[INFO]
[INFO] tez ................................................ SUCCESS [  1.019 s]
[INFO] hadoop-shim ........................................ SUCCESS [  0.736 s]
[INFO] tez-api ............................................ SUCCESS [  0.958 s]
[INFO] tez-common ......................................... SUCCESS [  0.079 s]
[INFO] tez-runtime-internals .............................. SUCCESS [  0.138 s]
[INFO] tez-runtime-library ................................ SUCCESS [  0.257 s]
[INFO] tez-mapreduce ...................................... SUCCESS [  0.183 s]
[INFO] tez-examples ....................................... SUCCESS [  0.137 s]
[INFO] tez-dag ............................................ SUCCESS [  0.655 s]
[INFO] tez-tests .......................................... SUCCESS [  0.171 s]
[INFO] tez-ext-service-tests .............................. SUCCESS [  0.162 s]
[INFO] tez-ui ............................................. SUCCESS [ 23.060 s]
[INFO] tez-plugins ........................................ SUCCESS [  0.045 s]
[INFO] tez-protobuf-history-plugin ........................ SUCCESS [  0.128 s]
[INFO] tez-yarn-timeline-history .......................... SUCCESS [  0.106 s]
[INFO] tez-yarn-timeline-history-with-acls ................ SUCCESS [  0.128 s]
[INFO] tez-yarn-timeline-cache-plugin ..................... SUCCESS [  9.514 s]
[INFO] tez-yarn-timeline-history-with-fs .................. SUCCESS [  0.173 s]
[INFO] tez-history-parser ................................. SUCCESS [ 10.342 s]
[INFO] tez-aux-services ................................... SUCCESS [  4.948 s]
[INFO] tez-tools .......................................... SUCCESS [  0.061 s]
[INFO] tez-perf-analyzer .................................. SUCCESS [  0.059 s]
[INFO] tez-job-analyzer ................................... SUCCESS [  0.094 s]
[INFO] tez-javadoc-tools .................................. SUCCESS [  0.073 s]
[INFO] hadoop-shim-impls .................................. SUCCESS [  0.059 s]
[INFO] hadoop-shim-2.8 .................................... SUCCESS [  0.095 s]
[INFO] tez-dist ........................................... SUCCESS [ 15.962 s]
[INFO] Tez ................................................ SUCCESS [  0.020 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

### Build [bigtop-interop-release](https://github.com/hortonworks/bigdata-interop-release)

```sh
mvn install -Phadoop2 -DskipTests
```

```xml
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for BigDataOSS Parent 1.9.10.3.1.0.0-78:
[INFO]
[INFO] BigDataOSS Parent .................................. SUCCESS [  0.523 s]
[INFO] util ............................................... SUCCESS [  0.753 s]
[INFO] util-hadoop-hadoop2 ................................ SUCCESS [  2.886 s]
[INFO] gcsio .............................................. SUCCESS [  0.090 s]
[INFO] gcs-connector-hadoop2 .............................. SUCCESS [  5.033 s]
[INFO] bigquery-connector-hadoop2 ......................... SUCCESS [  2.859 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

### Build [hive-release](https://github.com/hortonworks/hive-release)

```sh
mvn install -DskipTests
```

```xml
[INFO] Hive Storage API 2.3.0.3.1.0.0-78 .................. SUCCESS [  2.739 s]
[INFO] Hive 3.1.0-SNAPSHOT ................................ SUCCESS [  0.637 s]
[INFO] Hive Classifications 3.1.0-SNAPSHOT ................ SUCCESS [  0.740 s]
[INFO] Hive Shims Common 3.1.0-SNAPSHOT ................... SUCCESS [  1.371 s]
[INFO] Hive Shims 0.23 3.1.0-SNAPSHOT ..................... SUCCESS [  2.053 s]
[INFO] Hive Shims Scheduler 3.1.0-SNAPSHOT ................ SUCCESS [  1.097 s]
[INFO] Hive Shims 3.1.0-SNAPSHOT .......................... SUCCESS [  0.772 s]
[INFO] Hive Common 3.1.0-SNAPSHOT ......................... FAILURE [  3.714 s]
[INFO] Hive Serde 3.1.0-SNAPSHOT .......................... SUCCESS [  0.033 s]
[INFO] Hive Standalone Metastore 3.1.0-SNAPSHOT ........... SUCCESS [  0.775 s]
[INFO] Hive Metastore 3.1.0-SNAPSHOT ...................... SUCCESS [  4.347 s]
[INFO] Hive Vector-Code-Gen Utilities 3.1.0-SNAPSHOT ...... SUCCESS [  0.349 s]
[INFO] Hive Llap Common 3.1.0-SNAPSHOT .................... SUCCESS [  0.324 s]
[INFO] Hive Llap Client 3.1.0-SNAPSHOT .................... SUCCESS [  0.876 s]
[INFO] Hive Llap Tez 3.1.0-SNAPSHOT ....................... SUCCESS [  2.213 s]
[INFO] Hive Spark Remote Client 3.1.0-SNAPSHOT ............ SUCCESS [  1.231 s]
[INFO] Hive Query Language 3.1.0-SNAPSHOT ................. SUCCESS [  5.978 s]
[INFO] Hive Llap Server 3.1.0-SNAPSHOT .................... SUCCESS [  1.538 s]
[INFO] Hive Service 3.1.0-SNAPSHOT ........................ SUCCESS [  1.456 s]
[INFO] Hive Accumulo Handler 3.1.0-SNAPSHOT ............... SUCCESS [  0.987 s]
[INFO] Hive JDBC 3.1.0-SNAPSHOT ........................... SUCCESS [  0.567 s]
[INFO] Hive Beeline 3.1.0-SNAPSHOT ........................ SUCCESS [  2.985 s]
[INFO] Hive CLI 3.1.0-SNAPSHOT ............................ SUCCESS [  3.267 s]
[INFO] Hive Contrib 3.1.0-SNAPSHOT ........................ SUCCESS [  1.421 s]
[INFO] Hive Druid Handler 3.1.0-SNAPSHOT .................. SUCCESS [  0.213 s]
[INFO] Hive HBase Handler 3.1.0-SNAPSHOT .................. SUCCESS [  2.231 s]
[INFO] Hive JDBC Handler 3.1.0-SNAPSHOT ................... SUCCESS [  2.678 s]
[INFO] Hive HCatalog 3.1.0-SNAPSHOT ....................... SUCCESS [  1.318 s]
[INFO] Hive HCatalog Core 3.1.0-SNAPSHOT .................. SUCCESS [  0.867 s]
[INFO] Hive HCatalog Pig Adapter 3.1.0-SNAPSHOT ........... SUCCESS [  0.567 s]
[INFO] Hive HCatalog Server Extensions 3.1.0-SNAPSHOT ..... SUCCESS [  1.023 s]
[INFO] Hive HCatalog Webhcat Java Client 3.1.0-SNAPSHOT ... SUCCESS [  1.087 s]
[INFO] Hive HCatalog Webhcat 3.1.0-SNAPSHOT ............... SUCCESS [  0.567 s]
[INFO] Hive HCatalog Streaming 3.1.0-SNAPSHOT ............. SUCCESS [  0.876 s]
[INFO] Hive HPL/SQL 3.1.0-SNAPSHOT ........................ SUCCESS [  1.978 s]
[INFO] Hive Streaming 3.1.0-SNAPSHOT ...................... SUCCESS [  1.341 s]
[INFO] Hive Llap External Client 3.1.0-SNAPSHOT ........... SUCCESS [  1.219 s]
[INFO] Hive Shims Aggregator 3.1.0-SNAPSHOT ............... SUCCESS [  0.567 s]
[INFO] Hive Kryo Registrator 3.1.0-SNAPSHOT ............... SUCCESS [  0.432 s]
[INFO] Hive TestUtils 3.1.0-SNAPSHOT ...................... SUCCESS [  2.324 s]
[INFO] Hive Upgrade Acid 3.1.0-SNAPSHOT ................... SUCCESS [  0.862 s]
[INFO] Hive Pre Upgrade Acid 3.1.0-SNAPSHOT ............... SUCCESS [  0.543 s]
[INFO] Hive Kafka Storage Handler 3.1.0-SNAPSHOT .......... SUCCESS [  0.654 s]
[INFO] Hive Packaging 3.1.0-SNAPSHOT ...................... SUCCESS [  2.720 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[INFO] ------------------------------------------------------------------------
```

### Build [hadoop-release](https://github.com/hortonworks/hadoop-release)

```sh
mvn install -DskipTests
```

```xml
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Apache Hadoop Main 3.1.1.3.1.0.0-78:
[INFO]
[INFO] Apache Hadoop Main ................................. SUCCESS [  0.775 s]
[INFO] Apache Hadoop Build Tools .......................... SUCCESS [  0.958 s]
[INFO] Apache Hadoop Project POM .......................... SUCCESS [  0.775 s]
[INFO] Apache Hadoop Annotations .......................... SUCCESS [  1.320 s]
[INFO] Apache Hadoop Project Dist POM ..................... SUCCESS [  0.497 s]
[INFO] Apache Hadoop Assemblies ........................... SUCCESS [  0.443 s]
[INFO] Apache Hadoop Maven Plugins ........................ SUCCESS [  1.668 s]
[INFO] Apache Hadoop MiniKDC .............................. SUCCESS [  0.839 s]
[INFO] Apache Hadoop Auth ................................. SUCCESS [  2.363 s]
[INFO] Apache Hadoop Auth Examples ........................ SUCCESS [  1.173 s]
[INFO] Apache Hadoop Common ............................... SUCCESS [ 13.085 s]
[INFO] Apache Hadoop NFS .................................. SUCCESS [  1.667 s]
[INFO] Apache Hadoop KMS .................................. SUCCESS [  1.621 s]
[INFO] Apache Hadoop Common Project ....................... SUCCESS [  0.411 s]
[INFO] Apache Hadoop HDFS Client .......................... SUCCESS [  8.338 s]
[INFO] Apache Hadoop HDFS ................................. SUCCESS [ 11.669 s]
[INFO] Apache Hadoop HDFS Native Client ................... SUCCESS [  0.747 s]
[INFO] Apache Hadoop HttpFS ............................... SUCCESS [  1.921 s]
[INFO] Apache Hadoop HDFS-NFS ............................. SUCCESS [  1.604 s]
[INFO] Apache Hadoop HDFS-RBF ............................. SUCCESS [  2.727 s]
[INFO] Apache Hadoop HDFS Project ......................... SUCCESS [  0.342 s]
[INFO] Apache Hadoop YARN ................................. SUCCESS [  0.400 s]
[INFO] Apache Hadoop YARN API ............................. SUCCESS [  4.347 s]
[INFO] Apache Hadoop YARN Common .......................... SUCCESS [  5.125 s]
[INFO] Apache Hadoop YARN Registry ........................ SUCCESS [  1.583 s]
[INFO] Apache Hadoop YARN Server .......................... SUCCESS [  0.360 s]
[INFO] Apache Hadoop YARN Server Common ................... SUCCESS [  2.923 s]
[INFO] Apache Hadoop YARN NodeManager ..................... SUCCESS [  3.284 s]
[INFO] Apache Hadoop YARN Web Proxy ....................... SUCCESS [  1.569 s]
[INFO] Apache Hadoop YARN ApplicationHistoryService ....... SUCCESS [  1.830 s]
[INFO] Apache Hadoop YARN Timeline Service ................ SUCCESS [  1.848 s]
[INFO] Apache Hadoop YARN ResourceManager ................. SUCCESS [  5.570 s]
[INFO] Apache Hadoop YARN Server Tests .................... SUCCESS [  1.724 s]
[INFO] Apache Hadoop YARN Client .......................... SUCCESS [  1.903 s]
[INFO] Apache Hadoop YARN SharedCacheManager .............. SUCCESS [  1.516 s]
[INFO] Apache Hadoop YARN Timeline Plugin Storage ......... SUCCESS [  1.594 s]
[INFO] Apache Hadoop YARN TimelineService HBase Backend ... SUCCESS [  0.342 s]
[INFO] Apache Hadoop YARN TimelineService HBase Common .... SUCCESS [  2.112 s]
[INFO] Apache Hadoop YARN TimelineService HBase Client .... SUCCESS [  2.521 s]
[INFO] Apache Hadoop YARN TimelineService HBase Servers ... SUCCESS [  0.337 s]
[INFO] Apache Hadoop YARN TimelineService HBase Server 1.2  SUCCESS [  2.444 s]
[INFO] Apache Hadoop YARN TimelineService HBase tests ..... SUCCESS [  1.387 s]
[INFO] Apache Hadoop YARN Router .......................... SUCCESS [  1.649 s]
[INFO] Apache Hadoop YARN Applications .................... SUCCESS [  0.359 s]
[INFO] Apache Hadoop YARN DistributedShell ................ SUCCESS [  1.601 s]
[INFO] Apache Hadoop YARN Unmanaged Am Launcher ........... SUCCESS [  1.423 s]
[INFO] Apache Hadoop MapReduce Client ..................... SUCCESS [  1.064 s]
[INFO] Apache Hadoop MapReduce Core ....................... SUCCESS [  3.134 s]
[INFO] Apache Hadoop MapReduce Common ..................... SUCCESS [  2.150 s]
[INFO] Apache Hadoop MapReduce Shuffle .................... SUCCESS [  1.721 s]
[INFO] Apache Hadoop MapReduce App ........................ SUCCESS [  3.125 s]
[INFO] Apache Hadoop MapReduce HistoryServer .............. SUCCESS [  2.056 s]
[INFO] Apache Hadoop MapReduce JobClient .................. SUCCESS [  2.767 s]
[INFO] Apache Hadoop Mini-Cluster ......................... SUCCESS [  1.816 s]
[INFO] Apache Hadoop YARN Services ........................ SUCCESS [  0.334 s]
[INFO] Apache Hadoop YARN Services Core ................... SUCCESS [  2.368 s]
[INFO] Apache Hadoop YARN Services API .................... SUCCESS [  1.883 s]
[INFO] Apache Hadoop YARN Site ............................ SUCCESS [  0.420 s]
[INFO] Apache Hadoop YARN UI .............................. SUCCESS [  0.349 s]
[INFO] Apache Hadoop YARN Project ......................... SUCCESS [  1.612 s]
[INFO] Apache Hadoop MapReduce HistoryServer Plugins ...... SUCCESS [  1.319 s]
[INFO] Apache Hadoop MapReduce NativeTask ................. SUCCESS [  1.477 s]
[INFO] Apache Hadoop MapReduce Uploader ................... SUCCESS [  1.317 s]
[INFO] Apache Hadoop MapReduce Examples ................... SUCCESS [  1.691 s]
[INFO] Apache Hadoop MapReduce ............................ SUCCESS [  1.070 s]
[INFO] Apache Hadoop MapReduce Streaming .................. SUCCESS [  1.712 s]
[INFO] Apache Hadoop Distributed Copy ..................... SUCCESS [  1.831 s]
[INFO] Apache Hadoop Archives ............................. SUCCESS [  1.446 s]
[INFO] Apache Hadoop Archive Logs ......................... SUCCESS [  1.461 s]
[INFO] Apache Hadoop Rumen ................................ SUCCESS [  1.881 s]
[INFO] Apache Hadoop Gridmix .............................. SUCCESS [  1.687 s]
[INFO] Apache Hadoop Data Join ............................ SUCCESS [  1.538 s]
[INFO] Apache Hadoop Extras ............................... SUCCESS [  1.564 s]
[INFO] Apache Hadoop Pipes ................................ SUCCESS [  0.445 s]
[INFO] Apache Hadoop OpenStack support .................... SUCCESS [  1.214 s]
[INFO] Apache Hadoop Amazon Web Services support .......... SUCCESS [  5.135 s]
[INFO] Apache Hadoop Kafka Library support ................ SUCCESS [  1.250 s]
[INFO] Apache Hadoop Azure support ........................ SUCCESS [  1.974 s]
[INFO] Apache Hadoop Aliyun OSS support ................... SUCCESS [  1.311 s]
[INFO] Apache Hadoop Client Aggregator .................... SUCCESS [  1.276 s]
[INFO] Apache Hadoop Scheduler Load Simulator ............. SUCCESS [  2.167 s]
[INFO] Apache Hadoop Resource Estimator Service ........... SUCCESS [  1.621 s]
[INFO] Apache Hadoop Azure Data Lake support .............. SUCCESS [  1.177 s]
[INFO] Apache Hadoop Image Generation Tool ................ SUCCESS [  1.894 s]
[INFO] Apache Hadoop Tools Dist ........................... SUCCESS [  2.978 s]
[INFO] Apache Hadoop Tools ................................ SUCCESS [  0.341 s]
[INFO] Apache Hadoop Client API ........................... SUCCESS [ 52.723 s]
[INFO] Apache Hadoop Client Runtime ....................... SUCCESS [ 42.140 s]
[INFO] Apache Hadoop Client Packaging Invariants .......... SUCCESS [  3.507 s]
[INFO] Apache Hadoop Client Test Minicluster .............. SUCCESS [01:19 min]
[INFO] Apache Hadoop Client Packaging Invariants for Te   . SUCCESS [  2.033 s]
[INFO] Apache Hadoop Client Packaging Integration Tests ... SUCCESS [  0.404 s]
[INFO] Apache Hadoop Distribution ......................... SUCCESS [  2.689 s]
[INFO] Apache Hadoop Client Modules ....................... SUCCESS [  0.359 s]
[INFO] Apache Hadoop Cloud Storage ........................ SUCCESS [  2.319 s]
[INFO] Apache Hadoop Cloud Storage Project ................ SUCCESS [  0.348 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

### Build [hbase-release](https://github.com/hortonworks/hbase-release)

```sh
mvn install -DskipTests
```

```xml
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Apache HBase 2.0.2.3.1.0.0-78:
[INFO]
[INFO] Apache HBase ....................................... SUCCESS [  1.547 s]
[INFO] Apache HBase - Checkstyle .......................... SUCCESS [  0.796 s]
[INFO] Apache HBase - Build Support ....................... SUCCESS [  0.072 s]
[INFO] Apache HBase - Error Prone Rules ................... SUCCESS [  0.315 s]
[INFO] Apache HBase - Annotations ......................... SUCCESS [  0.449 s]
[INFO] Apache HBase - Build Configuration ................. SUCCESS [  0.099 s]
[INFO] Apache HBase - Shaded Protocol ..................... SUCCESS [ 18.741 s]
[INFO] Apache HBase - Common .............................. SUCCESS [  3.662 s]
[INFO] Apache HBase - Metrics API ......................... SUCCESS [  1.065 s]
[INFO] Apache HBase - Hadoop Compatibility ................ SUCCESS [  1.206 s]
[INFO] Apache HBase - Metrics Implementation .............. SUCCESS [  1.074 s]
[INFO] Apache HBase - Hadoop Two Compatibility ............ SUCCESS [  1.725 s]
[INFO] Apache HBase - Protocol ............................ SUCCESS [  0.638 s]
[INFO] Apache HBase - Client .............................. SUCCESS [  1.857 s]
[INFO] Apache HBase - Zookeeper ........................... SUCCESS [  1.341 s]
[INFO] Apache HBase - Replication ......................... SUCCESS [  1.228 s]
[INFO] Apache HBase - Resource Bundle ..................... SUCCESS [  0.148 s]
[INFO] Apache HBase - HTTP ................................ SUCCESS [  2.185 s]
[INFO] Apache HBase - Procedure ........................... SUCCESS [  0.975 s]
[INFO] Apache HBase - Server .............................. SUCCESS [  3.991 s]
[INFO] Apache HBase - MapReduce ........................... SUCCESS [  2.346 s]
[INFO] Apache HBase - Testing Util ........................ SUCCESS [  1.575 s]
[INFO] Apache HBase - Thrift .............................. SUCCESS [  2.841 s]
[INFO] Apache HBase - RSGroup ............................. SUCCESS [  1.809 s]
[INFO] Apache HBase - Shell ............................... SUCCESS [  2.156 s]
[INFO] Apache HBase - Coprocessor Endpoint ................ SUCCESS [  1.902 s]
[INFO] Apache HBase - Backup .............................. SUCCESS [  1.456 s]
[INFO] Apache HBase - Integration Tests ................... SUCCESS [  2.217 s]
[INFO] Apache HBase - Rest ................................ SUCCESS [  2.091 s]
[INFO] Apache HBase - Examples ............................ SUCCESS [  1.900 s]
[INFO] Apache HBase - External Block Cache ................ SUCCESS [  1.512 s]
[INFO] Apache HBase - Spark ............................... SUCCESS [  2.962 s]
[INFO] Apache HBase - Spark Integration Tests ............. SUCCESS [  1.378 s]
[INFO] Apache HBase - Shaded .............................. SUCCESS [  0.408 s]
[INFO] Apache HBase - Shaded - Client ..................... SUCCESS [  0.779 s]
[INFO] Apache HBase - Shaded - MapReduce .................. SUCCESS [  1.254 s]
[INFO] Apache HBase - Assembly ............................ SUCCESS [  3.315 s]
[INFO] Apache HBase Shaded Packaging Invariants ........... SUCCESS [  1.358 s]
[INFO] Apache HBase - Archetypes .......................... SUCCESS [  0.031 s]
[INFO] Apache HBase - Exemplar for hbase-client archetype . SUCCESS [  1.565 s]
[INFO] Apache HBase - Exemplar for hbase-shaded-client archetype SUCCESS [  1.549 s]
[INFO] Apache HBase - Archetype builder ................... SUCCESS [  0.383 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

### Building Spliceengine

Once all the dependencies are build and in your local `~/.m2 repo`, then you can build spliceengine 

```sh
mvn clean install -Pcore,hdp3.1.0,hdp_service -DskipTests
```

```xml
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for spliceengine-parent 3.1.0.2001-SNAPSHOT:
[INFO]
[INFO] spliceengine-parent ................................ SUCCESS [  0.984 s]
[INFO] utilities .......................................... SUCCESS [  4.413 s]
[INFO] db-shared .......................................... SUCCESS [  0.231 s]
[INFO] db-tools-testing ................................... SUCCESS [  0.314 s]
[INFO] db-build ........................................... SUCCESS [  0.096 s]
[INFO] db-engine .......................................... SUCCESS [  5.237 s]
[INFO] splice_encoding .................................... SUCCESS [  0.215 s]
[INFO] splice_protocol .................................... SUCCESS [  6.337 s]
[INFO] splice_timestamp_api ............................... SUCCESS [  0.107 s]
[INFO] splice_access_api .................................. SUCCESS [  0.161 s]
[INFO] splice_si_api ...................................... SUCCESS [  0.158 s]
[INFO] pipeline_api ....................................... SUCCESS [  0.551 s]
[INFO] db-tools-i18n ...................................... SUCCESS [  1.439 s]
[INFO] db-drda ............................................ SUCCESS [  0.640 s]
[INFO] db-client .......................................... SUCCESS [  0.701 s]
[INFO] db-tools-ij ........................................ SUCCESS [  0.297 s]
[INFO] db-testing ......................................... SUCCESS [  0.148 s]
[INFO] splice_machine ..................................... SUCCESS [  1.520 s]
[INFO] sqlj-it-procs ...................................... SUCCESS [  0.159 s]
[INFO] txn-it-procs ....................................... SUCCESS [  0.120 s]
[INFO] splice_aws ......................................... SUCCESS [  4.354 s]
[INFO] hbase_storage ...................................... SUCCESS [  0.690 s]
[INFO] hbase_pipeline ..................................... SUCCESS [  0.223 s]
[INFO] Splicemachine Scala Utils .......................... SUCCESS [  1.489 s]
[INFO] Spark SQL API Access ............................... SUCCESS [  1.845 s]
[INFO] hbase_sql .......................................... SUCCESS [  3.552 s]
[INFO] standalone ......................................... SUCCESS [  0.250 s]
[INFO] Splicemachine Spark Bindings ....................... SUCCESS [ 18.264 s]
[INFO] External Native Spark Data Source .................. SUCCESS [  3.494 s]
[INFO] platform_it ........................................ SUCCESS [ 24.502 s]
[INFO] splice_ck .......................................... SUCCESS [  5.684 s]
[INFO] splice_machine-assembly ............................ SUCCESS [  8.303 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```