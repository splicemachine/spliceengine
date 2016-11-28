# Splice Machine SQL Engine

Before proceeding with these instructions, please follow the instructions found in the GETTING-STARTED.md file.

## A note about branches in this project
This `master` branch is the home for the 3.0 version of Splice Machine.  Any pull requests submitted to this branch will only be picked up by that release. For patches to the current released version (2.0)  of Splice Machine see `branch-2.0`

## Quick start
(First make sure you have followed the instructions in the GETTING-STARTED.md file)

For a quick start using the defaults, from the top-level:

`./start-splice-cluster`

This will compile everything and start the database. 

Then to connect to the database:

`./sqlshell.sh`


## Code Structure
Code is separated into 2 conceptual tiers: Core, and Storage Architectures

### Core
The Core modules represent all code which is architecture-independent. This code is (by definition) shared by all architectures on which Splice runs. If you are writing code which is independent of architectures, or which can rely on the architecture APIs as currently defined, then place that code in a core module.

#### Core modules are further broken down as follows:

##### Library modules:
* **db-build**: Classes used as part of build.
* **db-client**: Splice JDBC driver.
* **db-drda**: DRDA classes.
* **db-engine**: Server, compiler, optimizer, etc.
* **db-shared**: Code shared by client/server.
* **db-tools-i18n**: Internationalization
* **db-tools-ij**: Our IJ implementation.
* **db-tools-testing**: Testing code shared by test cases in all modules.
* **db-testing**: Old integration tests that should be deleted but we want to hang on to them for a while.
* **splice_encoding**: This is a library module consisting of our sorted encoding library, and sundry other utilities
* **splice_protocol**: This houses our protocol buffer libraries and proto definition files. (-sf- this may later be dispersed amongst other module locations)
* **splice_timestamp_api**: This houses the api for generating distributed monotonic increasing timestamps for transactional purposes (-sf- this may later be merged with splice_si_api)
* **splice_access_api**: this holds the main interfaces for defining an appropriate storage architecture for running SpliceMachine over.

##### Core execution modules:
* **splice_si_api**: This holds the core, architecture-independent code necessary to properly implement a distributed SnapshotIsolation storage system over top of the library modules previously defined. Included in this is a suite of acceptance tests which verify that any constructed storage architecture is properly transactional.  these tests are annotated with an @Category(ArchitectureSpecific.class) annotation in their source.
* **pipeline_api**: This holds the core, architecture-independent code necessary to properly implement a bulk data movement pipeline over a transactional storage engine. Included are also a suite of acceptance tests which verify that any constructed storage engine can properly perform bulk data movement (these tests are annotated with @Category(ArchitectureSpecific.class) annotations)
* **splice_machine**: This holds the core SQL execution engine (and DataDictionary management tools) which constitutes SpliceMachine, relying on pipeline and si behaviors. Included here are acceptance tests in two forms: tests which are annotated with @Category(ArchitectureSpecific.class), and Integration tests which (by convention) end in the suffix IT.java.

### Storage Architectures
In addition to the core modules, we ship two distinct storage architectures:
* **Memory:** This is primarily for testing, and to serve as a simplistic reference architecture; there is no network behavior (aside from JDBC), and all data is held on-heap with no persistence. This architecture is _not_ for production use!
* **HBase:** This is SpliceMachine as we all know it. This architecture runs on HBase and Spark, and is suitable for production use.

#### Memory
The Memory system consists of three modules:
* **mem_storage**: This constructs a storage engine which satisfies all acceptance tests as a snapshot-isolation suitable transactional storage engine
* **mem_pipeline**: This is an in-memory (read direct) bulk data architecture built over pipeline_api
* **mem_sql**: This is the in-memory components necessary to run a fully functional SpliceMachine SQL execution engine over mem_storage and mem_pipeline

#### HBase
The HBase architecture consists of three modules:
* **hbase_storage**: This constructs an hbase-based SI storage engine, satisfying all acceptance tests for SI
* **hbase_pipeline**: An Hbase bulk-data movement pipeline built over HBase coprocessors
* **hbase_sql**: The hbase-specific (and spark-specific) components necessary to run a fully functional SpliceMachine SQL execution engine over HBase.

## Building and running Splice Machine locally (examples include `-DskipTests` option for expidiency)

### Build core modules and "mem" platform modules
Since Core modules are shared, they need only be built once; after that, they only need to be built when code in the core modules themselves change (or when a full clean build is desired). To build only the core modules,
* build core modules
```mvn clean install -Pcore -DskipTests```
> Note That there are *some* tests which will run inside of each module even in the absence of a specific architecture: these tests are annotated with @Category(ArchitectureIndependent.class) and are unit tests which validate architecture-independent code.

* build mem platform
```mvn clean install -Pmem -DskipTests```

> these steps can be combined with ```mvn clean install -Pcore,mem```
> This will build and test the in-memory database from top to bottom, including running all unit and integration tests against a fresh memory-database.

### Start a server running against the mem storage architecture
* start a server
```cd mem_sql && mvn exec:java```
* start the ij client
  * without rlwrap  ```cd splice_machine && mvn exec:java```
  * with rlwrap  ```cd splice_machine && rlwrap mvn exec:java```
* do sql stuffs (create tables, import data, etc.)

----

### Build core modules and a set of hbase platform modules
HBase is further separated into subprofiles, indicating the specific hbase distribution of interest. To build hbase against a specific distribution:
> * available hbase versions *(at the time of writing, double project check root pom for latest)* are:
>   *  `cdh5.6.0`
>   *  `cdh5.7.2`
>   *  `mapr5.1.0`
>   *  `mapr5.2.0`
>   *  `hdp2.4.2`
>   *  `hdp2.5.0`

* build core modules (only needs done once as mentioned above)
```mvn clean install -Pcore -DskipTests```
* build a set of hbase platform modules
```mvn clean install -Pcdh5.6.0```

> these steps can be combined with ```mvn clean install -Pcore,cdh5.6.0```
> This will build and test the hbase backed database from top to bottom, including running all unit and integration tests against a fresh hbase backed database.

### Start a server running against the [specified] hbase storage architecture
* start zookeeper
```cd hbase_sql && mvn exec:exec -Pcdh5.6.0,spliceZoo```
* start yarn
```cd hbase_sql && mvn exec:java -Pcdh5.6.0,spliceYarn```
* start kafka (not required)
```cd hbase_sql && mvn exec:exec -Pcdh5.6.0,spliceKafka```
* start hbase (master + 1 regionserver same JVM)
```cd hbase_sql && mvn exec:exec -Pcdh5.6.0,spliceFast```
* start additional RegionServer(s) -- memberNumber should increase as addidional RS are started
```cd hbase_sql && mvn exec:exec -Pcdh5.6.0,spliceClusterMember -DmemberNumber=2```
* start the ij client
  * without rlwrap  ```cd splice_machine && mvn exec:java```
  * with rlwrap  ```cd splice_machine && rlwrap mvn exec:java```
* do sql stuffs (create tables, import data, etc.)

----

##### Notes for running agains For MapR hbase:
* add the following empty file on your filesystem, executable and owned by root:
```
/opt/mapr/server/createJTVolume.sh
```
* create the following directory on your filesystem, owned by root with 700 permissions:
```
/private/var/mapr/cluster/yarn/rm/system
```

* add the following directory on your filesystem, owned by root with 777 permissions:
```
/private/var/mapr/cluster/yarn/rm/staging
```
