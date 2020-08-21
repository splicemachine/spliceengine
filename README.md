
<p align="left">
    <img src="https://doc.splicemachine.com/images/spliceTitleLogo.png" width="200"/>
</p>

# Splice Machine SQL Engine

Before proceeding with these instructions, please follow the instructions found in the GETTING-STARTED.md file.

## A Note About Branches in This Project
This `master` branch is the home for the 3.0 version of Splice Machine.  Any pull requests submitted to this branch will only be picked up by that release. For patches to the current released version (2.0)  of Splice Machine see `branch-2.0`

## Quick Start
(First make sure you have followed the instructions in the GETTING-STARTED.md file)

For a quick start using the defaults, from the top-level:
````
   ./start-splice-cluster
````

This will compile everything and start the database. Then, connect to the database:
````
   ./sqlshell.sh
````

## Code Structure
Code is separated into 2 conceptual tiers: Core, and Storage Architectures

### Core
The Core modules represent all code that is architecture-independent. This code is (by definition) shared by all architectures on which Splice runs. If you are writing code that is independent of architectures, or that can rely on the architecture APIs as currently defined, then place that code in a core module.

Core modules are further broken down as follows:

#### Library Modules:
   | Module                   | Description |
   |:-------------------------|:------------|
   | **db-build**             | Classes used as part of build. |
   | **db-client**            | Splice JDBC driver. |
   | **db-drda**              | DRDA classes. |
   | **db-engine**            | Server, compiler, optimizer, etc. |
   | **db-shared**            | Code shared by client/server. |
   | **db-tools-i18n**        | Internationalization |
   | **db-tools-ij**          | Our IJ implementation. |
   | **db-tools-testing**     | Testing code shared by test cases in all modules. |
   | **db-testing**           | Old integration tests that should be deleted but we want to hang on to them for a while. |
   | **splice_encoding**      | This is a library module consisting of our sorted encoding library, and sundry other utilities |
   | **splice_protocol**      | This houses our protocol buffer libraries and proto definition files. (-sf- this may later be dispersed amongst other module locations) |
   | **splice_timestamp_api** | This houses the api for generating distributed monotonic increasing timestamps for transactional purposes (-sf- this may later be merged with splice_si_api) |
   | **splice_access_api**    | this holds the main interfaces for defining an appropriate storage architecture for running SpliceMachine over. |


#### Core Execution modules:
   | Module                   | Description |
   |:-------------------------|:------------|
   | **splice_si_api**        | This holds the core, architecture-independent code necessary to properly implement a distributed SnapshotIsolation storage system over top of the library modules previously defined. Included in this is a suite of acceptance tests which verify that any constructed storage architecture is properly transactional.  these tests are annotated with an @Category(ArchitectureSpecific.class) annotation in their source. |
   | **pipeline_api**         | This holds the core, architecture-independent code necessary to properly implement a bulk data movement pipeline over a transactional storage engine. Included are also a suite of acceptance tests which verify that any constructed storage engine can properly perform bulk data movement (these tests are annotated with @Category(ArchitectureSpecific.class) annotations). |
   | **splice_machine**       | This holds the core SQL execution engine (and DataDictionary management tools) which constitutes SpliceMachine, relying on pipeline and si behaviors. Included here are acceptance tests in two forms: tests which are annotated with @Category(ArchitectureSpecific.class), and Integration tests which (by convention) end in the suffix IT.java. |

### Storage Architectures
In addition to the core modules, we ship two distinct storage architectures:

   | Module              | Description |
   |:--------------------|:------------|
   | **Memory**          | This is primarily for testing, and to serve as a simplistic reference architecture; there is no network behavior (aside from JDBC), and all data is held on-heap with no persistence. This architecture is _not for production use_! |
   | **HBase**           | This is Splice Machine as we all know it. This architecture runs on HBase and Spark, and is suitable for production use. |


#### Memory
The Memory system consists of three modules:

   | Module              | Description |
   |:--------------------|:------------|
   | **mem_storage**     | This constructs a storage engine which satisfies all acceptance tests as a snapshot-isolation suitable transactional storage engine. |
   | **mem_pipeline**    | This is an in-memory (read direct) bulk data architecture built over pipeline_api. |
   | **mem_sql**         | This is the in-memory components necessary to run a fully functional SpliceMachine SQL execution engine over mem_storage and mem_pipeline. |

#### HBase
The HBase architecture consists of three modules:

   | Module              | Description |
   |:--------------------|:------------|
   | **hbase_storage**   | This constructs an HBase-based SI storage engine, satisfying all acceptance tests for SI. |
   | **hbase_pipeline**  | An HBase bulk-data movement pipeline built over HBase coprocessors. |
   | **hbase_sql**       | The HBase-specific (and spark-specific) components necessary to run a fully functional SpliceMachine SQL execution engine over HBase. |

## Building and Running Splice Machine Locally

> These examples include the `-DskipTests` option for build-expediency.

### Build Core Modules and _mem_ Platform Modules
Since Core modules are shared, they need only be built once; after that, they only need to be built when code in the core modules themselves change (or when a full clean build is desired). v

#### Build Core Module
To build only the core modules:
````
    mvn clean install -Pcore -DskipTests
````

> Note that *some* tests will run inside of each module even in the absence of a specific architecture; these are unit tests that validate architecture-independent code, and are annotated with @Category(ArchitectureIndependent.class).

#### Build Mem Platform
To build only the mem modules:
````
   mvn clean install -Pmem -DskipTests
````

#### Combined Build
You can combine the build steps to build and test the in-memory database from top to bottom, including running all unit and integration tests against a fresh memory-database.
````
   mvn clean install -Pcore,mem
````

### Start a server running against the mem storage architecture
To run against the mem storage architecture, follow these steps:

1. Start a server:
2. Start the ij client:

   To start the client with rlwrap enabled:
   ````
      cd splice_machine && rlwrap mvn exec:java
   ````

   To start the client without rlwrap enabled:
   ````
      cd splice_machine && mvn exec:java
   ````
3. Run some SQL operations, such as creating tables, importing data, and running queries.

----

### Build Core Modules and a Set of HBase Platform Modules
HBase is further separated into sub-profiles, indicating the specific HBase distribution of interest. To build HBase against a specific distribution:

1. Select the HBase version to build. These are the currently available versions:
   *  `cdh5.6.0`
   *  `cdh5.7.2`
   *  `mapr5.1.0`
   *  `mapr5.2.0`
   *  `hdp2.4.2`
   *  `hdp2.5.0`

2. Build the core modules. As previously mentioned, you only need to do this once.
   ````
   mvn clean install -Pcore -DskipTests
   ````

3. Build a set of HBase platform modules
   ````
   mvn clean install -Pcdh5.6.0
   ````

   You can combine these steps to build and test the HBase backed database from top to bottom, including running all unit and integration tests against a fresh HBase backed database. For example:
   ````
   mvn clean install -Pcore,cdh5.6.0
   ````

### Start a Server Running Against the Specified HBase Storage Architecture
To start a server:

1. Start ZooKeeper:
   ````
   cd hbase_sql && mvn exec:exec -Pcdh5.6.0,spliceZoo
   ````
2. Start YARN:
   ````
   cd hbase_sql && mvn exec:java -Pcdh5.6.0,spliceYarn
   ````
3. Start Kafka (optionally):
   ````
   cd hbase_sql && mvn exec:exec -Pcdh5.6.0,spliceKafka
   ````
4. Start HBase (master + 1 regionserver, same JVM):
   ````
   cd hbase_sql && mvn exec:exec -Pcdh5.6.0,spliceFast
   ````
5. Start additional RegionServer(s) -- `memberNumber` should increase as additional RS are started:
   ````
   cd hbase_sql && mvn exec:exec -Pcdh5.6.0,spliceClusterMember -DmemberNumber=2
   ````
6. Start the ij client:

   To start the client with rlwrap enabled:
   ````
      cd splice_machine && rlwrap mvn exec:java
   ````

   To start the client without rlwrap enabled:
   ````
      cd splice_machine && mvn exec:java
   ````
7. Run some SQL operations, such as creating tables, importing data, and running queries.

----

#### Notes for Running with MapR HBase
If you're using MapR HBase:

1. Add the following empty file on your filesystem, executable and owned by root:
   ````
   /opt/mapr/server/createJTVolume.sh
   ````
2. Create the following directory on your filesystem, owned by root with 700 permissions:
   ````
   /private/var/mapr/cluster/yarn/rm/system
   ````
3. Add the following directory on your filesystem, owned by root with 777 permissions:
   ````
   /private/var/mapr/cluster/yarn/rm/staging
   ````
