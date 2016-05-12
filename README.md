SpliceSQL Engine
===

#Overview

##Code Structure
Code is separated into 2 conceptual tiers: Core, and architectures.

###Core

The Core modules represent all code which is architecture-independent. This code is (by definition) shared
by all architectures on which Splice runs. If you are writing code which is independent of architectures,
or which can rely on the architecture APIs as currently defined, then place that code in a core module.

Core modules are further broken down as follows:

Library modules:
* splice_encoding: This is a library module consisting of our sorted encoding library, and sundry other utilities
* splice_protocol: This houses our protocol buffer libraries and proto definition files. (-sf- this may later
be dispersed amongst other module locations)
* splice_timestamp_api: This houses the api for generating distributed monotonic increasing timestamps
for transactional purposes (-sf- this may later be merged with splice_si_api)
* splice_access_api: this holds the main interfaces for defining an appropriate storage architecture
for running SpliceMachine over.

Core execution modules:
* splice_si_api: This holds the core, architecture-independent code necessary to properly implement a 
distributed SnapshotIsolation storage system over top of the library modules previously defined. Included in this
is a suite of acceptance tests which verify that any constructed storage architecture is properly transactional.
these tests are annotated with an @Category(ArchitectureSpecific.class) annotation in their source.
* pipeline_api: This holds the core, architecture-independent code necessary to properly implement a bulk
data movement pipeline over a transactional storage engine. Included are also a suite of acceptance tests
which verify that any constructed storage engine can properly perform bulk data movement (these tests
are annotated with @Category(ArchitectureSpecific.class) annotations)
* splice_machine: This holds the core SQL execution engine (and DataDictionary management tools) which constitutes
SpliceMachine, relying on pipeline and si behaviors. Included here are acceptance tests in two forms: tests which
are annotated with @Category(ArchitectureSpecific.class), and Integration tests which (by convention) end in the
suffix IT.java.


In addition to the core modules, we ship two distinct storage architectures: 
* Memory: This is primarily for testing, and to serve as a simplistic reference architecture; there is no network
behavior (aside from JDBC), and all data is held on-heap with no persistence. This architecture is _not_ for production
use! 
* HBase: This is SpliceMachine as we all know it. This architecture runs on HBase and Spark, and is suitable for
production use.

####Memory
The Memory system consists of three modules:

* mem_storage: This constructs a storage engine which satisfies all acceptance tests as a snapshot-isolation suitable
transactional storage engine
* mem_pipeline: This is an in-memory (read direct) bulk data architecture built over pipeline_api
* mem_engine: This is the in-memory components necessary to run a fully functional SpliceMachine SQL execution engine
over mem_storage and mem_pipeline

####HBase
The HBase architecture consists of three modules:
* hbase_storage: This constructs an hbase-based SI storage engine, satisfying all acceptance tests for SI
* hbase_pipeline: An Hbase bulk-data movement pipeline built over HBase coprocessors
* hbase_sql: The hbase-specific (and spark-specific) components necessary to run a fully functional SpliceMachine SQL
execution engine over HBase.

# Building Splice
##### Prerequisites:
* java jdk1.7.0
* protobufs 2.5.0 
* maven v3.2.1 (or newer)
  * a proper settings.xml (in ~/.m2/)
  * access to the splice nexus repo
* Empty file (/opt/mapr/server/createJTVolume.sh) with open access (Hack around MapR's lack of Yarn Testing)
  
  
## Building Core modules
Since Core modules are shared, they need only be built once; after that, they only need to be built when
code in the core modules themselves change (or when a full clean build is desired). To build only the core modules,
`mvn install -Pcore`

Note That there are *some* tests which will run inside of each module even in the absence of a specific architecture:
these tests are annotated with @Category(ArchitectureIndependent.class) and are unit tests which validate architecture-
independent code.

##Building Memory
`mvn install -Pmem`

Note that this will build only the memory modules. If you want to also build the core modules, then add the core
profile as well:

`mvn install -Pcore,mem`

This will build and test the in-memory database from top to bottom, including running all unit and integration tests
against a fresh memory-database.

##Building HBase
HBase is further separated into subprofiles, indicating the specific hbase distribution of interest. To build
hbase against a specific distribution:

`mvn install -P${hbaseDistribution},${hbaseVersion}`

where `${hbaseDistribution},${hbaseVersion}` is the name of the distribution and hbase version that you want to build and test against. This will
run all unit _and_ integration tests(we will be separating those out better in the near-term)

###Available HBase distributions and version combos
* `cdh5.4.10,hbase1.0.0`
* `cdh5.5.2,hbase1.0.0.x`
* `cdh5.6.0,hbase1.0.0.x`
* `cdh5.7.0,hbase1.2.0`
* `mapr5.1.0,hbase1.1.0`
* `hdp2.30,hbase1.0.0.x`
* `hdp2.4.2,hbase1.1.0`

#Running testing environments

##Memory
To run an in-memory database which you can issue SQL against:
* Build mem
* `cd mem_engine`
* `mvn exec:java`

-sf- Note that this is likely to change to be something more like `mvn exec:java -Pserver` or similar, but for now.

##HBase
To run an Hbase database:
* build hbase for your profile
* `cd hbase_sql`
* `src/test/bin/start-splice-its -p ${PROFILE}`

Run `./src/test/bin/start-splice-its -h` for options:

```
Usage: src/test/bin/start-splice-its -c -p [<hbase_profile>] -h[elp]
Where:
  -c is an optional flag determining random task failures. Default is that the chaos
    monkey NOT run. To see if you have the chaos monkey running, execute:
        grep 'task fail' splice_machine/splice.log
  -p <hbase_profile> is the optional splice hbase platform to run.  One of:
  cloudera-cdh4.5.0, cloudera-cdh5.1.3, hdp2.1, mapr4.0.  Default is cloudera-cdh4.5.0.
  -h => print this message
```

-sf- there may be a different script which is preferable and which would be carried over from master_dataset. If you
have trouble finding it or using it, let me know.

-sf- Note that you MUST specify an HBase profile in order for start-splice-its to properly startup. Otherwise, 
problems will arise

##IJ
To open an ij prompt:
* `cd splice_machine`
* `mvn exec:java`

#Constructing an Uber jar
To build an uber jar for your architecture:
`mvn install -P${your architecture profiles},assemble`

##### Other notes:
* All dependencies will be pulled from splice nexus repo (http://nexus.splicemachine.com/nexus/).
* There is no requirement to build any other packages to build splice.
* All deployment (to nexus) is handled by [Jenkins](http://206.225.8.98:8080) and is not necessary for typical dev/test builds. (mvn deploy will fail with unauthorized access error)
* Quick set of instructions for installing protobufs on your Mac can be found [here](http://sleepythread.blogspot.com/2013/11/installing-protoc-25x-compiler-google.html)
