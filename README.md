SpliceSQL Engine
===

# Building Splice
##### Prerequisites:
* java jdk1.7.0
* protobufs 2.5.0 (for HBase 0.98+ platforms)
* maven v3.2.1 (or newer)
  * a proper settings.xml (in ~/.m2/)
  * access to the splice nexus repo

#### Building all target platforms and running no tests
`mvn clean install -DskipTests -Pall`

### To build a specific target first cd into to the platform target parent directory
##### Options are:
* `cdh5.4.1`

### Then you can build and start just that platform with:

`./<dev_root>/spliceengine/cdh5.4.1/splice_machine_test/start-spark-cluster1`

Run `./start-spark-cluster -h` for options:

```{spliceengine (master_dataset)}-> ./cdh5.4.1/splice_machine_test/start-spark-cluster -h
Usage: ./cdh5.4.1/splice_machine_test/start-spark-cluster -p [<hbase_profile>] [-s <n>] [-c] -h[elp]
Where:
  -b is an optional argument specifying to NOT mvn clean install -DskipTest -Dspark-prepare first.
    The default is to build first.
  -s <n> is an optional number of additional cluster RegionServer members to start.
    The default is 1 master and 2 region servers.
  -c is an optional flag determining random task failures. Default is that the chaos
    monkey NOT run. To see if you have the chaos monkey running, execute:
        grep 'task fail' <hbase_profile>/splice_machine_test/splice-derby.log
  -p <hbase_profile> is the optional splice hbase platform to run.  One of:
    c41 (cdh5.4.1), c51 (cdh5.5.1), h24 (hdp2.2.4), h30 (hdp2.3.0), m4 (mapr0.98.4), m12 (mapr0.98.12).
    Default is cdh5.4.1. CURRENTLY ONLY THE DEFAULT PROFILE WORKS WITH YARN!
  -h => print this message
```

* Optional, start the splice machine command line (derby's ij)
* `mvn exec:java -Dij` or if you have rlwrap installed `rlwrap mvn exec:java -Pij`

##### Other notes:
* All dependencies will be pulled from splice nexus repo (http://nexus.splicemachine.com/nexus/).
* There is no requirement to build any other packages to build splice.
* All deployment (to nexus) is handled by [Jenkins](http://206.225.8.98:8080) and is not necessary for typical dev/test builds. (mvn deploy will fail with unauthorized access error)
* Quick set of instructions for installing protobufs on your Mac can be found [here](http://sleepythread.blogspot.com/2013/11/installing-protoc-25x-compiler-google.html)
