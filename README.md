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

### Then you can build just that platform with:

cd splice_machine_test
src/test/bin/stop-splice-its
cd ..
mvn -DskipTests clean install -Dspark-prepare
cd splice_machine_test
src/test/bin/start-splice-its

* Optional, start the splice machine command line (derby's ij)
* `mvn exec:java -Dij` or if you have rlwrap installed `rlwrap mvn exec:java -Pij`

##### Other notes:
* All dependencies will be pulled from splice nexus repo (http://nexus.splicemachine.com/nexus/).
* There is no requirement to build any other packages to build splice.
* All deployment (to nexus) is handled by [Jenkins](http://206.225.8.98:8080) and is not necessary for typical dev/test builds. (mvn deploy will fail with unauthorized access error)
* Quick set of instructions for installing protobufs on your Mac can be found [here](http://sleepythread.blogspot.com/2013/11/installing-protoc-25x-compiler-google.html)
