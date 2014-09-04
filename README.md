SpliceSQL Engine
===

# Building Splice
##### Prerequisites:
* java jdk1.6.0_45 (1.6.0_65 on MacOS)
* maven v3.2.1 (or newer)
** a proper settings.xml (in ~/.m2/)
** access to the splice nexus repo

#### Building the Default profile and running no tests
mvn clean install -DskipTests=true

#### Building the Default profile and running only unit tests
mvn clean install

#### Building the Default profile and running unit and fast integration tests
mvn clean install -PITs

#### Building the Default profile and running all tests
mvn clean install -PITs -Dexcluded.categories=

#### Building alternate profiles
*to be certain inspect the top level pom.xml in this project*

At the time of this writing the available profiles are:
* cloudera-cdh4.5.0 <- default
* cloudera-cdh4.3.0
* hdp1.3
* mapr3.1

to build an hbase target other than the default, add "-Dhbase.profile=PROFILE_ID" to your maven command line.

Example:
mvn clean install -DskipTests=true -Dhbase.profile=hdp1.3

##### Other notes:
* All dependencies will be pulled from splice nexus repo (http://nexus.splicemachine.com/nexus/).
* There is no requirement to build any other packages to build splice.
* All deployment (to nexus) is handled by [Jenkins](http://206.225.8.98:8080) and is not necessary for typical dev/test builds. (mvn deploy will fail with unauthorized access error)
