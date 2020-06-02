# Splice Machine Developer Getting Started Guide

### Links (internal sites, online tools, etc):
* [Splice Machine Online Documentation (Released/Current Version)](http://doc.splicemachine.com/index.html)
* [GitHub](https://github.com/splicemachine)
* [JIRA](https://splice.atlassian.net/secure/Dashboard.jspa)

### Primary GitHub project
* [spliceengine](https://github.com/splicemachine/spliceengine) - Primary Splice Machine code<br />

> This is helpful for Github interactions, saves typing username and password repeatedly. https://help.github.com/articles/generating-ssh-keys/

#### Sample gitconfig file
* this text should be placed in ~/.gitconfig
```gitconfig
[color]
    ui = auto
    status = auto
    branch = auto
[user]
    name = <FIRST_NAME LAST_NAME> (ex. Aaron Molitor)
    email = <EMAIL ADDRESS> (ex. amolitor@splicemachine.com)
[credential "https://github.com"]
    username = <GITHUB USERNAME> (ex. ammolitor)
[push]
    default = simple
```

### Development Environment setup
#### Development/Build tools (Java, maven, etc.):
Note: JDK 1.8 is required for the master branch.

* [Java 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Apache Maven 3.3.x (Jenkins uses 3.3.9)](https://maven.apache.org/download.cgi)

#### Helpful Environment Variables, etc. (example assumes Mac OS X developer machine)
```bash
# java
export J6_HOME="`/usr/libexec/java_home -v 1.6`"
export J7_HOME="`/usr/libexec/java_home -v 1.7`"
export J8_HOME="`/usr/libexec/java_home -v 1.8`"
export JAVA_HOME=${J8_HOME}
# maven
export M2_HOME="/opt/maven/apache-maven-3.3.9"
export MAVEN_OPTS="-Xmx4g -Djava.awt.headless=true -XX:ReservedCodeCacheSize=512m"
export M2=${M2_HOME}/bin
export PATH="${M2}:${PATH}"
```
----

### IDE Setup

#### IntelliJ
* import the maven project from the top level pom.xml

#### Eclipse
* coming soon

## Quick start
For those of you who want a quick start using the defaults, from the top-level:

`./start-splice-cluster`

This will compile everything and start the database. 

Then to connect to the database:

`./sqlshell.sh`

Or if you are on Windows:

`call sqlshell.cmd`
