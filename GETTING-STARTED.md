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
* [Java 1.7.0_x](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)
or
* [Java 1.8.0_x](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

Note: JDK 1.8 is required for the master branch.

* [Apache Maven 3.3.x (Jenkins uses 3.3.9)](https://maven.apache.org/download.cgi)
* [Google Protocol Buffers (source download)](https://protobuf.googlecode.com/files/protobuf-2.5.0.tar.bz2)

> **Instructions to compile protobufs**<br />
1. Download the proper version of protocol buffers<br />
`wget https://protobuf.googlecode.com/files/protobuf-2.5.0.tar.bz2`<br />
2. Untar the tar.bz2 file<br />
`tar xfvj protobuf-2.5.0.tar.bz2`<br />
3. Configure the protobuf.<br />
`cd protobuf-2.5.0`<br />
`./configure CC=clang CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g' LDFLAGS='-stdlib=libc++' LIBS="-lc++ -lc++abi"`<br />
4. Make the source<br />
`make -j 4`<br />
5. Install the compiled binaries<br />
`sudo make install`<br />

#### Helpful Environment Variables, etc. (example assumes Mac OS X developer machine)
```bash
# for java nonsense
export J8_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_31.jdk/Contents/Home"
export J7_HOME="/Library/Java/JavaVirtualMachines/jdk1.7.0_67.jdk/Contents/Home"
export J6_HOME="/System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home"
# export JAVA_HOME="$(/usr/libexec/java_home)"
export JAVA_HOME=${J7_HOME}
# for maven builds
export M2_HOME="/opt/maven/maven"
# export M2_HOME="/opt/maven/apache-maven-3.3.9"
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
