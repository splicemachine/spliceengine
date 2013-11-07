SpliceSQL Engine
===

#Building 

mvn package -DskipTests=true 


#Deploying Snapshots

mvn -DskipTests clean javadoc:jar source:jar javadoc:test-jar source:test-jar deploy

#Deploy Release

 mvn -Darguments="-DskipTests" release:prepare
 
 mvn -Darguments="-DskipTests" release:perform

 Checkout Tag that was created and branch:

 git checkout -b branch_name tag_name_from_release_process

 git push master branch_name
 
 
Setting up HortonWorks for Splice Machine to work 

HDP-2.0-alpha

Download 
zookeeper-3.4.3.1.tar.gz
hbase-0.94.2.1.tar.gz
hadoop-2.0.2.1-alpha.tar.gz

For Hbase-0.94.2.1:

add these lines to the pom.xml and then type mvn -DskipTests deploy

-- > Distribution Site 
		<distributionManagement>
	<repository>
	<uniqueVersion>false</uniqueVersion>
	<id>structured-hbase-release</id>
	<name>Structured HBase Release</name>
	<url>scp://www.inciteretail.com/usr/local/sonatype-work/nexus/storage/releases/</url>
	</repository>
	
	<snapshotRepository>
	<uniqueVersion>true</uniqueVersion>
	<id>structured-hbase-snapshot</id>
	<name>Structured HBase Snapshot</name>
	<url>scp://www.inciteretail.com/usr/local/sonatype-work/nexus/storage/snapshots/</url>
	</snapshotRepository>
	</distributionManagement>

-- > Inside <build>...
	  <extensions>
      <!-- Enabling the use of FTP -->
      <extension>
        <groupId>org.apache.maven.wagon</groupId>
         <artifactId>wagon-ssh</artifactId>
         <version>1.0-beta-6</version>
      </extension>
    </extensions>
    
For Zookeeper 3.4.3.1
run ant package

put this pom.xml into base directory 

<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>
	<groupId>org.apache.zookeeper</groupId>
	<version>3.4.1.1</version>
	<packaging>pom</packaging>
	<artifactId>zookeeper</artifactId>
	<name>zookeeper</name>
			<distributionManagement>
	<repository>
	<uniqueVersion>false</uniqueVersion>
	<id>structured-hbase-release</id>
	<name>Structured HBase Release</name>
	<url>scp://www.inciteretail.com/usr/local/sonatype-work/nexus/storage/releases/</url>
	</repository>
	
	<snapshotRepository>
	<uniqueVersion>true</uniqueVersion>
	<id>structured-hbase-snapshot</id>
	<name>Structured HBase Snapshot</name>
	<url>scp://www.inciteretail.com/usr/local/sonatype-work/nexus/storage/snapshots/</url>
	</snapshotRepository>
	</distributionManagement>
	<build>
	  <extensions>
      <!-- Enabling the use of FTP -->
      <extension>
        <groupId>org.apache.maven.wagon</groupId>
         <artifactId>wagon-ssh</artifactId>
         <version>1.0-beta-6</version>
      </extension>
    </extensions>
	</build>

						
</project>

mvn deploy:deploy-file -Dfile=dist-maven/zookeeper-3.4.3.1.jar -Durl=scp://www.inciteretail.com/usr/local/sonatype-work/nexus/storage/releases/ -DgroupId=org.apache.zookeeper -DartifactId=zookeeper -Dversion=3.4.3.1 -Dpackaging=jar -DrepositoryId=inciteretail
mvn deploy:deploy-file -Dfile=dist-maven/zookeeper-3.4.3.1-sources.jar -Durl=scp://www.inciteretail.com/usr/local/sonatype-work/nexus/storage/releases/ -DgroupId=org.apache.zookeeper -DartifactId=zookeeper -Dversion=3.4.3.1 -Dpackaging=jar -Dclassifier=sources -DrepositoryId=inciteretail
mvn deploy:deploy-file -Dfile=dist-maven/zookeeper-3.4.3.1-javadoc.jar -Durl=scp://www.inciteretail.com/usr/local/sonatype-work/nexus/storage/releases/ -DgroupId=org.apache.zookeeper -DartifactId=zookeeper -Dversion=3.4.3.1 -Dpackaging=jar -Dclassifier=javadoc -DrepositoryId=inciteretail
mvn deploy:deploy-file -Dfile=dist-maven/zookeeper-3.4.3.1-tests.jar -Durl=scp://www.inciteretail.com/usr/local/sonatype-work/nexus/storage/releases/ -DgroupId=org.apache.zookeeper -DartifactId=zookeeper -Dversion=3.4.3.1 -Dpackaging=jar -Dclassifier=tests -DrepositoryId=inciteretail
mvn deploy:deploy-file -Dfile=dist-maven/zookeeper-3.4.3.1.pom -Durl=scp://www.inciteretail.com/usr/local/sonatype-work/nexus/storage/releases/ -DgroupId=org.apache.zookeeper -DartifactId=zookeeper -Dversion=3.4.3.1 -Dpackaging=pom -DrepositoryId=inciteretail

hadoop-2.0.2.1

-- TODO


