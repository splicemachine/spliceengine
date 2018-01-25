#!/usr/bin/env ambari-python-wrap
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import imp
import math
import os
import re
import socket
import traceback
import glob

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STACKS_DIR = os.path.join(SCRIPT_DIR, '../../../stacks/')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
  with open(PARENT_FILE, 'rb') as fp:
    service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
  traceback.print_exc()
  print "Failed to load parent"

class SPLICEMACHINE251ServiceAdvisor(service_advisor.ServiceAdvisor):

  def __init__(self, *args, **kwargs):
    self.as_super = super(SPLICEMACHINE251ServiceAdvisor, self)
    self.as_super.__init__(*args, **kwargs)

  def colocateService(self, hostsComponentsMap, serviceComponents):
      pass

  def getServiceComponentLayoutValidations(self, services, hosts):
      print "getServiceComponentLayoutValidations"
      return []


  def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
    #Update HBase Classpath
    print "getServiceConfigurationRecommendations",services
    splice_jars = ":".join([jar for jar in glob.glob('/var/lib/splicemachine/*.jar')])
    spark_jars = ":".join([jar for jar in glob.glob('/usr/hdp/2.6.3.0-235/spark2/jars/*.jar')])
    # Has a few libs that cause us to go sideways
    # master_jars = ":".join([jar for jar in glob.glob('/usr/hdp/current/hbase-master/lib/*.jar')])
    subsetMaster = "/usr/hdp/current/hbase-master/lib/joni-2.1.2.jar:/usr/hdp/current/hbase-master/lib/hbase-rsgroup-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/protobuf-java-2.5.0.jar:/usr/hdp/current/hbase-master/lib/commons-cli-1.2.jar:/usr/hdp/current/hbase-master/lib/jsr305-1.3.9.jar:/usr/hdp/current/hbase-master/lib/okhttp-2.4.0.jar:/usr/hdp/current/hbase-master/lib/jersey-client-1.9.jar:/usr/hdp/current/hbase-master/lib/hbase-spark.jar:/usr/hdp/current/hbase-master/lib/servlet-api-2.5.jar:/usr/hdp/current/hbase-master/lib/leveldbjni-all-1.8.jar:/usr/hdp/current/hbase-master/lib/jersey-server-1.9.jar:/usr/hdp/current/hbase-master/lib/aws-java-sdk-s3-1.10.6.jar:/usr/hdp/current/hbase-master/lib/disruptor-3.3.0.jar:/usr/hdp/current/hbase-master/lib/spymemcached-2.11.6.jar:/usr/hdp/current/hbase-master/lib/jaxb-impl-2.2.3-1.jar:/usr/hdp/current/hbase-master/lib/hbase-annotations-1.1.2.2.6.3.0-235-tests.jar:/usr/hdp/current/hbase-master/lib/hbase-it-1.1.2.2.6.3.0-235-tests.jar:/usr/hdp/current/hbase-master/lib/jetty-6.1.26.hwx.jar:/usr/hdp/current/hbase-master/lib/hbase-rest-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/joda-time-2.9.4.jar:/usr/hdp/current/hbase-master/lib/jsch-0.1.54.jar:/usr/hdp/current/hbase-master/lib/zookeeper.jar:/usr/hdp/current/hbase-master/lib/jsp-api-2.1-6.1.14.jar:/usr/hdp/current/hbase-master/lib/hbase-common-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/jettison-1.3.3.jar:/usr/hdp/current/hbase-master/lib/hbase-rsgroup.jar:/usr/hdp/current/hbase-master/lib/activation-1.1.jar:/usr/hdp/current/hbase-master/lib/hbase-hadoop-compat.jar:/usr/hdp/current/hbase-master/lib/hbase-server.jar:/usr/hdp/current/hbase-master/lib/ojdbc6.jar:/usr/hdp/current/hbase-master/lib/hbase-client-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/jackson-databind-2.2.3.jar:/usr/hdp/current/hbase-master/lib/hbase-server-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/netty-3.2.4.Final.jar:/usr/hdp/current/hbase-master/lib/aws-java-sdk-kms-1.10.6.jar:/usr/hdp/current/hbase-master/lib/hbase-resource-bundle.jar:/usr/hdp/current/hbase-master/lib/paranamer-2.3.jar:/usr/hdp/current/hbase-master/lib/jersey-json-1.9.jar:/usr/hdp/current/hbase-master/lib/jersey-guice-1.9.jar:/usr/hdp/current/hbase-master/lib/azure-storage-5.4.0.jar:/usr/hdp/current/hbase-master/lib/hbase-spark-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/findbugs-annotations-1.3.9-1.jar:/usr/hdp/current/hbase-master/lib/commons-configuration-1.6.jar:/usr/hdp/current/hbase-master/lib/aopalliance-1.0.jar:/usr/hdp/current/hbase-master/lib/hbase-shell.jar:/usr/hdp/current/hbase-master/lib/hbase-procedure.jar:/usr/hdp/current/hbase-master/lib/hbase-rsgroup-1.1.2.2.6.3.0-235-tests.jar:/usr/hdp/current/hbase-master/lib/commons-compress-1.4.1.jar:/usr/hdp/current/hbase-master/lib/hbase-prefix-tree-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/hbase-thrift-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/commons-collections-3.2.2.jar:/usr/hdp/current/hbase-master/lib/servlet-api-2.5-6.1.14.jar:/usr/hdp/current/hbase-master/lib/hbase-procedure-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/okio-1.4.0.jar:/usr/hdp/current/hbase-master/lib/httpcore-4.4.4.jar:/usr/hdp/current/hbase-master/lib/commons-codec-1.9.jar:/usr/hdp/current/hbase-master/lib/xercesImpl-2.9.1.jar:/usr/hdp/current/hbase-master/lib/jetty-sslengine-6.1.26.hwx.jar:/usr/hdp/current/hbase-master/lib/jackson-mapper-asl-1.9.13.jar:/usr/hdp/current/hbase-master/lib/commons-digester-1.8.jar:/usr/hdp/current/hbase-master/lib/commons-beanutils-1.7.0.jar:/usr/hdp/current/hbase-master/lib/hbase-annotations-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/xml-apis-1.3.04.jar:/usr/hdp/current/hbase-master/lib/jets3t-0.9.0.jar:/usr/hdp/current/hbase-master/lib/javax.inject-1.jar:/usr/hdp/current/hbase-master/lib/hbase-protocol.jar:/usr/hdp/current/hbase-master/lib/jamon-runtime-2.4.1.jar:/usr/hdp/current/hbase-master/lib/guice-3.0.jar:/usr/hdp/current/hbase-master/lib/netty-all-4.0.52.Final.jar:/usr/hdp/current/hbase-master/lib/hbase-it-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/jsp-2.1-6.1.14.jar:/usr/hdp/current/hbase-master/lib/curator-recipes-2.7.1.jar:/usr/hdp/current/hbase-master/lib/jackson-core-asl-1.9.13.jar:/usr/hdp/current/hbase-master/lib/guice-servlet-3.0.jar:/usr/hdp/current/hbase-master/lib/hbase-rest.jar:/usr/hdp/current/hbase-master/lib/jcodings-1.0.8.jar:/usr/hdp/current/hbase-master/lib/asm-3.1.jar:/usr/hdp/current/hbase-master/lib/hbase-examples.jar:/usr/hdp/current/hbase-master/lib/hbase-prefix-tree.jar:/usr/hdp/current/hbase-master/lib/api-util-1.0.0-M20.jar:/usr/hdp/current/hbase-master/lib/jackson-jaxrs-1.9.13.jar:/usr/hdp/current/hbase-master/lib/aws-java-sdk-core-1.10.6.jar:/usr/hdp/current/hbase-master/lib/junit-4.12.jar:/usr/hdp/current/hbase-master/lib/jackson-core-2.2.3.jar:/usr/hdp/current/hbase-master/lib/ranger-hbase-plugin-shim-0.7.0.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/java-xmlbuilder-0.4.jar:/usr/hdp/current/hbase-master/lib/hbase-annotations.jar:/usr/hdp/current/hbase-master/lib/commons-beanutils-core-1.8.0.jar:/usr/hdp/current/hbase-master/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/usr/hdp/current/hbase-master/lib/commons-io-2.4.jar:/usr/hdp/current/hbase-master/lib/hbase-protocol-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/guava-12.0.1.jar:/usr/hdp/current/hbase-master/lib/hbase-thrift.jar:/usr/hdp/current/hbase-master/lib/avro-1.7.6.jar:/usr/hdp/current/hbase-master/lib/hbase-client.jar:/usr/hdp/current/hbase-master/lib/jaxb-api-2.2.2.jar:/usr/hdp/current/hbase-master/lib/hbase-examples-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/curator-client-2.7.1.jar:/usr/hdp/current/hbase-master/lib/ranger-plugin-classloader-0.7.0.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/api-asn1-api-1.0.0-M20.jar:/usr/hdp/current/hbase-master/lib/hbase-common.jar:/usr/hdp/current/hbase-master/lib/json-smart-1.1.1.jar:/usr/hdp/current/hbase-master/lib/jasper-runtime-5.5.23.jar:/usr/hdp/current/hbase-master/lib/htrace-core-3.1.0-incubating.jar:/usr/hdp/current/hbase-master/lib/libthrift-0.9.3.jar:/usr/hdp/current/hbase-master/lib/xz-1.0.jar:/usr/hdp/current/hbase-master/lib/apacheds-i18n-2.0.0-M15.jar:/usr/hdp/current/hbase-master/lib/azure-keyvault-core-0.8.0.jar:/usr/hdp/current/hbase-master/lib/jetty-util-6.1.26.hwx.jar:/usr/hdp/current/hbase-master/lib/hbase-hadoop2-compat.jar:/usr/hdp/current/hbase-master/lib/commons-daemon-1.0.13.jar:/usr/hdp/current/hbase-master/lib/commons-math3-3.1.1.jar:/usr/hdp/current/hbase-master/lib/commons-lang3-3.5.jar:/usr/hdp/current/hbase-master/lib/commons-math-2.2.jar:/usr/hdp/current/hbase-master/lib/commons-net-3.1.jar:/usr/hdp/current/hbase-master/lib/jersey-core-1.9.jar:/usr/hdp/current/hbase-master/lib/xmlenc-0.52.jar:/usr/hdp/current/hbase-master/lib/snappy-java-1.0.5.jar:/usr/hdp/current/hbase-master/lib/hbase-shell-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/nimbus-jose-jwt-3.9.jar:/usr/hdp/current/hbase-master/lib/hbase-server-1.1.2.2.6.3.0-235-tests.jar:/usr/hdp/current/hbase-master/lib/hbase-resource-bundle-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/hbase-hadoop-compat-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/metrics-core-2.2.0.jar:/usr/hdp/current/hbase-master/lib/jackson-xc-1.9.13.jar:/usr/hdp/current/hbase-master/lib/log4j-1.2.17.jar:/usr/hdp/current/hbase-master/lib/hbase-it.jar:/usr/hdp/current/hbase-master/lib/jcip-annotations-1.0.jar:/usr/hdp/current/hbase-master/lib/jackson-annotations-2.2.3.jar:/usr/hdp/current/hbase-master/lib/commons-logging-1.2.jar:/usr/hdp/current/hbase-master/lib/hbase-common-1.1.2.2.6.3.0-235-tests.jar:/usr/hdp/current/hbase-master/lib/curator-framework-2.7.1.jar:/usr/hdp/current/hbase-master/lib/jasper-compiler-5.5.23.jar:/usr/hdp/current/hbase-master/lib/commons-lang-2.6.jar:/usr/hdp/current/hbase-master/lib/httpclient-4.5.2.jar:/usr/hdp/current/hbase-master/lib/hbase-hadoop2-compat-1.1.2.2.6.3.0-235.jar:/usr/hdp/current/hbase-master/lib/commons-el-1.0.jar:/usr/hdp/current/hbase-master/lib/gson-2.2.4.jar"
    executor_extraclasspath = "/usr/hdp/current/hbase-regionserver/conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar:" + splice_jars + ":" + subsetMaster
    splice_spark_jars = splice_jars + ":" + subsetMaster
    if "hbase-env" in services["configurations"]:
        hbase_env = services["configurations"]["hbase-env"]["properties"]
        if "content" in hbase_env:
          content = hbase_env["content"]
          HBASE_CLASSPATH_PREFIX = "export HBASE_CLASSPATH_PREFIX="+ splice_jars + ":" + spark_jars
          HBASE_MASTER_OPS = "export HBASE_MASTER_OPS=\"${HBASE_MASTER_OPS} -D"+ " -D".join(self.getMasterDashDProperties()) + \
              " -Dsplice.spark.executor.extraClassPath=" + executor_extraclasspath + \
              " -Dsplice.spark.jars=" + splice_spark_jars + "\""
          if "splicemachine" not in content:
            print "Updating Hbase Env Items"
            HBASE_CLASSPATH_PREFIX = "#Add Splice Jars to HBASE_PREFIX_CLASSPATH\n" + HBASE_CLASSPATH_PREFIX
            HBASE_MASTER_OPS = "Add Splice Specific Information to HBase Master\n" + HBASE_MASTER_OPS
            content = "\n\n".join((content, HBASE_CLASSPATH_PREFIX))
            content = "\n\n".join((content, HBASE_MASTER_OPS))
            print "content: " + content
            putHbaseEnvProperty = self.putProperty(configurations, "hbase-env", services)
            putHbaseEnvProperty("content", content)

    # Update HDFS properties in core-site
    if "core-site" in services["configurations"]:
      core_site = services["configurations"]["core-site"]["properties"]
      putCoreSiteProperty = self.putProperty(configurations, "core-site", services)

      for property, desired_value in self.getCoreSiteDesiredValues().iteritems():
        if property not in core_site or core_site[property] != desired_value:
          putCoreSiteProperty(property, desired_value)

    # Update hbase-site properties in hbase-site
    if "hbase-site" in services["configurations"]:
      hbase_site = services["configurations"]["hbase-site"]["properties"]
      putHbaseSitePropertyAttributes = self.putPropertyAttribute(configurations, "hbase-site")
      putHBaseSiteProperty = self.putProperty(configurations, "hbase-site", services)
      for property, desired_value in self.getHBaseSiteDesiredValues().iteritems():
        if property not in hbase_site or hbase_site[property] != desired_value:
          putHBaseSiteProperty(property, desired_value)

    # Update spark-defaults for spark
    if "spark2-env" in services["configurations"]:
        spark2_env = services['configurations']['spark2-env']["properties"]
        splice_driver_lib = "export spark.driver.extraLibraryPath=${spark.driver.extraLibraryPath}:" + splice_jars + "\n"
        splice_executor_lib = "export spark.executor.extraLibraryPath=${spark.executor.extraLibraryPath}:" + splice_jars + "\n"
        splice_path = "\n".join((splice_driver_lib,splice_executor_lib))
        if 'content' in spark2_env:
          spark_content = spark2_env["content"]
          if "splicemachine" not in spark_content:
            spark_content = "\n\n".join((spark_content,splice_path))
            putSparkProperty = self.putProperty(configurations, "spark2-env", services)
            putSparkProperty("content",spark_content)

   #update zookeeper configs
    if 'zoo.cfg' in services['configurations']:
      zoo_cfg = services['configurations']['zoo.cfg']["properties"]
      print(zoo_cfg),zoo_cfg
      putZooProperty = self.putProperty(configurations, "zoo.cfg", services)
      putZooProperty('maxClientCnxns',0)
      putZooProperty('maxSessionTimeout',120000)

  def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
      print "getServiceConfigurationsValidationItems"
      return []
#    print "getServiceConfigurationsValidationItems"
    # validate recommended properties in core-site
#    siteName = "core-site"
#    method = self.validateCoreSiteConfigurations
#    items = self.validateConfigurationsForSite(configurations, recommendedDefaults, services, hosts, siteName, method)

#    siteName = "hdfs-site"
#    method = self.validateHDFSSiteConfigurations
#    resultItems = self.validateConfigurationsForSite(configurations, recommendedDefaults, services, hosts, siteName, method)
#    items.extend(resultItems)

#    siteName = "hbase-site"
#    method = self.validateHBaseSiteConfigurations
#    resultItems = self.validateConfigurationsForSite(configurations, recommendedDefaults, services, hosts, siteName, method)
#    items.extend(resultItems)

  def validateCoreSiteConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
    print "validateCoreSiteConfigurations"
    core_site = properties
    validationItems = []
    for property, desired_value in self.getCoreSiteDesiredValues().iteritems():
      if property not in core_site or core_site[property] != desired_value:
        message = "Splice Machine requires this property to be set to the recommended value of " + desired_value
        validationItems.append({"config-name": property, "item": self.getWarnItem(message)})
    return self.toConfigurationValidationProblems(validationItems, "core-site")

  def validateHDFSSiteConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
      print "validateHDFSSiteConfigurations"
      hdfs_site = properties
      validationItems = []
      for property, desired_value in self.getHDFSSiteDesiredValues().iteritems():
          if property not in hdfs_site or hdfs_site[property] != desired_value:
              message = "Splice Machine requires this property to be set to the recommended value of " + desired_value
              validationItems.append({"config-name": property, "item": self.getWarnItem(message)})
      return self.toConfigurationValidationProblems(validationItems, "hdfs-site")

  def validateHBaseSiteConfigurations(self, properties, recommendedDefaults, configurations, services, hosts):
      print "validateHBaseSiteConfigurations"
      hbase_site = properties
      validationItems = []
      for property, desired_value in self.getHBaseSiteDesiredValues().iteritems():
          print "->" + property + ":" + desired_value + ":" + hbase_site[property]
          if property not in hbase_site or hbase_site[property] != desired_value:
              message = "Splice Machine requires this property to be set to the recommended value of " + desired_value
              validationItems.append({"config-name": property, "item": self.getWarnItem(message)})
      return self.toConfigurationValidationProblems(validationItems, "hbase-site")

  def getCoreSiteDesiredValues(self):
    core_site_desired_values = {
        "ipc.server.listen.queue.size" : "3300"
    }
    return core_site_desired_values

  def getHDFSSiteDesiredValues(self):
    hdfs_site_desired_values = {
        "dfs.datanode.handler.count" : "20",
        "dfs.client.read.shortcircuit.buffer.size" : "131072",

    }
    return hdfs_site_desired_values

  def getHBaseSiteDesiredValues(self):
    hbase_site_desired_values = {
        "hbase.coprocessor.master.classes" : "org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor,com.splicemachine.hbase.SpliceMasterObserver",
        "hbase.regionserver.global.memstore.size" : "0.25",
        "hfile.block.cache.size" : "0.25",
        "hbase.regionserver.handler.count" : "200",
        "hbase.client.scanner.caching" : "1000",
        "hbase.hstore.blockingStoreFiles" : "20",
        "hbase.hstore.compactionThreshold" : "5",
        "hbase.balancer.period" : "60000",
        "hbase.client.ipc.pool.size" : "10",
        "hbase.client.max.perregion.tasks" : "100",
        "hbase.coprocessor.regionserver.classes" : "org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor,com.splicemachine.hbase.RegionServerLifecycleObserver",
        "hbase.hstore.compaction.max.size" : "260046848",
        "hbase.hstore.compaction.min.size" : "16777216",
        "hbase.hstore.compaction.min" : "5",
        "hbase.hstore.defaultengine.compactionpolicy" : "com.splicemachine.compactions.SpliceDefaultCompactionPolicy",
        "hbase.hstore.defaultengine.compactor" : "com.splicemachine.compactions.SpliceDefaultCompactor",
        "hbase.coprocessor.region.classes" : "org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor,com.splicemachine.hbase.MemstoreAwareObserver,com.splicemachine.derby.hbase.SpliceIndexObserver,com.splicemachine.derby.hbase.SpliceIndexEndpoint,com.splicemachine.hbase.RegionSizeEndpoint,com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint,com.splicemachine.si.data.hbase.coprocessor.SIObserver,com.splicemachine.hbase.BackupEndpointObserver",
        "hbase.htable.threads.max" : "96",
        "hbase.ipc.warn.response.size" : "-1",
        "hbase.ipc.warn.response.time" : "-1",
        "hbase.master.loadbalance.bytable" : "true",
        "hbase.htable.threads.max" : "96",
        "hbase.regions.slop" : "0.01",
        "hbase.regionserver.global.memstore.size.lower.limit" : "0.9",
        "hbase.client.scanner.timeout.period" : "1200000",
        "hbase.regionserver.maxlogs" : "48",
        "hbase.regionserver.thread.compaction.large" : "1",
        "hbase.regionserver.thread.compaction.small" : "4",
        "hbase.regionserver.wal.enablecompression" : "true",
        "hbase.rowlock.wait.duration" : "0",
        "hbase.splitlog.manager.timeout" : "3000",
        "hbase.status.multicast.port" : "16100",
        "hbase.wal.disruptor.batch" : "true",
        "hbase.wal.provider" : "multiwal",
        "hbase.wal.regiongrouping.numgroups" : "16",
        "hbase.zookeeper.property.tickTime" : "6000",
        "hfile.block.bloom.cacheonwrite" : "TRUE",
        "io.storefile.bloom.error.rate" : "0.005",
        "splice.authentication.native.algorithm" : "SHA-512",
        "splice.authentication" : "NATIVE",
        "splice.client.numConnections" : "1",
        "splice.client.write.maxDependentWrites" : "60000",
        "splice.client.write.maxIndependentWrites" : "60000",
        "splice.compression" : "snappy",
        "splice.marshal.kryoPoolSize" : "1100",
        "splice.olap_server.clientWaitTime" : "900000",
        "splice.ring.bufferSize" : "131072",
        "splice.splitBlockSize" : "67108864",
        "splice.timestamp_server.clientWaitTime" : "120000",
        "splice.txn.activeTxns.cacheSize" : "10240",
        "splice.txn.completedTxns.concurrency" : "128",
        "splice.txn.concurrencyLevel" : "4096"
    }
    return hbase_site_desired_values

  def getMasterDashDProperties(self):
    dashDProperties = [
        "splice.spark.yarn.maxAppAttempts=1",
        "splice.spark.driver.maxResultSize=1g",
        "splice.spark.driver.cores=2",
        "splice.spark.yarn.am.memory=1g",
        "splice.spark.dynamicAllocation.enabled=true",
        "splice.spark.dynamicAllocation.executorIdleTimeout=120",
        "splice.spark.dynamicAllocation.cachedExecutorIdleTimeout=120",
        "splice.spark.dynamicAllocation.minExecutors=0",
        "splice.spark.kryo.referenceTracking=false"
        "splice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator",
        "splice.spark.kryoserializer.buffer.max=512m",
        "splice.spark.kryoserializer.buffer=4m",
        "splice.spark.locality.wait=100",
        "splice.spark.memory.fraction=0.5",
        "splice.spark.scheduler.mode=FAIR"
        "splice.spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "splice.spark.shuffle.compress=false",
        "splice.spark.shuffle.file.buffer=128k",
        "splice.spark.shuffle.service.enabled=true",
        "splice.spark.reducer.maxReqSizeShuffleToMem=134217728",
        "splice.spark.yarn.am.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native",
        "splice.spark.yarn.am.waitTime=10s",
        "splice.spark.yarn.executor.memoryOverhead=2048",
        "splice.spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/etc/spark/conf/log4j.properties",
        "splice.spark.driver.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native",
        "splice.spark.driver.extraClassPath=/usr/hdp/current/hbase-regionserver/conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar",
        "splice.spark.executor.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native",
        "splice.spark.ui.retainedJobs=100",
        "splice.spark.ui.retainedStages=100",
        "splice.spark.worker.ui.retainedExecutors=100",
        "splice.spark.worker.ui.retainedDrivers=100",
        "splice.spark.streaming.ui.retainedBatches=100",
        "splice.spark.executor.cores=4",
        "splice.spark.executor.memory=8g",
        "spark.compaction.reserved.slots=4",
        "splice.spark.eventLog.enabled=true",
        "splice.spark.eventLog.dir=hdfs:///user/splice/history",
        "splice.spark.local.dir=/tmp",
        "splice.spark.executor.userClassPathFirst=true",
        "splice.spark.driver.userClassPathFirst=true",
        "splice.spark.executor.extraJavaOptions=-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl -Djavax.xml.parsers.SAXParserFactory=com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl"
      ]
    return dashDProperties

