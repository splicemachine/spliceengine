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
    if "hbase-env" in services["configurations"]:
        hbase_env = services["configurations"]["hbase-env"]["properties"]
        if "content" in hbase_env:
          content = hbase_env["content"]
          HBASE_CLASSPATH_PREFIX = "export HBASE_CLASSPATH_PREFIX=/var/lib/splicemachine/*:/usr/hdp/2.6.4.0-91/spark2/jars/*:/usr/hdp/2.6.4.0-91/hadoop/lib/ranger-hdfs-plugin-impl/*"
          HBASE_MASTER_OPTS = "export HBASE_MASTER_OPTS=\"${HBASE_MASTER_OPTS} -D"+ " -D".join(self.getMasterDashDProperties()) + "\""
          HBASE_REGIONSERVER_OPTS = "export HBASE_REGIONSERVER_OPTS=\"${HBASE_REGIONSERVER_OPTS} -D"+ " -D".join(self.getRegionServerDashDProperties()) + "\""
          HBASE_CONF_DIR = "export HBASE_CONF_DIR=${HBASE_CONF_DIR}:/etc/splicemachine/conf/"

          if "splicemachine" not in content:
            print "Updating Hbase Env Items"
            HBASE_CLASSPATH_PREFIX = "#Add Splice Jars to HBASE_PREFIX_CLASSPATH\n" + HBASE_CLASSPATH_PREFIX
            HBASE_MASTER_OPTS = "#Add Splice Specific Information to HBase Master\n" + HBASE_MASTER_OPTS
            HBASE_REGIONSERVER_OPTS = "#Add Splice Specific Information to Region Server\n" + HBASE_REGIONSERVER_OPTS
            HBASE_CONF_DIR = "#Add Splice Specific Information to Region Server\n" + HBASE_CONF_DIR
            content = "\n\n".join((content, HBASE_CLASSPATH_PREFIX))
            content = "\n\n".join((content, HBASE_MASTER_OPTS))
            content = "\n\n".join((content, HBASE_REGIONSERVER_OPTS))
            content = "\n\n".join((content, HBASE_CONF_DIR))
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

    # Update hbase-site properties in hbase-site
    if "yarn-site" in services["configurations"]:
        yarn_site = services["configurations"]["yarn-site"]["properties"]
        putYarnSitePropertyAttributes = self.putPropertyAttribute(configurations, "yarn-site")
        putYarnSiteProperty = self.putProperty(configurations, "yarn-site", services)
        for property, desired_value in self.getYarnSiteDesiredValues().iteritems():
            if property not in yarn_site or yarn_site[property] != desired_value:
                putYarnSiteProperty(property, desired_value)

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

  def getYarnSiteDesiredValues(self):
      yarn_site_desired_values = {
          "hdp.version" : "2.6.4.0-91"

      }
      return yarn_site_desired_values

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
        "hbase.coprocessor.regionserver.classes" : "org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor,com.splicemachine.hbase.RegionServerLifecycleObserver,com.splicemachine.si.data.hbase.coprocessor.SpliceRSRpcServices",
        "hbase.hstore.compaction.min.size" : "136314880",
        "hbase.hstore.compaction.min" : "3",
        "hbase.hstore.defaultengine.compactionpolicy.class" : "com.splicemachine.compactions.SpliceDefaultCompactionPolicy",
        "hbase.hstore.defaultengine.compactor.class" : "com.splicemachine.compactions.SpliceDefaultCompactor",
        "hbase.coprocessor.region.classes" : "org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor,com.splicemachine.hbase.MemstoreAwareObserver,com.splicemachine.derby.hbase.SpliceIndexObserver,com.splicemachine.derby.hbase.SpliceIndexEndpoint,com.splicemachine.hbase.RegionSizeEndpoint,com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint,com.splicemachine.si.data.hbase.coprocessor.SIObserver,com.splicemachine.hbase.BackupEndpointObserver",
        "hbase.htable.threads.max" : "96",
        "hbase.ipc.warn.response.size" : "-1",
        "hbase.ipc.warn.response.time" : "-1",
        "hbase.master.loadbalance.bytable" : "true",
        "hbase.master.balancer.stochastic.regionCountCost" : "1500",
        "hbase.regions.slop" : "0",
        "hbase.regionserver.global.memstore.size.lower.limit" : "0.9",
        "hbase.client.scanner.timeout.period" : "1200000",
        "hbase.regionserver.maxlogs" : "48",
        "hbase.regionserver.thread.compaction.large" : "4",
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
        "splice.txn.concurrencyLevel" : "4096",
        "splice.olap_server.memory" : "8192",
        "splice.olap_server.memoryOverhead" : "2048",
        "splice.olap_server.virtualCores" : "2",
        "splice.authorization.scheme" : "NATIVE"
    }
    return hbase_site_desired_values

  def getMasterDashDProperties(self):
    dashDProperties = [
        "splice.spark.enabled=true",
        "splice.spark.app.name=SpliceMachine",
        "splice.spark.master=yarn",
        "splice.spark.submit.deployMode=client",
        "splice.spark.logConf=true",
        "splice.spark.yarn.maxAppAttempts=1",
        "splice.spark.driver.maxResultSize=1g",
        "splice.spark.driver.cores=2",
        "splice.spark.yarn.am.memory=1g",
        "splice.spark.dynamicAllocation.enabled=true",
        "splice.spark.dynamicAllocation.executorIdleTimeout=120",
        "splice.spark.dynamicAllocation.cachedExecutorIdleTimeout=120",
        "splice.spark.dynamicAllocation.minExecutors=0",
        "splice.spark.kryo.referenceTracking=false",
        "splice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator",
        "splice.spark.kryoserializer.buffer.max=512m",
        "splice.spark.kryoserializer.buffer=4m",
        "splice.spark.locality.wait=0",
        "splice.spark.memory.fraction=0.5",
        "splice.spark.scheduler.mode=FAIR",
        "splice.spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "splice.spark.shuffle.compress=false",
        "splice.spark.shuffle.file.buffer=128k",
        "splice.spark.shuffle.service.enabled=true",
        "splice.spark.reducer.maxReqSizeShuffleToMem=134217728",
        "splice.spark.yarn.am.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native",
        "splice.spark.yarn.am.waitTime=10s",
        "splice.spark.yarn.executor.memoryOverhead=2048",
        "splice.spark.yarn.am.extraJavaOptions=-Dhdp.version=2.6.4.0-91",
        "splice.spark.driver.extraJavaOptions=-Dhdp.version=2.6.4.0-91",
        "splice.spark.driver.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native",
        "splice.spark.driver.extraClassPath=/usr/hdp/current/hbase-regionserver/conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar",
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
        "splice.spark.executor.extraJavaOptions=-Dhdp.version=2.6.4.0-91",
        "splice.spark.executor.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native",
        "splice.spark.executor.extraClassPath=/usr/hdp/current/hbase-regionserver/conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar:/var/lib/splicemachine/*:/usr/hdp/2.6.4.0-91/spark2/jars/*:/usr/hdp/current/hbase-master/lib/*",
        "splice.spark.yarn.jars=/usr/hdp/2.6.4.0-91/spark2/jars/*"
      ]
    return dashDProperties

  def getRegionServerDashDProperties(self):
    dashDProperties = [
          "com.sun.management.jmxremote.authenticate=false",
          "com.sun.management.jmxremote.ssl=false",
          "com.sun.management.jmxremote.port=10102"
      ]
    return dashDProperties
