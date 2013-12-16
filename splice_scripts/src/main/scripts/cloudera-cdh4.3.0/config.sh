#!/bin/bash

# Cloudera VM Splice Machine configurator
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv

echo "Configuring Splice Machine on $SPLICE_ENV"

curl -X PUT -H "Content-Type:application/json" -u admin:admin \
  -d '{ "roleTypeConfigs" : [ {
            "roleType" : "MASTER",
                "items": [
                    { "name": "hbase_coprocessor_master_classes", "value": "com.splicemachine.derby.hbase.SpliceMasterObserver" } ] }, {
            "roleType" : "REGIONSERVER",
                "items" : [
                    { "name": "hbase_coprocessor_region_classes", "value": "com.splicemachine.derby.hbase.SpliceOperationRegionObserver,com.splicemachine.derby.hbase.SpliceIndexObserver,com.splicemachine.derby.hbase.SpliceDerbyCoprocessor,com.splicemachine.derby.hbase.SpliceIndexManagementEndpoint,com.splicemachine.derby.hbase.SpliceIndexEndpoint,com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler,com.splicemachine.si.coprocessors.SIObserver" } ] } ] }' \
  'http://localhost:7180/api/v1/clusters/Cluster%201%20-%20CDH4/services/hbase1/config'

