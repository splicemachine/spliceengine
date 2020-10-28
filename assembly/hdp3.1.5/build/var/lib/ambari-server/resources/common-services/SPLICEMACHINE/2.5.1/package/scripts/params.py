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
Ambari Agent
"""

import os
import re
from resource_management import *
from resource_management.libraries.functions.default import default
#from resource_management.libraries.functions.version import format_hdp_stack_version
from resource_management.libraries.script.script import Script

# server configurations
config = Script.get_config()

zookeeper_znode_parent = config['configurations']['hbase-site']['zookeeper.znode.parent']
hbase_zookeeper_quorum = config['configurations']['hbase-site']['hbase.zookeeper.quorum']

# detect spark queue
# if 'spark.yarn.queue' in config['configurations']['spark-defaults']:
#    spark_queue = config['configurations']['spark-defaults']['spark.yarn.queue']
#else:
#    spark_queue = 'default'

# e.g. 2.3
stack_version_unformatted = str(config['clusterLevelParams']['stack_version'])

# e.g. 2.3.0.0
#hdp_stack_version = format_hdp_stack_version(stack_version_unformatted)

# e.g. 2.3.0.0-2130
full_version = default("/commandParams/version", None)
hdp_version = full_version
