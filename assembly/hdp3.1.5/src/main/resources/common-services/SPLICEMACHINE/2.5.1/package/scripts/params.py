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
from resource_management.libraries.functions import format
from resource_management.libraries.functions.default import default
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions import conf_select
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources

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
full_version = default("/clusterLevelParams/version", None)
hdp_version = full_version
stack_root = Script.get_stack_root()

hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
hdfs_principal_name = config['configurations']['hadoop-env']['hdfs_principal_name']
hadoop_bin_dir = stack_select.get_hadoop_dir("bin")
hadoop_conf_dir = conf_select.get_hadoop_conf_dir()
security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
dfs_type = default("/clusterLevelParams/dfs_type", "")
splice_pid_file = "/tmp/splice-ambari-master.pid"


import functools
#create partial functions with common arguments for every HdfsResource call
#to create/delete hdfs directory/file/copyfromlocal we need to call params.HdfsResource in code
HdfsResource = functools.partial(
        HdfsResource,
        user=hdfs_user,
        hdfs_resource_ignore_file = "/var/lib/ambari-agent/data/.hdfs_resource_ignore",
        security_enabled = security_enabled,
        keytab = hdfs_user_keytab,
        kinit_path_local = kinit_path_local,
        hadoop_bin_dir = hadoop_bin_dir,
        hadoop_conf_dir = hadoop_conf_dir,
        principal_name = hdfs_principal_name,
        hdfs_site = hdfs_site,
        default_fs = default_fs,
        immutable_paths = get_not_managed_resources(),
        dfs_type = dfs_type
)
