import sys, os, fnmatch
from resource_management.libraries.script.script import Script
from resource_management.core.resources import Directory
from resource_management.core.resources.system import Execute, Link
from resource_management.libraries.resources import XmlConfig
from resource_management.libraries.functions import format
from resource_management import *
from urlparse import urlparse


reload(sys)
sys.setdefaultencoding('utf8')

class SpliceInstall(Script):
  def install(self, env):
    import params
    self.install_packages(env)
    env.set_params(params)
    self.configure(env)

  def configure(self, env):
    import params
    print 'Configure the client'
    hbase_user = params.config['configurations']['hbase-env']['hbase_user']
    user_group = params.config['configurations']['cluster-env']["user_group"]
    splicemachine_conf_dir = '/etc/splicemachine/conf'
    hdfs_audit_spool = params.config['configurations']['ranger-splicemachine-audit']['xasecure.audit.destination.hdfs.batch.filespool.dir']
    solr_audit_spool = params.config['configurations']['ranger-splicemachine-audit']['xasecure.audit.destination.solr.batch.filespool.dir']
    policy_cache_dir = params.config['configurations']['ranger-splicemachine-security']['ranger.plugin.splicemachine.policy.cache.dir']
    hdfs_audit_dir = params.config['configurations']['ranger-splicemachine-audit']['xasecure.audit.destination.hdfs.dir']

    Directory( splicemachine_conf_dir,
               owner = hbase_user,
               group = user_group,
               create_parents = True
               )

    Directory( hdfs_audit_spool,
               owner = hbase_user,
               group = user_group,
               create_parents = True
               )

    Directory( solr_audit_spool,
               owner = hbase_user,
               group = user_group,
               create_parents = True
               )

    Directory( policy_cache_dir,
               owner = hbase_user,
               group = user_group,
               create_parents = True
               )

    XmlConfig( "ranger-splicemachine-security.xml",
               conf_dir = splicemachine_conf_dir,
               configurations = params.config['configurations']['ranger-splicemachine-security'],
               configuration_attributes=params.config['configuration_attributes']['ranger-splicemachine-security'],
               owner = hbase_user,
               group = user_group,
               )
    XmlConfig( "ranger-splicemachine-audit.xml",
               conf_dir = splicemachine_conf_dir,
               configurations = params.config['configurations']['ranger-splicemachine-audit'],
               configuration_attributes=params.config['configuration_attributes']['ranger-splicemachine-audit'],
               owner = hbase_user,
               group = user_group,
               )


if __name__ == "__main__":
  SpliceInstall().execute()
