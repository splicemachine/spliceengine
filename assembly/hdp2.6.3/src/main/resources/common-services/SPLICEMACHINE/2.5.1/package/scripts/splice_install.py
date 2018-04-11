import sys
import sys
from resource_management.libraries.script.script import Script
from resource_management.core.resources import Directory
from resource_management.libraries.resources import XmlConfig
from resource_management import *

reload(sys)
sys.setdefaultencoding('utf8')

class SpliceInstall(Script):
  def install(self, env):
    import params
    self.install_packages(env)
    env.set_params(params)
    print 'Before Print';
    print(params.config)
    print 'Install the client';
    self.configure(env)

#    dir = '/var/lib/splicemachine'
#    if os.path.exists(dir):
#      shutil.rmtree(dir)
#    os.makedirs(dir)

  def configure(self, env):
    import params
    print 'Configure the client';
    hbase_user = params.config['configurations']['hbase-env']['hbase_user']
    user_group = params.config['configurations']['cluster-env']["user_group"]
    splicemachine_conf_dir = '/etc/splicemachine/conf'

    Directory( splicemachine_conf_dir,
               owner = hbase_user,
               group = user_group,
               create_parents = True
               )
    Directory( splicemachine_conf_dir,
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

def somethingcustom(self, env):
    print 'Something custom';

if __name__ == "__main__":
  SpliceInstall().execute()