#!/bin/bash

# XXX - this only works if HBase and YARN roles are co-located on the same system

# get an environmet
for rcfile in /etc/profile /etc/bashrc /etc/profile.d/*.sh ; do
  test -e ${rcfile} && source ${rcfile}
done

# XXX - if we don't have jps just look through the HBase/YARN dirs ...
#  HBase dirs:
#    sudo find /var/run/cloudera-scm-agent/process/ -maxdepth 1  \( -name \*-hbase-MASTER\* -o -name \*-hbase-REGIONSERVER\* \)
#  YARN dirs
#    sudo find /var/run/cloudera-scm-agent/process/ -maxdepth 1  \( -name \*--yarn-NODEMANAGER\* -o -name \*-yarn-RESOURCEMANAGER\* \)

# we need the "jps" utility to associate PIDs to service names
which jps >/dev/null 2>&1 || {
  echo "please make sure the 'jps' utility is available"
  exit 1
}
JPS=$(which jps)

# our YARN config file
YS="yarn-site.xml"

# our YARN Kerberos keytab
YK="yarn.keytab"

# PIDs for HBase Master/RegionServer and YARN NodeManager/ResourceManager
HMPID=$(sudo  ${JPS} | awk '/HMaster/{print $1}')
HRSPID=$(sudo ${JPS} | awk '/HRegionServer/{print $1}')
NMPID=$(sudo  ${JPS} | awk '/NodeManager/{print $1}')
RMPID=$(sudo  ${JPS} | awk '/ResourceManager/{print $1}')

# the Cloudera Manager agent sets up a service home directory under:
#   /var/run/cloudera-scm-agent/process
# we should be able to get to the CWD via the service's /proc/PID entry
HMHOME=$(sudo  readlink /proc/${HMPID}/cwd)
HRSHOME=$(sudo readlink /proc/${HRSPID}/cwd)
NMHOME=$(sudo  readlink /proc/${NMPID}/cwd)
RMHOME=$(sudo  readlink /proc/${RMPID}/cwd)

# for every HBase Master/RegionServer
for HD in ${HMHOME} ${HRSHOME} ; do
  # see if we have the target of the /proc/PID symlink
  sudo test -e ${HD} && {
    # if we do, see if there's a YARN config and/or keytab in there
    for YC in ${YS} ${YK} ; do
      sudo test -e ${HD}/${YC} || {
        # if not, copy *one* of the YARN NodeManager or ResourceManager's config files
        # prefer NodeManager since it's a "client"
        for YD in ${NMHOME} ${RMHOME} ; do
          sudo test -e ${YD} && {
              sudo test -e ${YD}/${YC} && {
              sudo cp ${YD}/${YC} ${HD}/
              sudo chown hbase:hbase ${HD}/${YC}
              break
            }
          }
        done
      }
      sudo test -e ${HD}/${YC} && sudo ls -la ${HD}/${YC}
    done
  }
done
