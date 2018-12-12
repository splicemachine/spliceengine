#!/bin/bash

# Update yarn-site.xml on MapR cluster nodes.  Needs to be run on control node.

# Default to active user for SSH login
ssh_user=$(whoami)

# Node hostnames
nodes=()

show_help()
{
    echo "Update yarn-site.xml on MapR cluster nodes.  Needs to be run on control node."
    echo "Usage: $(basename $BASH_SOURCE) [-h] [-u <ssh login name>] [-i <ssh identity file>] [-c <number cores>] [-p <zookeeper port>] [-n <node hostname1>] [-n <node hostname2>] [-n <node hostname3>] ..."
    echo -e "\t-h display this message"
    echo -e "\t-u ssh login username (default ${ssh_user})"
    echo -e "\t-i ssh identity file"
    echo -e "\t-p Zoozeeper port (default ${zk_port})"
    echo -e "\t-n node hostname, repeat this option for each node (defaults to all nodes if none specified)"
}

# Process command line args
while getopts "hc:u:i:n:p:" opt; do
    case $opt in
	h)
	    show_help
	    exit 0
	    ;;
	i)
	    ssh_identity="-i ${OPTARG}"
	    ;;
	n)
	    nodes+=( ${OPTARG} )
	    ;;
	p)
	    zk_port=${OPTARG}
	    ;;
	u)
	    ssh_user=${OPTARG}
	    ;;
	\?)
	    show_help
	    exit 1
	    ;;
    esac
done

# Some sanity checks
if [ ! -f /opt/mapr/server/data/nodelist.txt ] || [ ! -f /opt/mapr/hadoop/hadoopversion ]; then
    echo "This script needs to be run on a fully installed MapR control node."
    exit 1
fi

# Get full list of nodes if none specified.  Can't use maprcli, since warden may not be running.
if (( ${#nodes} == 0 )); then
    nodes="$( awk -F' ' '{print $1}' /opt/mapr/server/data/nodelist.txt )"
fi
# Installer location
installer_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )
# HBase properties
yarn_site_xml="yarn-site.xml"
hbase_version="$(cat /opt/mapr/hbase/hbaseversion)"
hadoop_version="$(cat /opt/mapr/hadoop/hadoopversion)"
hadoop_conf_dir="/opt/mapr/hadoop/hadoop-${hadoop_version}/etc/hadoop"
hbase_lib_dir="/opt/mapr/hbase/hbase-${hbase_version}/lib"
yarn_lib_dir="/opt/mapr/hadoop/hadoop-${hadoop_version}/share/hadoop/yarn"

# Assemble SSH options
common_ssh_opts="${ssh_identity} -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
# Adding -t option to work-around requiretty sudo option
ssh_opts="${common_ssh_opts} -t"
scp_opts="${common_ssh_opts}"

for node in ${nodes[@]}; do
    
    echo "COPYING ${yarn_site_xml}.patch to ${ssh_user}@${node}"
    ssh ${ssh_opts} ${ssh_user}@${node} "mkdir -p splice-installer-temp"
    scp ${scp_opts} ${installer_dir}/conf/${yarn_site_xml}.patch ${ssh_user}@${node}:splice-installer-temp
 
    echo "PATCHING ${hadoop_conf_dir}/${yarn_site_xml} on ${node}"
    # SSH commands to patch/modify yarn-site.xml on target node
    ssh_cmd="sudo patch -b -p0 ${hadoop_conf_dir}/${yarn_site_xml} < ~/splice-installer-temp/${yarn_site_xml}.patch"
    ssh ${ssh_opts} ${ssh_user}@${node} "[ ! -f  ${hadoop_conf_dir}/${yarn_site_xml}.orig ] && ${ssh_cmd}"
    
    # Clean up
    ssh ${ssh_opts} ${ssh_user}@${node} "rm -rf splice-installer-temp"

done
