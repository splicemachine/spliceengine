#!/bin/bash

# Update hbase-env.sh on MapR cluster nodes.  Needs to be run on control node.

# Default to active user for SSH login
ssh_user=$(whoami)
# Node hostnames
nodes=()

show_help()
{
    echo "Update hbase-env.sh on MapR cluster nodes.  Needs to be run on control node."
    echo "Usage: $(basename $BASH_SOURCE) [-h] [-u <ssh login name>] [-i <ssh identity file>] [-n <node hostname1>] [-n <node hostname2>] [-n <node hostname3>] ..."
    echo -e "\t-h display this message"
    echo -e "\t-u ssh login username (default ${ssh_user})"
    echo -e "\t-i ssh identity file"
    echo -e "\t-n node hostname, repeat this option for each node (defaults to all nodes if none specified)"
}

# Process command line args
while getopts "hu:i:n:" opt; do
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

	u)
	    ssh_user=$OPTARG
	    ;;
	\?)
	    show_help
	    exit 1
	    ;;
    esac
done

# Some sanity checks
if [ ! -f /opt/mapr/server/data/nodelist.txt ] || [ ! -f /opt/mapr/hbase/hbaseversion ]; then
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
hbase_env_sh="hbase-env.sh"
hbase_version="$(cat /opt/mapr/hbase/hbaseversion)"
hbase_conf_dir="/opt/mapr/hbase/hbase-${hbase_version}/conf"
# Spark History Server
# THIS IS NOT RIGHT -AMM
# (blindly assuming it is the cldb host from where this scrpt is running and the port is 18088)
spark_history_server_hostname="$(hostname):18088"

# Assemble SSH options
common_ssh_opts="${ssh_identity} -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
# Adding -t option to work-around requiretty sudo option
ssh_opts="${common_ssh_opts} -t"
scp_opts="${common_ssh_opts}" 

for node in ${nodes[@]}; do
    echo "COPYING ${hbase_env_sh}.patch to ${ssh_user}@${node}"
    ssh ${ssh_opts} ${ssh_user}@${node} "mkdir -p splice-installer-temp"
    scp ${scp_opts} ${installer_dir}/conf/${hbase_env_sh}.patch ${ssh_user}@${node}:splice-installer-temp

    echo "PATCHING ${hbase_conf_dir}/${hbase_env_sh} on ${node}"
    # SSH commands to patch/modify hbase-site.xml on target node
    ssh_cmd="sudo patch -b -p0 ${hbase_conf_dir}/${hbase_env_sh} < ~/splice-installer-temp/${hbase_env_sh}.patch"
    ssh_cmd="${ssh_cmd} && sudo sed -e 's/SPARKHISTORYSERVER/${spark_history_server_hostname}/' -i ${hbase_conf_dir}/${hbase_env_sh}"
    ssh ${ssh_opts} ${ssh_user}@${node} "[ ! -f  ${hbase_conf_dir}/${hbase_env_sh}.orig ] && ${ssh_cmd}"

    # Clean up
    ssh ${ssh_opts} ${ssh_user}@${node} "rm -rf splice-installer-temp"

done
