#!/bin/bash

# Update zoo.cfg on MapR cluster nodes. Needs to be run on control node.

# Default to active user for SSH login
ssh_user=$(whoami)
# Node hostnames
nodes=()
# RegionServer heap size 8GByte
heapsize='8192'

show_help()
{
    echo "Update zoo.cfg on MapR cluster nodes. Needs to be run on control node."
    echo "Usage: $(basename $BASH_SOURCE) [-h] [-u <ssh login name>] [-i <ssh identity file>] [-s <heap size>] [-n <node hostname1>] [-n <node hostname2>] [-n <node hostname3>] ..."
    echo -e "\t-h display this message"
    echo -e "\t-u ssh login username (default ${ssh_user})"
    echo -e "\t-i ssh identity file"
    echo -e "\t-s RegionServer heap size (default ${heapsize})"
    echo -e "\t-n node hostname, repeat this option for each node (defaults to all nodes if none specified)"
}

# Process command line args
while getopts "hu:i:n:s:" opt; do
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

	s)
	    heapsize=$OPTARG
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
if [ ! -f /opt/mapr/server/data/nodelist.txt ] ; then
    echo "This script needs to be run on a fully installed MapR control node."
    exit 1
fi

# Assemble SSH options
ssh_opts="-t ${ssh_identity} -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=QUIET"

# Get full list of nodes if none specified
if (( ${#nodes} == 0 )); then
    nodes="$( awk -F' ' '{print $1}' /opt/mapr/server/data/nodelist.txt )"
fi
# Installer location
installer_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )
# zookeeper properties
zoo_cfg="zoo.cfg"
zk_basedir="/opt/mapr/zookeeper"

for node in ${nodes[@]}; do
    ssh ${ssh_opts} ${ssh_user}@${node} "test -e ${zk_basedir} && test -e ${zk_basedir}/zookeeperversion" || continue
    zk_ver=$(ssh ${ssh_opts} ${ssh_user}@${node} "cat ${zk_basedir}/zookeeperversion" | tr -d \\r)
    zk_conf_dir="${zk_basedir}/zookeeper-${zk_ver}/conf"
    ssh ${ssh_opts} ${ssh_user}@${node} "test -e ${zk_conf_dir}/${zoo_cfg}" || continue
    echo "UPDATING ${zoo_cfg} on ${ssh_user}@${node}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed -e 's/maxClientCnxns=.*/maxClientCnxns=0/g' -i.orig ${zk_conf_dir}/${zoo_cfg}"
    # we may not have maxSessionTimeout - kill the line if we do, then (re)append
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed '/^maxSessionTimeout=/d' -i.orig ${zk_conf_dir}/${zoo_cfg}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo su - root -c 'echo maxSessionTimeout=120000 >> ${zk_conf_dir}/${zoo_cfg}'"
done
