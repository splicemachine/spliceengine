#!/bin/bash

# Deploy Splice Machine jars to MapR cluster nodes.  Needs to be run on control node.

# Default to active user for SSH login
ssh_user=$(whoami)
# Node hostnames
nodes=()

show_help()
{
    echo "Deploy Splice Machine jars to MapR cluster nodes.  Needs to be run on control node."
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
installer_dir=$( dirname "${BASH_SOURCE[0]}" )
# HBase properties
hbase_version="$(cat /opt/mapr/hbase/hbaseversion)"
hbase_lib_dir="/opt/mapr/hbase/hbase-${hbase_version}/lib"
# Jar filenames
splice_jar=$(basename ${installer_dir}/resources/jars/splice_machine-*complete.jar)

# Assemble SSH options
common_ssh_opts="${ssh_identity} -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
scp_opts="${common_ssh_opts} -r"
# Adding -t option to work-around requiretty sudo option; doesn't work with scp
ssh_opts="${common_ssh_opts} -t"

for node in ${nodes[@]}; do
    
    echo "COPYING ${splice_jar} to ${ssh_user}@${node}"
    ssh ${ssh_opts} ${ssh_user}@${node} "mkdir -p splice-installer-temp"
    scp ${scp_opts} ${installer_dir}/resources/jars/${splice_jar} ${ssh_user}@${node}:splice-installer-temp
    scp ${scp_opts} ${installer_dir}/resources/scripts/sqlshell.sh ${ssh_user}@${node}:.

    echo "INSTALLING ${splice_jar} to ${hbase_lib_dir} on ${node}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo rm -rf ${hbase_lib_dir}/splice_machine-*complete.jar" # Remove old splice jar (in case version #s changed)
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo cp splice-installer-temp/${splice_jar} ${hbase_lib_dir}"

    # Clean up
    ssh ${ssh_opts} ${ssh_user}@${node} "rm -rf splice-installer-temp"

done
