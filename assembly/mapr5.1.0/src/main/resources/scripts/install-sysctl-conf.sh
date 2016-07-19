#!/bin/bash

# Update /etc/sysctl.conf on MapR cluster nodes.
# Needs to be run on control node.
# Limits ephemeral port range to <50k for MR1 and HBase
# Default on both RHEL 6.x and Ubuntu LTS 12 is 32,768 - 61,000

# Default to active user for SSH login
ssh_user=$(whoami)
# Node hostnames
nodes=()

# file we'll be modifying
sysctl='/etc/sysctl.conf'
# start and end port values
sport='32768'
eport='49999'

show_help()
{
    echo "Limit ephemeral port range in ${sysctl} on MapR cluster nodes. Needs to be run on control node."
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

# Timestamp
TS="$(date '+%Y%m%d%H%M%S')"

for node in ${nodes[@]}; do
    echo "BACKING UP ${sysctl} ON ${ssh_user}@${node}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo cp -f ${sysctl} ${sysctl}.PRE-${TS}"
    echo "UPDATING ${sysctl} ON ${ssh_user}@${node}"
    # we may not or may not have a net.ipv4.ip_local_port_range setting - kill the line if we do, then (re)append
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed -i '/net.ipv4.ip_local_port_range/d' ${sysctl}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo su - root -c 'echo net.ipv4.ip_local_port_range = ${sport} ${eport} >> ${sysctl}'"
    ssh ${ssh_opts} ${ssh_user}@${node} 'sudo sysctl -p | grep -i "net\.ipv4\.ip_local_port_range" | sed s#^#$(hostname -s):#g'
done
