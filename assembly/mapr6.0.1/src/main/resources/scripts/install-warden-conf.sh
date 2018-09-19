#!/bin/bash

# Update warden.conf on MapR cluster nodes.  Needs to be run on control node.

# Default to active user for SSH login
ssh_user=$(whoami)
# Node hostnames
nodes=()
# RegionServer heap size 24GB
hbregionheapsize='24576'
hbregionheappercent='50'
hbmasterheapsize='5120'
hbmasterheappercent='25'
retries='1'

show_help()
{
    echo "Update warden.conf on MapR cluster nodes.  Needs to be run on control node."
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

# Get full list of nodes if none specified.  Can't use maprcli, since warden may not be running.
if (( ${#nodes} == 0 )); then
    nodes="$( awk -F' ' '{print $1}' /opt/mapr/server/data/nodelist.txt )"
fi
# Installer location
installer_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )
# Warden properties
warden_conf="warden.conf"
mapr_conf_dir="/opt/mapr/conf"

# Assemble SSH options
#   -t option to work-around requiretty sudo option
ssh_opts="${ssh_identity} -t -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

for node in ${nodes[@]}; do
    echo "BACKING UP ${warden_conf} on ${ssh_user}@${node}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo cp ${mapr_conf_dir}/${warden_conf}{,.orig}"
    echo "UPDATING ${warden_conf} on ${ssh_user}@${node}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed -e 's/service.command.hbregion.heapsize.percent=.*/service.command.hbregion.heapsize.percent=${hbregionheappercent}/g' -i ${mapr_conf_dir}/${warden_conf}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed -e 's/service.command.hbregion.heapsize.max=.*/service.command.hbregion.heapsize.max=${hbregionheapsize}/g' -i ${mapr_conf_dir}/${warden_conf}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed -e 's/service.command.hbregion.heapsize.min=.*/service.command.hbregion.heapsize.min=${hbregionheapsize}/g' -i ${mapr_conf_dir}/${warden_conf}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed -e 's/service.command.hbmaster.heapsize.percent=.*/service.command.hbmaster.heapsize.percent=${hbmasterheappercent}/g' -i ${mapr_conf_dir}/${warden_conf}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed -e 's/service.command.hbmaster.heapsize.max=.*/service.command.hbmaster.heapsize.max=${hbmasterheapsize}/g' -i ${mapr_conf_dir}/${warden_conf}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed -e 's/service.command.hbmaster.heapsize.min=.*/service.command.hbmaster.heapsize.min=${hbmasterheapsize}/g' -i ${mapr_conf_dir}/${warden_conf}"
    ssh ${ssh_opts} ${ssh_user}@${node} "sudo sed -e 's/services.retries=.*/services.retries=${retries}/g' -i ${mapr_conf_dir}/${warden_conf}"
done
