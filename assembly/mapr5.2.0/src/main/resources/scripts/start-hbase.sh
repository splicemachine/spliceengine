#!/bin/bash

# Start HBase services on MapR cluster nodes.  Needs to be run on control node.

# Node hostnames
nodes=()

show_help()
{
    echo "Start HBase services on MapR cluster nodes.  Needs to be run on control node."
    echo "Usage: $(basename $BASH_SOURCE) [-h] [-n <node hostname1>] [-n <node hostname2>] [-n <node hostname3>] ..."
    echo -e "\t-h display this message"
    echo -e "\t-n node hostname, repeat this option for each node (defaults to all nodes if none specified)"
}

# Process command line args
while getopts "hn:" opt; do
    case $opt in
	h)
	    show_help
	    exit 0
	    ;;
	n)
	    nodes+=( ${OPTARG} )
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

# Get full list of nodes if none specified
if (( ${#nodes} == 0 )); then
    nodes="$( sudo maprcli node list -output terse -columns hn | tail -n +2 | awk '{print $1}' )"
fi

# Assemble hostname filter
mapr_hnfilter=''
for node in ${nodes[@]}; do
    mapr_hnfilter="[hn==${node}]or${mapr_hnfilter}"
done
mapr_hnfilter="$(echo -n ${mapr_hnfilter:0:${#mapr_hnfilter}-2})"

# Start all HBase RegionServers and then Masters
echo "STARTING all HBase Masters"
sudo maprcli node services -hbmaster start -filter "${mapr_hnfilter}and[csvc==hbmaster]"
echo "STARTING all HBase RegionServers"
sudo maprcli node services -hbregionserver start -filter "${mapr_hnfilter}and[csvc==hbregionserver]"
