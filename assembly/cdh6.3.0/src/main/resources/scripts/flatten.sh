#!/bin/bash

cm_host="localhost"
cm_port="7180"
cm_user="admin"
cm_pass="admin"
cm_api_ver="api/v33"
ssh_user=$(whoami)
cluster_hosts=()
timeout=240

show_help()
{
	echo "Re-deploy Splice Machine jars to Cloudera CDH cluster nodes. Assuming one cluster on Cloudera Manager."
	echo "Usage: $(basename $BASH_SOURCE) [-h] [-a <admin URL>] [-u <ssh login name>] [-i <ssh identity file>] [-n <node hostname1>] [-n <node hostname2>] [-n <node hostname3>] ..."
	echo -e "\t-h display this message"
	echo -e "\t-a Cloudera Manager URL of the form http://user:password@hostname:port (default ${admin_url} )"
	echo -e "\t-u ssh login username (default ${ssh_user})"
	echo -e "\t-i ssh identity file"
	echo -e "\t-n node hostname, repeat this option for each node (defaults to all nodes if none specified)"
}

# Process command line args
while getopts "ha:u:i:n:s:" opt; do
	case $opt in
	h)
	show_help
	exit 0
	;;
	a)
		cm_host=$OPTARG
	;;
	i)
		ssh_identity="-i ${OPTARG}"
	;;
	n)
		cluster_hosts+=( ${OPTARG} )
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
command -v curl >/dev/null 2>&1 || { echo "This script requires curl.  Aborting." >&2; exit 1; }

# Assemble CM API URL
cm_url="http://${cm_user}:${cm_pass}@${cm_host}:${cm_port}/${cm_api_ver}"
# Assemble SSH options
ssh_opts="${ssh_identity} -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

get_cluster_name()
{
	curl -s ${cm_url}/clusters | sed 's/[",]//g' | awk '/name/ {print $3}'
}

get_cluster_hosts()
{
	# TODO this is wrong, it gets all of th ehosts that CM knows about,
	# not necessarily only the ones in this cluster.  for now they are
	# the same, but not always
	curl -s ${cm_url}/hosts | sed 's/[",]//g' | awk '/name/ {print $3}'
}

# get cluster_hosts if they are not provided on command line.
cluster_name=$(get_cluster_name)
if [ "${cluster_hosts[0]}" = "" ]; then
	cluster_hosts=( $(get_cluster_hosts) )
fi

get_zookeeper_host()
{
	local zookeeper_host_id=$(curl -s ${cm_url}/clusters/${cluster_name}/services/zookeeper/roles | sed 's/[",]//g' | awk '/hostId/ {print $3}')
	curl -s ${cm_url}/hosts/${zookeeper_host_id} | sed 's/[",]//g' | awk '/hostname/ {print $3}'
}

zookeeper_host=$(get_zookeeper_host)
zookeeper_port="2181"

get_regionserver_hosts()
{
	local regionserver_host_ids=( $(curl -s ${cm_url}/clusters/${cluster_name}/services/hbase/roles | sed 's/[",]//g' | awk '/name/ && ! /http/ && /hbase-REGIONSERVER/ {print $3}') )
	for regionserver_host_id in ${regionserver_host_ids[@]}; do
		local hostid=$(curl -s ${cm_url}/clusters/${cluster_name}/services/hbase/roles/${regionserver_host_id} | sed 's/[",]//g' | awk '/hostId/ && ! /http/ {print $3}')
		curl -s ${cm_url}/hosts/${hostid} | sed 's/[",]//g' | awk '/hostname/ && ! /http/ {print $3}'
	done
}

get_hmaster_hosts()
{
	local hmaster_host_ids=( $(curl -s ${cm_url}/clusters/${cluster_name}/services/hbase/roles | sed 's/[",]//g' | awk '/name/ && ! /http/ && /hbase-MASTER/ {print $3}') )
	for hmaster_host_id in ${hmaster_host_ids[@]}; do
		local hostid=$(curl -s ${cm_url}/clusters/${cluster_name}/services/hbase/roles/${hmaster_host_id} | sed 's/[",]//g' | awk '/hostId/ && ! /http/ {print $3}')
		curl -s ${cm_url}/hosts/${hostid} | sed 's/[",]//g' | awk '/hostname/ && ! /http/ {print $3}'
	done
}

regionserver_hosts=( $(get_regionserver_hosts) )
hmaster_hosts=( $(get_hmaster_hosts) )

stop_service()
{
	local service_name=${1}
	curl -s -X POST ${cm_url}/clusters/${cluster_name}/services/${service_name}/commands/stop
}

start_service()
{
	local service_name=${1}
	curl -s -X POST ${cm_url}/clusters/${cluster_name}/services/${service_name}/commands/start
}

wait_for_service_state()
{
	local service_name=${1}
	local expected_state=${2}
	local state=""
	while [ "${state}" != "${expected_state}" ]; do
		state=$(curl -s ${cm_url}/clusters/${cluster_name}/services/${service_name} | sed 's/[",]//g' | awk '/serviceState/ {print $3}')
		sleep 5
	done
}

wait_for_service_health()
{
	local service_name=${1}
	local expected_health=${2}
	local health=""
	while [ "${health}" != "${expected_health}" ]; do
		health=$(curl -s ${cm_url}/clusters/${cluster_name}/services/${service_name} | sed 's/[",]//g' | awk '/healthSummary/ {print $3}')
		sleep 5
	done
}

clean_out_zookeeper()
{
	zookeeper-client -server ${zookeeper_host}:${zookeeper_port} <<< "
ls /
deleteall /splice
deleteall /hbase
ls /
quit
"
echo
}

delete_hbase_root_dir()
{
	echo "deleting the HBASE_ROOT_DIR"
	sudo -su hdfs hadoop fs -ls /hbase
	sudo -su hdfs hadoop fs -rm -r -f -skipTrash /hbase /BAD
	sudo -su hdfs hadoop fs -expunge
}

create_hbase_root_dir()
{
	echo "creating the HBASE_ROOT_DIR"
	sudo -su hdfs hadoop fs -mkdir -p /hbase /BAD
	sudo -su hdfs hadoop fs -chown -R hbase:hbase /hbase /BAD
	sudo -su hdfs hadoop fs -chmod 755 /hbase /BAD

	# -AMM- this seemed flaky so we now do it manually
	# curl -s -X POST ${cm_url}/clusters/${cluster_name}/services/hbase/commands/hbaseCreateRoot
}

set_spark_dfs_perms() {
	sudo -su hdfs hadoop fs -mkdir -p /user/splice/history
	sudo -su hdfs hadoop fs -chmod 1777 /user /user/splice/history
	sudo -su hdfs hadoop fs -chmod 777 /user/splice
	id spark >/dev/null && {
		sudo -su hdfs hadoop fs -chown spark /user/splice /user/splice/history
	}
}

check_hbase_ready()
{
	for host in ${regionserver_hosts[@]} ; do
		echo -e "\nWaiting (up to" $timeout "seconds) for Splice Machine startup on ${host}"
		local count; count=0
		until ssh -t ${ssh_opts} ${ssh_user}@${host} "grep \"Ready to accept JDBC connections\" /var/log/hbase/*REGION*"; do
			echo -e "waited ${count} seconds..."
			sleep 5
			count=$(( $count+5 ))
			if [ "${count}" -gt "${timeout}" ]; then
				echo -e "Waited for ${count} seconds, exiting"
				break
			fi
		done
	done
}

install_yarn_site()
{
	for host in ${regionserver_hosts[@]} ${hmaster_hosts[@]} ; do
		ssh -t ${ssh_opts} ${ssh_user}@${host} "sudo /opt/cloudera/parcels/CDH/bin/copy-yarn-site.sh"
	done

}

#######################################--#######################################
#                                  STOP HBASE                                  #
#######################################--#######################################
echo "stopping hbase"
stop_service hbase
wait_for_service_state hbase STOPPED

#######################################--#######################################
#                            CLEAN OUT ZOOKEEPEER                              #
#######################################--#######################################
echo "cleaning out zookeeper"
clean_out_zookeeper

sleep 60

#######################################--#######################################
#                       REMOVE/CREATE HBASE ROOT DIR                           #
#######################################--#######################################
echo "refreshing hbase root dir"
delete_hbase_root_dir
create_hbase_root_dir
sleep 20

#######################################--#######################################
#                      SETUP SPARK DIRECTORIES IN DFS                          #
#######################################--#######################################
echo "creating and setting perms on spark applicationHistory"
set_spark_dfs_perms

#######################################--#######################################
#                              CLEAN HBASE LOG DIR                             #
#######################################--#######################################
for host in ${cluster_hosts[@]} ; do
	echo "cleaning hbase logs"
	ssh -t ${ssh_opts} ${ssh_user}@${host} 'sudo rm /var/log/hbase/*log* /var/log/hbase/*out*'
	echo "cleaning yarn application logs"
	ssh -t ${ssh_opts} ${ssh_user}@${host} 'sudo find /var/log/hadoop-yarn/ -type d -name application_\* -exec rm -rf {} \;'
done

#######################################--#######################################
#                                 START HBASE                                  #
#######################################--#######################################
echo "starting hbase"
start_service hbase
wait_for_service_state hbase STARTED
check_hbase_ready

echo "done"
