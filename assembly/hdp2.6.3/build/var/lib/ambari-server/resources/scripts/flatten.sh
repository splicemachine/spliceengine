#!/bin/bash

ambari_host="localhost"
ambari_port="8080"
ambari_user="admin"
ambari_pass="admin"
ambari_api_ver="api/v1"
ssh_user=$(whoami)
cluster_hosts=()
timeout=240

show_help()
{
	echo "Deploy Splice Machine jars to Hortonworks cluster nodes. Assuming one cluster managed by Ambari."
	echo "Usage: $(basename $BASH_SOURCE) [-h] [-a <admin URL>] [-u <ssh login name>] [-i <ssh identity file>] [-n <node hostname1>] [-n <node hostname2>] [-n <node hostname3>] ..."
	echo -e "\t-h display this message"
	echo -e "\t-a Ambari URL of the form http://user:password@hostname:port (default ${admin_url} )"
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
		ambari_host=$OPTARG
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
ambari_url="http://${ambari_user}:${ambari_pass}@${ambari_host}:${ambari_port}/${ambari_api_ver}"
# Assemble SSH options
ssh_opts="${ssh_identity} -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

get_cluster_name()
{
	curl -s ${ambari_url}/clusters?fields=Clusters/cluster_name | sed 's/[",]//g' | awk '/cluster_name/ && ! /http/ {print $3}'
}

get_cluster_hosts()
{
	# TODO this is wrong, it gets all of th ehosts that CM knows about,
	# not necessarily only the ones in this cluster.  for now they are
	# the same, but not always
	curl -s ${ambari_url}/hosts?fields=Hosts/host_name | sed 's/[",]//g' | awk '/host_name/ && ! /http/ {print $3}'
}

# get cluster_hosts if they are not provided on command line.
cluster_name=$(get_cluster_name)
if [ "${cluster_hosts[0]}" = "" ]; then
	cluster_hosts=( $(get_cluster_hosts) )
fi

get_zookeeper_host()
{
	curl -s ${ambari_url}/clusters/${cluster_name}/services/ZOOKEEPER/components/ZOOKEEPER_SERVER?fields=host_components/HostRoles/host_name | sed 's/[",]//g' | awk '/host_name/ && ! /http/ {print $3}'
}

zookeeper_host=$(get_zookeeper_host)
zookeeper_port="2181"

get_regionserver_hosts()
{
	curl -s ${ambari_url}/clusters/${cluster_name}/services/HBASE/components/HBASE_REGIONSERVER?fields=host_components/HostRoles/host_name | sed 's/[",]//g' | awk '/host_name/ && ! /http/ {print $3}'
}

regionserver_hosts=( $(get_regionserver_hosts) )

stop_service()
{
	local service_name=${1}
	curl -i -H 'X-Requested-By: ambari' -X PUT -d "{\"RequestInfo\": {\"context\" :\"Stop ${service_name} via REST\"}, \"Body\": {\"ServiceInfo\": {\"state\": \"INSTALLED\"}}}" ${ambari_url}/clusters/${cluster_name}/services/${service_name}
}

start_service()
{
	local service_name=${1}
	curl -i -H 'X-Requested-By: ambari' -X PUT -d "{\"RequestInfo\": {\"context\" :\"Start ${service_name} via REST\"}, \"Body\": {\"ServiceInfo\": {\"state\": \"STARTED\"}}}"  ${ambari_url}/clusters/${cluster_name}/services/${service_name} 
}

wait_for_service_state()
{
	local service_name=${1}
	local expected_state=${2}
	local state=""
	while [ "${state}" != "${expected_state}" ]; do
		state=$(curl -s ${ambari_url}/clusters/${cluster_name}/services/${service_name}?fields=ServiceInfo/state | sed 's/[",]//g' | awk '/state/ && ! /http/ {print $3}')
		sleep 5
	done
}

wait_for_service_health()
{
	local service_name=${1}
	local expected_health=${2}
	local health=""
	while [ "${health}" != "${expected_health}" ]; do
		health=$(curl -s ${ambari_url}/clusters/${cluster_name}/services/${service_name}?fields=ServiceInfo/state | sed 's/[",]//g' | awk '/state/ && ! /http/ {print $3}')
		sleep 5
	done
}

clean_out_zookeeper()
{
	zookeeper-client -server ${zookeeper_host}:${zookeeper_port} <<< "
ls /
rmr /startupPath
rmr /spliceJobs
rmr /derbyPropertyPath
rmr /spliceTasks
rmr /hbase-unsecure
rmr /conglomerates
rmr /transactions
rmr /ddl
rmr /splice
ls /
quit
"
echo
}

delete_hbase_root_dir()
{
	echo "deleting the HBASE_ROOT_DIR"
	sudo -su hdfs hadoop fs -ls /apps/hbase
	sudo -su hdfs hadoop fs -rm -r -f -skipTrash /apps/hbase /BAD
	sudo -su hdfs hadoop fs -expunge
}

create_hbase_root_dir()
{
	echo "creating the HBASE_ROOT_DIR"
	sudo -su hdfs hadoop fs -mkdir -p /apps/hbase/data /BAD
	sudo -su hdfs hadoop fs -chown -R hbase:hdfs /apps/hbase /BAD
	sudo -su hdfs hadoop fs -chmod 755 /apps/hbase /BAD
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
		until ssh -t ${ssh_opts} ${ssh_user}@${host} "grep \"Ready to accept JDBC connections\" /var/log/hbase/*regionserver*"; do 
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


#######################################--#######################################
#                                  STOP HBASE                                  #
#######################################--#######################################
echo "stopping hbase"
stop_service HBASE
wait_for_service_state HBASE INSTALLED

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
#                              CLEAN HBASE LOG DIR                             #
#######################################--#######################################
echo "cleaning logs"
for host in ${cluster_hosts[@]} ; do
	ssh -t ${ssh_opts} ${ssh_user}@${host} 'sudo rm /var/log/hbase/*log* /var/log/hbase/*out*'
done

#######################################--#######################################
#                                 START HBASE                                  #
#######################################--#######################################
echo "starting hbase"
start_service HBASE
wait_for_service_state HBASE STARTED
check_hbase_ready

echo "done"
