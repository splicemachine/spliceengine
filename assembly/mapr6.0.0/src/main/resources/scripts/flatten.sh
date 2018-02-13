#!/bin/bash

# these are MapR install defaults
mapr_user="mapr"
mapr_pass="mapr"
mapr_web_port="8443"
mapr_scheme="https"
mapr_zk_ver="3.4.5"
mapr_zk_port="5181"

# generated from other values/programmatically 
mapr_host="$(hostname)"
mapr_url="${mapr_scheme}://${mapr_user}:${mapr_pass}@${mapr_host}:${mapr_web_port}"

# splice machine defaults
ssh_user="$(whoami)"
spark_enabled="false"
cluster_hosts=()
timeout=240

# curl options
curl_opts="-k -s --sslv3 --tlsv1"

show_help()
{
	cat <<- EOHELP
	Clean out HBase/Zookeeper data on MapR cluster nodes for a fresh Splice Machine.
	Assuming one cluster managed by MapR.
	Usage: $(basename $BASH_SOURCE) [-h] [-a <admin URL>] [-u <ssh login name>] [-i <ssh identity file>] [-n <node hostname1>] [-n <node hostname2>] [-n <node hostname3>] ...
	  -h display this message
	  -a MapR web UI base admin URL (default ${mapr_url})
	  -u ssh login username (default ${ssh_user})
	  -i ssh identity file
	  -n node hostname, repeat this option for each node (defaults to all nodes if none specified)
	EOHELP
}

# Process command line args
while getopts "ha:u:i:n:s:" opt; do
	case $opt in
	h)
	show_help
	exit 0
	;;
	a)
		mapr_url=$OPTARG
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

# we might get a new ${mapr_url} as a cmdline option so set our REST endpoint after
mapr_rest="${mapr_url}/rest"

# Assemble SSH options
ssh_opts="${ssh_identity} -q -t -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# Some sanity checks
for reqcmd in curl jq maprcli ; do
	command -v ${reqcmd} >/dev/null 2>&1 || {
		echo "This script requires the '${reqcmd}' command. Aborting." >&2
		exit 1
	}
done

check_mapr_master()
{
	# we should be a cldb, webserver and a zookeeper
	# XXX - might need to loosen this check up
	for i in cldb webserver zookeeper ; do
		test -e /opt/mapr/roles/${i} || {
			echo "This host does not appear to have a MapR ${i} role. Aborting." >&2
			exit 1
		}
	done

	# paranoia - check that we're a/the MapR CLDB
	# XXX - we may not be the CLDB master, but should be fine as long as we're a CLDB node
	curl ${curl_opts} ${mapr_rest}/node/listcldbs | jq -r ' . | .data | .[] | .CLDBs ' | grep -qi ^$(hostname)$ || {
		echo "This host does not appear to be a MapR CLDB. Aborting." >&2
		exit 1
	}

	# if we insist on being the CLDB master, one of these should work:
	#   sudo maprcli node cldbmaster | grep -i hostname: | tail -1 | awk -F: '{print $NF}' | tr -d ' ' | grep -qi "^`hostname`$"
	#   curl ${curl_opts} ${mapr_rest}/node/cldbmaster | jq -r ' . | .data | .[] | .cldbmaster ' | awk '{print $NF}'

	# canonical zk version - only use default above if not set
	test -e /opt/mapr/zookeeper/zookeeperversion && mapr_zk_ver=$(cat /opt/mapr/zookeeper/zookeeperversion)
}

get_cluster_name()
{
	# XXX - 'maprcli cluster ...' is only useful for gateway and mapreduce info
	local clustconf="/opt/mapr/conf/mapr-clusters.conf"
	test -e ${clustconf} || {
		echo "${clustconf} not found. Aborting." >&2
		exit 1
	}
	# XXX - this just returns the last cluster for which we're responsible, which is not even close to right
	grep -i `hostname` ${clustconf}  | tail -1 | awk '{print $1}'
}

get_cluster_hosts()
{
	# TODO this is wrong, it gets all of the hosts that MapR knows about,
	# not necessarily only the ones in this cluster.
	# for now they are the same, but not always.
	# can likely provide a filter given the cluster name
	curl ${curl_opts} ${mapr_rest}/node/list | jq -r ' . | .data | .[] | .hostname '
}

get_zookeeper_host()
{
	# just get one (the first) zk
	curl ${curl_opts} ${mapr_rest}/node/listzookeepers | jq -r ' . | .data | .[] | .Zookeepers ' | head -1 | cut -f1 -d:
}

get_zookeeper_port()
{
	local zkp=$(curl ${curl_opts} ${mapr_rest}/node/listzookeepers | jq -r ' . | .data | .[] | .Zookeepers ' | awk -F: "/^${1}:/ { print \$2 }")
	test -z "${zkp}" || mapr_zk_port="${zkp}"
	echo ${mapr_zk_port}
}

get_hbmaster_hosts()
{
	curl ${curl_opts} --data-urlencode 'filter=[csvc==hbmaster]' ${mapr_rest}/node/list | jq -r ' . | .data | .[] | .hostname '
}

get_hbregionserver_hosts()
{
	curl ${curl_opts} --data-urlencode 'filter=[csvc==hbregionserver]' ${mapr_rest}/node/list | jq -r ' . | .data | .[] | .hostname '
}

get_fileserver_hosts()
{
	curl ${curl_opts} --data-urlencode 'filter=[csvc==fileserver]' ${mapr_rest}/node/list | jq -r ' . | .data | .[] | .hostname '
}

stop_service()
{
	local service_name=${1}
	curl ${curl_opts} --data "name=${service_name}" --data 'action=stop' --data-urlencode "filter=[csvc==${service_name}]" ${mapr_rest}/node/services
    echo
}

start_service()
{
	local service_name=${1}
	curl ${curl_opts} --data "name=${service_name}" --data 'action=start' --data-urlencode "filter=[csvc==${service_name}]" ${mapr_rest}/node/services
    echo
}

wait_for_service_state()
{
	local service_name=${1}
	local expected_state=${2}
	local service_host=${3}
	local state=""
	while [ "${state}" != "${expected_state}" ]; do
		state=$(curl ${curl_opts} --data "node=${service_host}" ${mapr_rest}/service/list | jq -r " . | .data | .[] | if .name == \"${service_name}\" then .state else empty end ")
		if [ "${state}" != "${expected_state}" ] ; then
			sleep 5
		fi
	done
    echo
}

# XXX - CDH / HDP, needed on MapR?
wait_for_service_health()
{
	local service_name=${1}
	local expected_health=${2}
	local health=""
	while [ "${health}" != "${expected_health}" ]; do
		health=$(curl ${curl_opts} ${ambari_url}/clusters/${cluster_name}/services/${service_name}?fields=ServiceInfo/state | sed 's/[",]//g' | awk '/state/ && ! /http/ {print $3}')
		sleep 5
	done
    echo
}

clean_out_zookeeper()
{
	zkclient="/opt/mapr/zookeeper/zookeeper-${mapr_zk_ver}/bin/zkCli.sh"
	test -e ${zkclient} || {
		echo "${zkclient} not found. Aborting." >&2
	}
	${zkclient} -server ${zookeeper_host}:${zookeeper_port} <<< "
ls /
rmr /startupPath
rmr /spliceJobs
rmr /derbyPropertyPath
rmr /spliceTasks
rmr /hbase
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
	sudo -su ${mapr_user} hadoop fs -ls /hbase
	sudo -su ${mapr_user} hadoop fs -rm -r -f -skipTrash /hbase /BAD
	sudo -su ${mapr_user} hadoop fs -expunge
    echo
}

create_hbase_root_dir()
{
	echo "creating the HBASE_ROOT_DIR"
	sudo -su ${mapr_user} hadoop fs -mkdir -p /hbase/data /BAD
	sudo -su ${mapr_user} hadoop fs -chown -R ${mapr_user}:${mapr_user} /hbase /BAD
	sudo -su ${mapr_user} hadoop fs -chmod 755 /hbase /BAD
    echo
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
	for host in ${hbregionserver_hosts[@]} ; do
		echo -e "\nWaiting (up to" $timeout "seconds) for Splice Machine startup on ${host}"
		local count; count=0
		until ssh ${ssh_opts} ${ssh_user}@${host} "grep \"Ready to accept JDBC connections\" /opt/mapr/hbase/hbase-$(cat /opt/mapr/hbase/hbaseversion)/logs/*regionserver*"; do
			echo -e "waited ${count} seconds..."
			sleep 5
			count=$(( $count+5 ))
			if [ "${count}" -gt "${timeout}" ]; then
				echo -e "Waited for ${count} seconds, exiting"
				break
			fi
		done
	done
    echo
}

#############################################
# make sure we are running on a mapr master #
#############################################
check_mapr_master

# grab cluster name
cluster_name=$(get_cluster_name)

# get cluster_hosts if they are not provided on command line.
if [ "${cluster_hosts[0]}" = "" ]; then
	cluster_hosts=( $(get_cluster_hosts) )
fi

# get zk info programatically
zookeeper_host=$(get_zookeeper_host)
zookeeper_port=$(get_zookeeper_port ${zookeeper_host})

# get HBase master
hbmaster_hosts=( $(get_hbmaster_hosts) )

# get HBase RS nodes
hbregionserver_hosts=( $(get_hbregionserver_hosts) )

# get fileserver nodes
fileserver_hosts=( $(get_fileserver_hosts) )


#######################################--#######################################
#                                  STOP HBASE                                  #
#######################################--#######################################
echo "stopping hbase"
# mapr controls hbase master(s) and regionserver(s) separately
#   RS
stop_service hbregionserver
for hbrs in ${hbregionserver_hosts[@]} ; do
	wait_for_service_state hbregionserver 3 ${hbrs}
done
#   master
stop_service hbmaster 
for hbm in ${hbmaster_hosts[@]} ; do
	wait_for_service_state hbmaster 3 ${hbm}
done

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
	ssh ${ssh_opts} ${ssh_user}@${host} 'sudo rm -f /opt/mapr/hbase/hbase-$(cat /opt/mapr/hbase/hbaseversion)/logs/*log*'
	ssh ${ssh_opts} ${ssh_user}@${host} 'sudo rm -f /opt/mapr/hbase/hbase-$(cat /opt/mapr/hbase/hbaseversion)/logs/*out*'
done

#######################################--#######################################
#                                 START HBASE                                  #
#######################################--#######################################
# start up hbmasters/hbregionservers then wait for them to come up
start_service hbmaster
start_service hbregionserver
# master
for hbm in ${hbmaster_hosts[@]} ; do
	wait_for_service_state hbmaster 2 ${hbm}
done
# regionserver
for hbrs in ${hbregionserver_hosts[@]} ; do
	wait_for_service_state hbregionserver 2 ${hbrs}
done
check_hbase_ready

echo "done"
