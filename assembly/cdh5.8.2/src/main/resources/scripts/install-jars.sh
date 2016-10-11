#!/bin/bash

# Re-deploy Splice Machine jars to Cloudera CDH cluster nodes. Assuming one cluster on Cloudera Manager.

# Note this scripts only attempts to replace existing HBase, Zookeeper, Splice jars under CDH4 and CDH5.
# It assumes a Splice parcel has already been distributed to all nodes, and that the new jars to be installed
# have the same CDH mini-version and HBase version as the existing jars.

# Default to active user for SSH login
ssh_user=$(whoami)
# Default URL for Cloudera Manager UI
admin_url='http://admin:admin@localhost:7180'
# Node hostnames
nodes=()

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
while getopts "ha:u:i:n:" opt; do
    case $opt in
	h)
	    show_help
	    exit 0
	    ;;
	a)
	    if [[ ${OPTARG} != http* ]] ; then
		echo "Please include protocol in the Ambari admin URL, i.e. http or https."
		show_help
		exit 1
	    else
		admin_url=$OPTARG
	    fi
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
command -v curl >/dev/null 2>&1 || { echo "This script requires curl.  Aborting." >&2; exit 1; }

# Assuming this manager has only one cluster, determine whether CDH4 or CDH5
cdh_ver=$(curl -s ${admin_url}/api/v1/clusters | grep -m 1 version | awk 'BEGIN{FS="\""}{print $4;}')
if [ ! "${cdh_ver}" ] ; then
    echo "Aborting. Can't retrieve CDH version from Cloudera Manager URL at URL ${admin_url}" >&2
    exit 1
fi

# Get full list of nodes if none specified
if (( ${#nodes} == 0 )); then
    nodes=( $(curl -s ${admin_url}/api/v1/hosts | grep hostname | awk 'BEGIN{FS="\""}{print $4;}') )
    if [ ! "${nodes}" ] ; then
	echo "Aborting. Can't retrieve hostnames from Cloudera Manager URL at URL ${admin_url}" >&2
	exit 1
    fi
fi

# Assemble SSH options
ssh_opts="${ssh_identity} -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# Installer location
installer_dir=$( dirname "${BASH_SOURCE[0]}" )

# Destinations to copy HBase/Oozie/Zookeeper jars
parcel_dir='/opt/cloudera/parcels/CDH'
hbase_dir="${parcel_dir}/lib/hbase"
oozie_dir_cdh4="${parcel_dir}/lib/oozie/libserver"
zookeeper_dirs_cdh4=(
    "${parcel_dir}/lib/flume-ng/lib"
    "${parcel_dir}/lib/hadoop-0.20-mapreduce/lib"
    "${parcel_dir}/lib/hadoop-hdfs/lib"
    "${parcel_dir}/lib/hadoop-httpfs/webapps/webhdfs/WEB-INF/lib"
    "${parcel_dir}/lib/hcatalog/share/webhcat/svr/lib"
    "${parcel_dir}/lib/mahout/lib"
    "${parcel_dir}/lib/oozie/libtools"
    "${parcel_dir}/lib/zookeeper"
)

# CDH5 jar filename globs, assuming these will uniquely match jars in installer resources/jars and in parcel dir.
# Matching jars ending in "-tests.jar" will be ignored.
hbase_jars_cdh5=(
    "hbase-client-*.jar"
    "hbase-common-*.jar"
    "hbase-examples-*.jar"
    "hbase-hadoop-compat-*.jar"
    "hbase-hadoop2-compat-*.jar"
    "hbase-it-*.jar"
    "hbase-prefix-tree-*.jar"
    "hbase-protocol-*.jar"
    "hbase-server-*.jar"
    "hbase-shell-*.jar"
    "hbase-testing-util-*.jar"
    "hbase-thrift-*.jar"
)

# Next, glean the jar filesnames, etc, in the active parcel and those distributed in this installer
splice_jar=$(basename ${installer_dir}/resources/jars/splice_machine-*complete.jar)
case $cdh_ver in
    "CDH4")
	hbase_security_jar_cdh4=$( exec ssh ${ssh_opts} ${ssh_user}@${nodes[0]} "basename ${hbase_dir}/hbase-*-security.jar" )
	hbase_oozie_jar_cdh4=$( exec ssh ${ssh_opts} ${ssh_user}@${nodes[0]} "basename ${oozie_dir_cdh4}/hbase-*.jar" )
	zookeeper_jar_cdh4=$( exec ssh ${ssh_opts} ${ssh_user}@${nodes[0]} "basename ${zookeeper_dirs_cdh4[0]}/zookeeper-*.jar" )
	if [[ "${hbase_security_jar_cdh4}" =~ "*" ]] || [[ "${hbase_oozie_jar_cdh4}" =~ "*" ]] || [[ "${zookeeper_jar_cdh4}" =~ "*" ]] ; then
	    echo "Aborting. Can't find existing CDH4 jar(s) on ${nodes[0]} under path ${parcel_dir}." >&2
	    exit 1
	fi
	#Locate HBase jar in installer that isn't the tests jar
	hbase_splice_jar_cdh4="$(find ${installer_dir}/resources/jars/hbase-*.jar ! -name *-tests.jar -exec basename {} \; | head -n 1)"
	;;
    "CDH5")
	# Don't need to determine exact jar filenames for CDH5, will simply overwrite them below.  But do verify their existence.
	hbase_jar_cdh5=$( exec ssh ${ssh_opts} ${ssh_user}@${nodes[0]} "basename ${hbase_dir}/lib/${hbase_jars_cdh5[0]}" ) 
	if [[ "${hbase_jar_cdh5}" =~ "*" ]] ; then
	    echo "Aborting. Can't find existing CDH5 jar(s) on ${nodes[0]} under path ${parcel_dir}." >&2
	    exit 1
	fi
	;;
esac

for node in ${nodes[@]}; do
    
    # Using ssh -t option to work-around requiretty sudo option
    echo "COPYING jars to ${ssh_user}@${node}"
    ssh -t ${ssh_opts} ${ssh_user}@${node} "mkdir -p splice-installer-temp"
    scp ${ssh_opts} -r ${installer_dir}/resources/jars/*.jar ${ssh_user}@${node}:splice-installer-temp

    echo "INSTALLING jars on ${ssh_user}@${node}"
    ssh -t ${ssh_opts} ${ssh_user}@${node} "sudo rm -rf ${hbase_dir}/lib/splice_machine-*complete.jar" # Remove old splice jar (in case version #s changed)
    ssh -t ${ssh_opts} ${ssh_user}@${node} "sudo cp splice-installer-temp/${splice_jar} ${hbase_dir}/lib" # Copy new splice jar

    case $cdh_ver in
	"CDH4")
	    # Copy HBase security and oozie jars 
	    ssh -t ${ssh_opts} ${ssh_user}@${node} "sudo cp splice-installer-temp/${hbase_splice_jar_cdh4} ${hbase_dir}/${hbase_security_jar_cdh4}"
	    ssh -t ${ssh_opts} ${ssh_user}@${node} "sudo cp splice-installer-temp/${hbase_splice_jar_cdh4} ${oozie_dir_cdh4}/${hbase_oozie_jar_cdh4}"
	    # Copy zookeeper jar to all destinations
	    ssh -t ${ssh_opts} ${ssh_user}@${node} 'for i in '"${zookeeper_dirs_cdh4[@]}"' ; do sudo cp -- splice-installer-temp/zookeeper-*.jar "${i}/'"${zookeeper_jar_cdh4}"'" ; done'
	    ;;
	"CDH5")
	    # Copy HBase jars to parcel dir, using their existing filenames
	    ssh -t ${ssh_opts} ${ssh_user}@${node} 'for i in '"${hbase_jars_cdh5[@]}"' ; do find '"${hbase_dir}"'/lib/${i} ! -name *-tests.jar -exec sudo cp -- splice-installer-temp/${i} {} \; ; done'
	    ;;
    esac

    # Clean up
    ssh -t ${ssh_opts} ${ssh_user}@${node} "rm -rf splice-installer-temp"

done
