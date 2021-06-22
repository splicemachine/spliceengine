#!/bin/bash

# cloudera manager defaults
cmhost="localhost"
cmport="7180"
cmscheme="http"
cmuser="admin"
cmpass="admin"

# empty hosts, cluster, etc.
cmcluster=""
declare -A cmhosts
cmyarnsvc=""
cmyarnnms=""

# timestamp
TS="$(date '+%Y%m%d%H%M%S')"

# curl
copts="-k -L -s"

# ssh
sshopts="-t -o LogLevel=QUIET -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# location of YARN application/container logs
yarnlogsdir="/var/log/hadoop-yarn/container"

# default directory to save logs
savedir="/tmp/yarnlogs-${TS}-${$}"

# XXX - needs a usage option...

while getopts "d:h:p:s:U:P:" opt ; do
	case ${opt} in
		d)
			savedir="${OPTARG}"
			;;
		h)
			cmhost="${OPTARG}"
			;;
		p)
			cmport="${OPTARG}"
			;;
		s)
			cmscheme="${OPTARG}"
			;;
		U)
			cmuser="${OPTARG}"
			;;
		P)
			cmpass="${OPTARG}"
			;;
		\?)
			echo "no such option"
			exit 1
			;;
	esac
done

# build our URLs from defaults or user supplied info
cmbase="${cmscheme}://${cmuser}:${cmpass}@${cmhost}:${cmport}"
cmapi="${cmbase}/api"

# get_cm_apiv
#   get cloudera manager API version
get_cm_apiv() {
	local apiv="$(curl ${copts} ${cmapi}/version)"
	if [[ ! "${apiv}" =~ ^v[0-9]+ ]] ; then
		echo "could not determine CM API version" 1>&2
		exit 1
	fi
	cmapi="${cmapi}/${apiv}"
}

# get cloudera manager cluster name
#   XXX - only returns the first cluster name
get_cm_cluster_name() {
	local cluster_name="$(curl ${copts} ${cmapi}/clusters | jq -r '.items[].name' | head -1)"
	echo "${cluster_name}"
}

# get_cm_hosts
#   builds an associative array of host ID to name
get_cm_hosts() {
	local cluster_hostids="$(curl ${copts} ${cmapi}/clusters/${cmcluster}/hosts | jq -r '.items[].hostId')"
	for cluster_hostid in ${cluster_hostids} ; do
		local cluster_hostname="$(curl ${copts} ${cmapi}/hosts | jq -r '.items[] | select(.hostId=="'${cluster_hostid}'") | .hostname')"
		cmhosts["${cluster_hostid}"]="${cluster_hostname}"
	done
}

# get_cm_yarn_svc
#   gets name of CM YARN service
get_cm_yarn_svc() {
	local yarn_servicename="$(curl ${copts} ${cmapi}/clusters/${cmcluster}/services | jq -r '.items[] | select(.type=="YARN") | .name')"
	echo "${yarn_servicename}"
}

# get_cm_yarn_nms
#   gets a list of YARN NodeManagers
get_cm_yarn_nms() {
	local yarn_nodemanagers="$(curl ${copts} ${cmapi}/clusters/${cmcluster}/services/${cmyarnsvc}/roles | jq -r '.items[] | select(.type=="NODEMANAGER") | .hostRef.hostId')"
	echo "${yarn_nodemanagers}"
}

get_cm_apiv
cmcluster="$(get_cm_cluster_name)"
get_cm_hosts
cmyarnsvc="$(get_cm_yarn_svc)"
cmyarnnms="$(get_cm_yarn_nms)"

test -d "${savedir}" || {
	mkdir -p "${savedir}" || {
		echo "could not mkdir ${savedir}" 1>&2
		exit 1
	}
}
pushd "${savedir}" >/dev/null 2>&1 || {
	echo "could not cd into ${savedir}" 1>&2
	exit 1
}
echo "saving per-host application container logs to ${savedir}"
for cmhostid in ${cmyarnnms} ; do
	cmhost="${cmhosts["${cmhostid}"]}"
	mkdir -p "${cmhost}" || {
		echo "could not mkdir ${PWD}/${cmhost}" 1>&2
		popd >/dev/null 2>&1
		exit 1
	}
	pushd "${cmhost}" >/dev/null 2>&1 || {
		echo "could cd into ${PWD}/${cmhost}" 1>&2
		popd >/dev/null 2>&1
		exit 1
	}
	echo "copying logs from ${cmhost}:${yarnlogsdir} into ${PWD}"
	logtar="/tmp/yarnlogs-${TS}.tar"
	localtar="$(basename "${logtar}")"
	# XXX - remote ssh+tar with local scp+tar is slow
	# rsync? - requires root SSH
	# tar via pipe over ssh looks iffy or broken?
	ssh ${sshopts} "${cmhost}" "cd ${yarnlogsdir} ; sudo tar --ignore-failed-read -cpsf ${logtar} ."
	scp "${cmhost}":"${logtar}" .
	test -e "${localtar}" || {
		echo "didn't get a tarred copy of the logs via ssh/scp" 1>&2
		continue
	}
	tar xpsf "${localtar}" && rm -f "${localtar}"
	ssh ${sshopts} "${cmhost}" "sudo rm -f ${logtar}"
	echo "done copying logs from ${cmhost}:${yarnlogsdir}"
	echo "cleaning up empty stdout logs"
	find . -type f -name stdout -exec file {} \; | \
		grep ': empty' | \
		cut -f1 -d: | \
		xargs rm -f
	popd >/dev/null 2>&1
done
popd >/dev/null 2>&1

echo "done copying all YARN NodeManager logs"
echo "logs are in ${savedir}"
