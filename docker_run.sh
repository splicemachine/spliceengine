# used base image for docker is maven:3.6.3-jdk-8 + netcat installed.
# docker_run.sh
#----------------------
# helper script to build/run spliceengine standalone

# examples:
#  bash docker_run.sh --- mvn install clean -Pcdh6.3.0,core -DskipTests
#  bash docker_run.sh -p 2527:1527 --- /bin/bash
# then, inside the container:
#  ./start-splice-cluster -pcdh6.3.0 -b

# note you can also do 
# bash docker_run.sh -p 2527:1527 --- ./start-splice-cluster
# however this will exit after the cluster is started, so no cluster anymore :-/

# to prevent this, start a cluster and attach a sqlshell to it, you need this helper script:
# bash docker_run.sh --- /bin/bash docker/start.sh <additional ./start-splice-cluster parameters>

#### port forwarding ####

# - to be able to sqlshell.sh into the container, we need to expose the containers' port 1527
# to do this, add port forwarding and other docker options before the ---
# bash docker_run.sh -p 1527:1527 -p 4000:4000 -p 4020:4020 -p 4025:4025 --- /bin/bash docker/start.sh -l

# shortcut for these mapping is --stdPortMap
# docker_run.sh --stdPortMap --- /bin/bash docker/start.sh -l

 # if you need mapping to different port, but all shifted, you can use --portMap 1000, which will give you
 #  -p 2527:1527 -p 5000:4000 -p 5020:4020 -p 5025:4025

# with this, you can connect e.g. with
#   ./sqlshell.sh -h localhost -p 2527

### MAVEN HOME ###
# - to re-use the $HOME/.m2/settings.xml and the already downloaded
# maven dependencies, we map the path $HOME/.m2 of host to /root/.m2 inside of the container
# (since $HOME=/root, this mimicks $HOME/.m2 in the container)
# 
# if your host has a different maven directory you want to use (e.g. on jenkins)
# you can override this behavior with the command --setMavenHome DIRECTORY, which would map host DIRECTORY to docker /root/.m2.
# 
### SOURCE  ###
# - to have access to the source, we map $(pwd) to /usr/source/
# you can override the source path with --setSource SOURCEDIR
# - we cd into /usr/source in the container.


# With these preparation steps, you can then just instantiate mvn,
# short scripts are:
# 
# build:
# ./docker_run.sh --- mvn clean install -Pcdh6.3.0 -DskipTests
# 
# start splice cluster + sqlshell:
# ./docker_start-splice_cluster.sh -b
# 
# To get a normal bash inside the container:
# ./docker_run.sh /bin/bash
# ./start-splice-cluster -pcdh6.3.0 -b


# options:
#  bash docker_run.sh --- mvn install clean -Pcdh6.3.0,core -DskipTests

# build only, use other maven root, use source from some temporary directory (clean dir build)
# bash docker_run.sh --setMavenHome /tmp/maven_home --setSource ${MY_TMP_SOURCE} --- mvn install clean -Pcdh6.3.0,core -DskipTests

# creating a full link to MY_TMP_SOURCE:
# MY_TMP_SOURCE=/tmp/my_source
#  mkdir ${MY_TMP_SOURCE}
#  cd $HOME/.m2
#  find . -type d -exec mkdir -p ${MY_TMP_SOURCE}/{} ';'
#  find . -type f -exec ln {} ${MY_TMP_SOURCE}/{} ';'
#  cd -

docker build --tag se-spliceengine-build docker

# note that this will map $HOME/.m2 and $(pwd)/.. (=gitroot) into the container, and the container
# will also write to this directories.

# change with using --setMavenHome and --setSource

# default settings
OUR_PORT_MAP="OFF"
OUR_MAVEN_HOME="$HOME/.m2"
OUR_SOURCE=$(pwd)
OTHER_DOCKER_OPTIONS=""

while [ true ];
do
	if [ $1 = "--stdPortMap" ]; then
		OUR_PORT_MAP=0
		shift 1
	elif [ $1 = "--portMap" ]; then
		OUR_PORT_MAP=$2
		shift 2
	elif [ $1 = "--setMavenHome" ]; then
		OUR_MAVEN_HOME=$2
		shift 2
	elif [ $1 = "--setSource" ]; then
		OUR_SOURCE=$2
		shift 2
	elif [ $1 = "---" ]; then
		shift
		break
	else
		OTHER_DOCKER_OPTIONS="${OTHER_DOCKER_OPTIONS} $1"
		shift 1
	fi
done

# 1527 inside docker -> ${PORT_1527_MAPPING}
if [ ${OUR_PORT_MAP} = "OFF" ]; then
	CONFIG_PORT_MAPPING=""
else
	CONFIG_PORT_MAPPING="-p $((1527+OUR_PORT_MAP)):1527 -p $((4000+OUR_PORT_MAP)):4000 -p $((4020+OUR_PORT_MAP)):4020 -p $((4025+OUR_PORT_MAP)):4025"
fi

# reusing maven root from the host (avoid re-download)
CONFIG_MAVEN_VOLUME="-v ${OUR_MAVEN_HOME}:/home/splice/.m2"

# map the gitroot of spliceengine to /usr/source
CONFIG_SOURCE_MAPPING="-v ${OUR_SOURCE}:/home/splice/spliceengine/"

# on linux: use current user
# otherwise if the container writes to mounted volumes they will have owner root
if [ "$OSTYPE" == "darwin"* ]; then
	CONFIG_USER_INFO="" # docker on macos writes per default as current user
else
	CONFIG_USER_INFO="--user $(id -u):$(id -g)"
fi

FULL_DOCKER_OPTIONS="${OTHER_DOCKER_OPTIONS} ${CONFIG_PORT_MAPPING} ${CONFIG_MAVEN_VOLUME} ${CONFIG_SOURCE_MAPPING} ${CONFIG_USER_INFO} \
-w /home/splice/spliceengine se-spliceengine-build $*"

echo
echo "--- STARTING DOCKER WITH FOLLOWING OPTIONS ---"
echo "  - OTHER_DOCKER_OPTIONS  = $OTHER_DOCKER_OPTIONS"
echo "  - CONFIG_PORT_MAPPING   = $CONFIG_PORT_MAPPING"
echo "  - CONFIG_MAVEN_VOLUME   = $CONFIG_MAVEN_VOLUME"
echo "  - CONFIG_SOURCE_MAPPING = $CONFIG_SOURCE_MAPPING"
echo "  - CONFIG_USER_INFO      = $CONFIG_USER_INFO"
echo ""
echo "docker container run -it $FULL_DOCKER_OPTIONS"
echo
docker container run -it $(echo ${FULL_DOCKER_OPTIONS})


