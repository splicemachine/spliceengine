# used base image for docker is maven:3.6.3-jdk-8 + netcat installed.

docker build --tag se-spliceengine-build docker

# - to be able to sqlshell.sh into the container, we are exposing the containers' port 1527
# as port 2527 for the host, so you can connect from host e.g. with
#   ./sqlshell.sh -h localhost -p 2527
# 
# - to re-use the $HOME/.m2/settings.xml and the already downloaded
# maven dependencies, we map the path $HOME/.m2 of host to
#  /root/.m2 inside of the container (since $HOME=/root, this mimicks $HOME/.m2 in the container)
# 
# - to have access to the source, we map $(pwd) to /usr/src/
# - we cd into /usr/src in the container.
# 
# With these preparation steps, you can then just instantiate mvn,
# short scripts are:
# 
# build:
# ./docker_run.sh mvn clean install -Pcdh6.3.0 -DskipTests
# 
# start splice cluster + sqlshell:
# ./docker_start-splice_cluster.sh -b
# 
# To get a normal bash inside the container:
# ./docker_run.sh /bin/bash
# ./start-splice-cluster -pcdh6.3.0 -b
# 
#
# TODO: when running on linux, the container will create files with user "root"
# this can be prevented with e.g. https://vsupalov.com/docker-shared-permissions/

# ATTENTION: Because of the path mapping of $HOME/.m2 and $(pwd), this means the docker
# container will MODIFY these directories on the host.

HOST_PORT=2527

echo "starting $*"

docker run -it \
  -p ${HOST_PORT}:1527         `# 1527 inside docker -> ${HOST_PORT}`                   \
  -v "$HOME/.m2":/root/.m2     `# reusing maven root from the host (avoid re-download)` \
  -v "$(pwd)":/usr/src/        `# map the gitroot of spliceengine to /usr/src`          \
  -w /usr/src/                 `# set /usr/src as current working directoy`             \
  se-spliceengine-build \
  $*
