#!/bin/bash
UPGRADE_URL=s3://splice-snapshots/upgrade_tests

if [ $# -lt 2 ]
then
	echo "usage: bash create_upgrade_targz.sh {VERSION} {additional start-splice-cluster parameters}"
	echo "------------------------------------------------------------------------------------------"
	echo "uses a previously created tar.gz to test upgrade"
	echo "e.g. bash create_upgrade_targz.sh 3.2.2021 -T 16"
	echo "make sure you current branch has already been build"
	echo "don't use -b, since we are deleting some files in platform_it/target"
    
    exit 1
fi

VERSION=${1}
shift

# stop current cluster
./start-splice-cluster -k

# clean up platform_it
cd platform_it
git clean -dfx
cd ..

# download the previous standalone data
aws s3 cp ${UPGRADE_URL}/platform_it_${VERSION}.tar.gz .
tar -xzvf platform_it_${VERSION}.tar.gz
rm platform_it_${VERSION}.tar.gz

# restart cluster
./start-splice-cluster -l $*

# test
if mvn -B -e surefire:test -Pcore,cdh6.3.0 -Dtest='UpgradeTestIT#*' -DskipServerStart -DfailIfNoTests=false; then
    echo "UPGRADE SUCCEEDED"
    cat platform_it/splice.log | grep 'upgrade scripts'
    cat platform_it/splice.log | grep 'Running upgrade script'	
else
	echo "!!! UPGRADE FAILED !!!"
fi
