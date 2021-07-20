#!/bin/bash

UPGRADE_URL=s3://splice-snapshots/upgrade_tests

VERSION=${1} # e.g. 3.1.0.1971
shift 1

PREVIOUS_BRANCH=`git rev-parse --abbrev-ref HEAD`
git stash

# creates a file platform_it_${VERSION}.tar.gz
git checkout tags/${VERSION}
cd platform_it
rm -rf target *.log snappy*.jnilib
cd ..

./start-splice-cluster $*
./start-splice-cluster -k

rm -rf upgrade_test_TMP
mkdir -p upgrade_test_TMP/platform_it/target
cd upgrade_test_TMP
cp -r ../platform_it/target/hbase platform_it/target/.
cp -r ../platform_it/target/zookeeper platform_it/target/.
tar -czvf ../platform_it_${VERSION}.tar.gz platform_it
cd ..
rm -rf upgrade_test_TMP

git checkout ${PREVIOUS_BRANCH}
git stash pop

# restart on that version
./start-splice-cluster $*

# clean up platform_it
cd platform_it
git clean -dfx
cd ..

# download the previous standalone data
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
