#!/bin/bash

UPGRADE_URL=s3://splice-snapshots/upgrade_tests


if [ $# -lt 2 ]
then
    echo "usage: bash pipelines/upgrade-testing/create_upgrade_targz.sh {VERSION} {test/upload} {additional start-splice-cluster parameters}"
    echo "----------------------------------------------------------------------------------------------------------------------------------"
    echo "creates a tar.gz of a spliceengine standalone cluster that can be used to test upgrade"
    echo "e.g. bash create_upgrade_targz.sh 3.1.0.1971 test -pcore,cdh6.3.0"
    exit 1
fi

VERSION=${1} # e.g. 3.1.0.1971
MODE=${2} # test or upload
shift 2

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

if [[ $MODE == "upload" ]]; then
    aws s3 cp platform_it_${VERSION}.tar.gz ${UPGRADE_URL}/platform_it_${VERSION}.tar.gz
fi

git checkout ${PREVIOUS_BRANCH}
git stash pop
