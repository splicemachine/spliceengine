#!/bin/bash

VERSION=3.1.0.1971

# make sure we're in git root
gitroot=$(git rev-parse --show-toplevel)
pushd ${gitroot}

# stop current cluster
./start-splice-cluster -k

# clean up platform_it
#cd platform_it
#git clean -dfx
#cd ..

rm -rf $gitroot/platform_it/target/hbase
rm -rf $gitroot/platform_it/target/zookeeper
rm -rf $gitroot/platform_it/target/hbase-site.xml
rm -rf $gitroot/platform_it/target/SpliceTestYarnPlatform

# download the previous standalone data
aws s3 cp s3://splice-snapshots/upgrade_tests/platform_it_${VERSION}.tar.gz .
tar -xzf platform_it_${VERSION}.tar.gz
rm platform_it_${VERSION}.tar.gz

./start-splice-cluster -b -p${1}
