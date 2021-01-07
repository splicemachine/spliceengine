#!/bin/bash

VERSION=3.1.0.1971

# stop current cluster
./start-splice-cluster -k

# clean up platform_it
cd platform_it
git clean -dfx
cd ..

# download the previous standalone data
aws s3 cp s3://splice-snapshots/upgrade_tests/platform_it_${VERSION}.tar.gz .
tar -xzvf platform_it_${VERSION}.tar.gz
rm platform_it_${VERSION}.tar.gz

# restart cluster
./start-splice-cluster -l

# test
if mvn -B -e surefire:test -Pcore,cdh6.3.0 -Dtest='UpgradeTestIT#*' -DskipServerStart -DfailIfNoTests=false; then
    echo "UPGRADE SUCCEEDED"
    cat platform_it/splice.log | grep 'upgrade scripts'
    cat platform_it/splice.log | grep 'Running upgrade script'	
else
	echo "!!! UPGRADE FAILED !!!"
fi

exit

# create a .tar.gz to upgrade from
VERSION=3.1.0.1971



############

# creates a file platform_it_${VERSION}.tar.gz
function create_upgrade_targz
{
	git checkout tags/${VERSION}
	cd platform_it
	git clean -dfx
	cd ..

	./start-splice-cluster

	rm -rf upgrade_test_TMP
	mkdir -p upgrade_test_TMP/platform_it/target
	cd upgrade_test_TMP
	cp -r ../platform_it/target/hbase platform_it/target/.
	cp -r ../platform_it/target/zookeeper platform_it/target/.
	tar -czvf ../platform_it_${VERSION}.tar.gz platform_it
	cd ..
	rm -rf upgrade_test_TMP
}
