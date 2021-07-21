#!/bin/bash
UPGRADE_URL=s3://splice-snapshots/upgrade_tests

if [ $# -lt 2 ]
then
	echo "usage: bash pipelines/upgrade-testing/check_upgrade.sh {VERSION} {additional start-splice-cluster parameters}"
	echo "------------------------------------------------------------------------------------------"
	echo "uses a previously created tar.gz to test upgrade"
	echo "e.g. bash check_upgrade.sh 3.2.2021"
    
    exit 1
fi

function test_upgrade
{
	OLDVERSION=$1
	shift
	echo ""
	echo "                        testing $OLDVERSION upgrade"
	echo "--------------------------------------------------------------------------------"
    echo ""

	UPGRADE_URL=s3://splice-snapshots/upgrade_tests
    
    # stop current cluster
    ./start-splice-cluster -k
    
    # clean up platform_it
    cd platform_it
    git clean -dfx
    cd ..

	echo ""
  	echo "   download ${UPGRADE_URL}/platform_it_${OLDVERSION}.tar.gz"
	echo "--------------------------------------------------------------------------------"
    echo ""
	# download the previous standalone data
	aws s3 cp ${UPGRADE_URL}/platform_it_${OLDVERSION}.tar.gz .
	tar -xzf platform_it_${OLDVERSION}.tar.gz
    rm platform_it_${OLDVERSION}.tar.gz

	echo ""
  	echo "	./start-splice-cluster -l $*"
	echo "--------------------------------------------------------------------------------"
    echo ""
    
	# restart cluster
	./start-splice-cluster -l $*
	
	# test
	if mvn -B -e surefire:test -Pcore,cdh6.3.0 \
    	-Dtest='UpgradeTestIT#*' -DskipServerStart \
        -DfailIfNoTests=false; \
        -Dmaven.test.redirectTestOutputToFile=true
   then
	    echo "UPGRADE SUCCEEDED"
	    cat platform_it/splice.log | grep 'upgrade scripts'
	    cat platform_it/splice.log | grep 'Running upgrade script'	
	else
		echo "!!! UPGRADE FAILED !!!"
		exit 2
	fi

	echo "--------------------------------------------------------------------------------"
    echo ""
}


test_upgrade $*