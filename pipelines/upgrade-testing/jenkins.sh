# JENKINS FILE USED FOR UPGRADE-TESTING

#!/bin/bash

export platform=${ghprbCommentBody##*@}
platform="$( sed 's/\\n//g' <<<"$platform" )"
platform="$( sed 's/\\r//g' <<<"$platform" )"

platform=($(echo $platform | awk '{print $1}'))

if [[ ${platform} =~ ^mem$ ]] ; then
  profiles="core,mem"
elif [[ ${platform} =~ ^cdh ]] ; then
  profiles="mem,core,${platform},ee,parcel"
elif [[ ${platform} =~ ^hdp ]] ; then
  profiles="core,${platform},ee,hdp_service"
elif [[ ${platform} =~ ^hdp ]] ; then
  profiles="core,${platform},ee,hdp_service"
else
  profiles=mem,"core,${platform},ee,installer"
fi

echo "building ${profiles} for ${platform}"

MAVEN_OPTS="-Xms64m -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30 -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=YYYY-MM-dd|HH:mm:ss,SSS"

function test_upgrade
{
	OLDVERSION=$1
	echo ""
	echo "                        testing $OLDVERSION upgrade"
	echo "--------------------------------------------------------------------------------"
    echo ""
	pushd spliceengine

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
	tar -xzvf platform_it_${OLDVERSION}.tar.gz
    rm platform_it_${OLDVERSION}.tar.gz

	echo ""
  	echo "	./start-splice-cluster -l"
	echo "--------------------------------------------------------------------------------"
    echo ""
    
	# restart cluster
	./start-splice-cluster -l #-p${profiles},ee 
	
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
	popd # spliceengine
}

echo "ghprbCommentBody = ${ghprbCommentBody}"

if [[ -f spliceengine/pipelines/upgrade-testing/check_upgrade.sh && ${platform} =~ ^cdh6.3.0 ]];
then
	echo "upgrade file there"
	upgrade=${ghprbCommentBody##*#upgrade:}
	if [[ $ghprbCommentBody != $upgrade ]] ; then
		echo "upgrade comment there"
		upgrades=($(echo $upgrade | tr ":" "\n"))
		for OLDVERSION in "${upgrades[@]}"
		do
			test_upgrade $OLDVERSION
		done
		exit 0
	fi	
fi

mvn \
  -B \
  -f spliceengine/pom.xml \
  -P${profiles} \
  -D${platform} \
  -Djenkins.build.number=${BUILD_NUMBER} \
  -Dexcluded.categories=com.splicemachine.test.SlowTest \
  -Dmaven.test.redirectTestOutputToFile=true \
  clean install

