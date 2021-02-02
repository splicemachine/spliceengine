VERSION=3.1.0.1971

gitroot=$(git rev-parse --show-toplevel) || return
cd $gitroot

./start-splice-cluster -k

# save previous results
mkdir platform_it2/
mv platform_it/*.log platform_it/*log.* platform_it2/
mkdir platform_it2/target
mv platform_it/target/SpliceTestYarnPlatform platform_it/target/*.log platform_it/target/*log.* platform_it2/target

cd platform_it
git clean -dfx
cd ..

tar -xzf upgrade_test/platform_it_${VERSION}.tar.gz
mv platform_it2 platform_it/IT

./start-splice-cluster -l -p${1}

if mvn -B -e surefire:test -Pcore,cdh6.3.0 -Dtest='UpgradeTestIT#*' -DskipServerStart -DfailIfNoTests=false; then
    echo "UPGRADE SUCCEEDED"
    cat platform_it/splice.log | grep 'upgrade scripts'
    cat platform_it/splice.log | grep 'Running upgrade script'
else
  echo "!!! UPGRADE FAILED !!!"
fi
exit

./start-splice-cluster -k