src/test/bin/stop-splice-its
cd ..
mvn -DskipTests clean install -Dspark-prepare
cd splice_machine_test
src/test/bin/start-splice-its
