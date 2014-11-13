#---------------------------------------------------------------------------------------
# Runs all unit and integration tests.
#
# usage-example: ./run_all_tests.sh cloudera-cdh4.5.0  (from repository root)
#---------------------------------------------------------------------------------------

# cd to directory of pom for profile we will use
cd $1/splice_machine_test

# Start zookeeper in background.
mvn exec:exec -PspliceZoo > zoo_it.log 2>&1 &

# Start SpliceTestPlaform in background.
mvn exec:exec -PspliceFast -DfailTasksRandomly=false > splice_it.log 2>&1 &

# Wait for for splice-JDBC connections to be available, then run all tests.
mvn exec:java -PspliceWait && \
    mvn verify -Dskip.integration.tests=false -DskipTests=true -Dexcluded.categories=  
