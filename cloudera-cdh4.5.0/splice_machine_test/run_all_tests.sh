#------------------------------------------------------
# Runs all unit and integration tests.
#------------------------------------------------------

# Start zookeeper in background.
mvn exec:exec -PspliceZoo > zoo_it.log 2>&1 &

# Start SpliceTestPlaform in background.
mvn exec:exec -PspliceFast -DfailTasksRandomly=true > splice_it.log 2>&1 &

# Wait for for splice-JDBC connections to be available, then run all tests.
mvn exec:java -PspliceWait && \
    mvn verify -Dskip.integration.tests=false -DskipTests=true -Dexcluded.categories=  
