mvn exec:java -Dzoo -DskipTests > zoo.log &
mvn exec:exec -DspliceFast -DskipTests > splice.log
