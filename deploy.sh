mvn clean install -DskipTests=true
find . | grep "\.jar$" | grep struc | grep -v test | grep -v ucle | xargs -I xx cp xx ~/project/hbase/current/lib
