pwd
sed -i "/* Add filters to the SparkUI./,/\*\*/{s/./REPLACE_ME /g}"  target/tmp/org/apache/spark/scheduler/cluster/YarnSchedulerBackend.scala
awk '/REPLACE_ME/&&c++>0 {next} 1' target/tmp/org/apache/spark/scheduler/cluster/YarnSchedulerBackend.scala > target/temp.java
sed -e "/REPLACE_ME/{r mapr/src/main/resources/patch/yarn-patch.class" -e "d}" target/temp.java >  target/YarnSchedulerBackend.scala
cp target/YarnSchedulerBackend.scala target/tmp/org/apache/spark/scheduler/cluster/YarnSchedulerBackend.scala