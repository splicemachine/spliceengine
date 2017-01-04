#!/bin/bash

# this is also part of flatten.sh; really need to make these a shell library

set_spark_dfs_perms() {
	sudo -su hdfs hadoop fs -mkdir -p /user/spark/applicationHistory
	sudo -su hdfs hadoop fs -chmod 1777 /user /user/spark/applicationHistory
	sudo -su hdfs hadoop fs -chmod 777 /user/spark
	id spark >/dev/null && {
		sudo -su hdfs hadoop fs -chown spark:spark /user/spark /user/spark/applicationHistory
		sudo -su hdfs hadoop fs -chgrp -R spark /user/spark/applicationHistory
	}
}

set_spark_dfs_perms
