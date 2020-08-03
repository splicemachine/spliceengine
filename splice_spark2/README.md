## Native Spark Data Source (NSDS)

### Troubleshooting

#### Kafka Timeout / Kafka Bootstrap Server Config

NSDS times out on Kafka when this param isn't set properly.
The _splice.kafka.bootstrapServers_ param is used by the classes in the DB to connect to Kafka.

In a bare metal installation, be sure the config parameter _splice.kafka.bootstrapServers_ is set as described in [CDH-installation](../platforms/cdh6.3.0/docs/CDH-installation.md) or [HDP-installation](../platforms/hdp3.1.0/docs/HDP-installation.md).

In standalone, it should be fine using the default of _localhost:9092_ and the config param won't need to be set.

### Kafka Maintenance

The [Kafka Maintenance process](src/main/java/com/splicemachine/nsds/kafka/KafkaMaintenance.java) can be run on the 
command line like

    java -cp $jars/splice_spark2-3.1.0.1959-SNAPSHOT-cdh6.3.0.jar:$jars/kafka-clients-2.2.1-cdh6.3.0.jar:$jars/log4j-1.2.17.jar:$jars/slf4j-log4j12-1.7.25.jar:$jars/slf4j-api-1.7.15.jar \
        -Dlog4j.configuration=file:$props/info-log4j.properties com.splicemachine.nsds.kafka.KafkaMaintenance localhost:9092 /tmp/km.dat

where $jars is the path to the directory containing the jars from the spliceengine build,
$props is the path to the directory containing the splice log4j properties file, 
localhost:9092 is the host and port of the Kafka server,
and /tmp/km.dat is a data file that will be written by KafkaMaintenance.

An optional parameter that may be added is the number of minutes for the age cutoff. 
A topic won't be deleted until it's older than the cutoff.
It defaults to the number of minutes in a day.

#### Kafka Maintenance Set Up on Bare Metal

In a bare metal installation, set up an hourly cron job like

    0 * * * * export jars=<full path to the dir containing the spliceengine jars>; java -cp $jars/splice_spark2-3.1.0.1959-SNAPSHOT-cdh6.3.0.jar:$jars/kafka-clients-2.2.1-cdh6.3.0.jar:$jars/log4j-1.2.17.jar:$jars/slf4j-log4j12-1.7.25.jar:$jars/slf4j-api-1.7.15.jar -Dlog4j.configuration=file:<full path to the directory containing the splice log4j properties file>/info-log4j.properties com.splicemachine.nsds.kafka.KafkaMaintenance <host:port of the Kafka server> <full path to the KafkaMaintenance data file> > <full path to the KafkaMaintenance log file> &

The data file and log file won't exist before the first run.
The version numbers in the jars may vary from one release to another.
