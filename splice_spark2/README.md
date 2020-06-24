## Native Spark Data Source (NSDS)

### Troubleshooting

#### Kafka Timeout / Kafka Bootstrap Server Config

NSDS times out on Kafka when this param isn't set properly.
The _splice.kafka.bootstrapServers_ param is used by the classes in the DB to connect to Kafka.

In a bare metal installation, be sure the config parameter _splice.kafka.bootstrapServers_ is set as described in [CDH-installation](../platforms/cdh6.3.0/docs/CDH-installation.md) or [HDP-installation](../platforms/hdp3.1.0/docs/HDP-installation.md).

In standalone, it should be fine using the default of _localhost:9092_ and the the config param won't need to be set.
