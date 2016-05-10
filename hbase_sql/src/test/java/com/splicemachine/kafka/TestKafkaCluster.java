package com.splicemachine.kafka;

import java.io.IOException;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;

public class TestKafkaCluster {
    KafkaServerStartable kafkaServer;

    public TestKafkaCluster(String connectString) throws Exception {
        KafkaConfig config = getKafkaConfig(connectString);
        kafkaServer = new KafkaServerStartable(config);
        kafkaServer.startup();
    }

    public static void main(String [] args) throws Exception {
        TestKafkaCluster cluster;
        if (args.length==1)
            cluster = new TestKafkaCluster(args[0]);
        else
            throw new RuntimeException("No zookeper local");
    }

    private static KafkaConfig getKafkaConfig(final String zkConnectString) {
        scala.collection.Iterator<Properties> propsI =
                TestUtils.createBrokerConfigs(1,true).iterator();
        assert propsI.hasNext();
        Properties props = propsI.next();
        assert props.containsKey("zookeeper.connect");
        props.put("zookeeper.connect", zkConnectString);
        return new KafkaConfig(props);
    }

    public String getKafkaBrokerString() {
        return String.format("localhost:%d",
                kafkaServer.serverConfig().port());
    }

    public int getKafkaPort() {
        return kafkaServer.serverConfig().port();
    }

    public void stop() throws IOException {
        kafkaServer.shutdown();
    }
}
