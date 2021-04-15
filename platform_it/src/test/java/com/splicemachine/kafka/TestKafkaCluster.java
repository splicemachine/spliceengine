/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.kafka;

import java.io.IOException;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class TestKafkaCluster {
    private KafkaServerStartable kafkaServer;

    /**
     * 
     * @param connectString Zookeeper host:port
     * @param offsetsTopicReplicationFactor The value to set for property offsets.topic.replication.factor
     * @param externalListenerHost The host or IP to be used for the advertised listener on port 19092
     * @param kafkaProps Any Kafka properties in the form "prop1=val1;prop2=val2;prop3=val3"
     */
    public TestKafkaCluster(
            String connectString, 
            String offsetsTopicReplicationFactor,
            String externalListenerHost,
            String kafkaProps
    ) {
        KafkaConfig config = getKafkaConfig(connectString, offsetsTopicReplicationFactor, externalListenerHost, kafkaProps);
        kafkaServer = new KafkaServerStartable(config);
        kafkaServer.startup();
    }

    public TestKafkaCluster(String connectString) {
        this(connectString, "1", "localhost", "");
    }

    public static void main(String [] args) throws Exception {
        if (args.length==1)
            new TestKafkaCluster(args[0]);
        else if (args.length==2)
            new TestKafkaCluster(args[0], args[1], "localhost", "");
        else if (args.length==3)
            new TestKafkaCluster(args[0], args[1], args[2], "");
        else if (args.length==4)
            new TestKafkaCluster(args[0], args[1], args[2], args[3]);
        else
            throw new RuntimeException("No zookeeper local");
    }

    private static KafkaConfig getKafkaConfig(
            final String zkConnectString, 
            final String offsetsTopicReplicationFactor,
            final String externalListenerHost,
            final String kafkaProps
    ) {
        Properties props = new Properties();
        assert props.containsKey("zookeeper.connect");
        props.put("zookeeper.connect", zkConnectString);
        props.put("broker.id","0");
        props.put("port","9092");
        props.put("advertised.listeners","PLAINTEXT://localhost:9092,EXTERNAL://"+externalListenerHost+":19092");
        props.put("listeners","PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:19092");
        props.put("listener.security.protocol.map","PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT");
        props.put("offsets.topic.replication.factor", offsetsTopicReplicationFactor);  // helps splice standalone work on Kafka 2.2
        props.put("log.dir", System.getProperty("java.io.tmpdir", "target/tmp") + "/kafka-logs");
        if( ! kafkaProps.isEmpty() ) {
            String[] keyValues = kafkaProps.split(";");
            for(String keyValue: keyValues) {
                System.out.println("Setting Kafka property " + keyValue);
                String[] kv = keyValue.split("=");
                props.put(kv[0], kv[1]);
            }
        }
        return new KafkaConfig(props);
    }

//    public String getKafkaBrokerString() {
//        return String.format("localhost:%d",
//                kafkaServer.serverConfig().port());
//    }
//
//    public int getKafkaPort() {
//        return kafkaServer.serverConfig().port();
//    }

    public void stop() throws IOException {
        kafkaServer.shutdown();
    }
}
