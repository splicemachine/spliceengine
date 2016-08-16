/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
                TestUtils.createBrokerConfigs(1, zkConnectString, true, false, null, null, true, false, false, false).iterator();
        assert propsI.hasNext();
        Properties props = propsI.next();
        assert props.containsKey("zookeeper.connect");
        props.put("zookeeper.connect", zkConnectString);
        props.put("port","9092");
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
