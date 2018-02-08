/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import scala.Option;

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
        final Option<File> noFile = scala.Option.apply(null);
        final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
        scala.collection.Iterator<Properties> propsI =
                TestUtils.createBrokerConfigs(1, zkConnectString, true, false, noInterBrokerSecurityProtocol, noFile, true, false, false, false).iterator();
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
