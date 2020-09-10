/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.derby.stream.spark;

import java.util.*;
import java.io.Externalizable;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;

public class KafkaUtils {
    public static long messageCount(String bootstrapServers, String topicName, int partition) {
        Properties props = new Properties();
        String group_id = "kafka-util-consumer-dss-ku";
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, group_id+"-"+UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ExternalizableDeserializer.class.getName());

        KafkaConsumer<Integer, Externalizable> consumer = new KafkaConsumer<Integer, Externalizable>(props);

        TopicPartition topicPartition = new TopicPartition(topicName, partition);
        List<TopicPartition> partitionList = Arrays.asList(topicPartition);
        consumer.assign(partitionList);
        consumer.seekToEnd(partitionList);
        long nextOffset = consumer.position(topicPartition);

        consumer.seekToBeginning(partitionList);
        long firstOffset = consumer.position(topicPartition);

        consumer.close();

        return nextOffset - firstOffset;
    }
}
