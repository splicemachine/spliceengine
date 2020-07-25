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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportKafkaOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskKilledException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;

class KafkaReadFunction extends SpliceFlatMapFunction<ExportKafkaOperation, Integer, ExecRow> {
    private String topicName;
    private String bootstrapServers;

    public KafkaReadFunction() {
    }

    public KafkaReadFunction(OperationContext context, String topicName) {
        this(context, topicName, SIDriver.driver().getConfiguration().getKafkaBootstrapServers());
    }

    public KafkaReadFunction(OperationContext context, String topicName, String bootstrapServers) {
        super(context);
        this.topicName = topicName;
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public Iterator<ExecRow> call(Integer partition) throws Exception {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String consumer_id = "spark-consumer-dss-krf-"+UUID.randomUUID();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumer_id);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumer_id);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ExternalizableDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, Externalizable> consumer = new KafkaConsumer<Integer, Externalizable>(props);
        consumer.assign(Arrays.asList(new TopicPartition(topicName, partition)));

        return new Iterator<ExecRow>() {
            Iterator<ConsumerRecord<Integer, Externalizable>> it = null;
            
            Predicate<ConsumerRecords<Integer, Externalizable>> noRecords = records ->
                records == null || records.isEmpty();
            
            private ConsumerRecords<Integer, Externalizable> kafkaRecords(int maxAttempts) throws TaskKilledException {
                int attempt = 1;
                ConsumerRecords<Integer, Externalizable> records = null;
                do {
                    records = consumer.poll(java.time.Duration.ofMillis(1000));
                    if (TaskContext.get().isInterrupted()) {
                        consumer.close();
                        throw new TaskKilledException();
                    }
                } while( noRecords.test(records) && attempt++ < maxAttempts );
                
                return records;
            }
            
            private boolean hasMoreRecords(int maxAttempts) throws TaskKilledException {
                ConsumerRecords<Integer, Externalizable> records = kafkaRecords(maxAttempts);
                if( noRecords.test(records) ) {
                    consumer.close();
                    return false;
                } else {
                    it = records.iterator();
                    return it.hasNext();
                }
            }

            @Override
            public boolean hasNext() {
                if (it == null) {
                    return hasMoreRecords(60);
                }
                if (it.hasNext()) {
                    return true;
                }
                else {
                    return hasMoreRecords(1);
                }
            }

            @Override
            public ExecRow next() {
                return (ExecRow)it.next().value();
            }
        };
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(topicName);
        out.writeUTF(bootstrapServers);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        topicName = in.readUTF();
        bootstrapServers = in.readUTF();
    }
}
