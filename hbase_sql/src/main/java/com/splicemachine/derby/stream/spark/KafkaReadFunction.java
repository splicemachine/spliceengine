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
//import com.splicemachine.db.impl.sql.execute.ValueRow;
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
import org.apache.log4j.Logger;
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

    private static final Logger LOG = Logger.getLogger(KafkaReadFunction.class);
    
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
        String id = topicName.substring(0,5)+":"+partition.toString();
        LOG.info( id+" KRF.call p "+partition );
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String consumer_id = "spark-consumer-dss-krf-"+UUID.randomUUID();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumer_id);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumer_id);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ExternalizableDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // MAX_POLL_RECORDS_CONFIG helped performance in standalone with lower values (default == 500).
        //  With high values, it spent too much time retrieving records from Kafka.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
//        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10485760"); // 10% of max == 5242880, default == 1048576

        KafkaConsumer<Integer, Externalizable> consumer = new KafkaConsumer<Integer, Externalizable>(props);
        consumer.assign(Arrays.asList(new TopicPartition(topicName, partition)));

        return new Iterator<ExecRow>() {
            Iterator<ConsumerRecord<Integer, Externalizable>> it = null;
            
            Predicate<ConsumerRecords<Integer, Externalizable>> noRecords = records ->
                records == null || records.isEmpty();
            
            long kafkaRcdCount = KafkaUtils.messageCount(bootstrapServers, topicName, partition);
            long totalCount = 0L;
            int maxRetries = 10;
            int retries = 0;
            
            private ConsumerRecords<Integer, Externalizable> kafkaRecords(int maxAttempts) throws TaskKilledException {
                int attempt = 1;
                ConsumerRecords<Integer, Externalizable> records = null;
                do {
                    records = consumer.poll(java.time.Duration.ofMillis(500));
                    if (TaskContext.get().isInterrupted()) {
                        LOG.warn( id+" KRF.call kafkaRecords Spark TaskContext Interrupted");
                        consumer.close();
                        throw new TaskKilledException();
                    }
                } while( noRecords.test(records) && attempt++ < maxAttempts );
                
                return records;
            }
            
            private boolean hasMoreRecords(int maxAttempts) throws TaskKilledException {
                ConsumerRecords<Integer, Externalizable> records = kafkaRecords(maxAttempts);
                if( noRecords.test(records) ) {
                    kafkaRcdCount = KafkaUtils.messageCount(bootstrapServers, topicName, partition);
                    if( totalCount < kafkaRcdCount && retries < maxRetries ) {
                        retries++;
                        LOG.warn( id+" KRF.call Missed rcds, got "+totalCount+" of "+kafkaRcdCount+" retry "+retries );
                        return hasMoreRecords(maxAttempts);
                    }
                    LOG.info( id+" KRF.call p "+partition+" t "+topicName+" expected "+kafkaRcdCount );
                    consumer.close();
                    return false;
                } else {
                    int ct = records.count();
                    totalCount += ct;
                    LOG.info( id+" KRF.call p "+partition+" t "+topicName+" records "+ct );
                    //LOG.info( id+" KRF.call p "+partition+" t "+topicName+" progress "+totalCount+" "+kafkaRcdCount );
                    
                    it = records.iterator();
                    return it.hasNext();
                }
            }

            @Override
            public boolean hasNext() {
                if (it == null) {
                    return hasMoreRecords(2);
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
                //return ((ValueRow.Message)it.next().value()).vr();
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
