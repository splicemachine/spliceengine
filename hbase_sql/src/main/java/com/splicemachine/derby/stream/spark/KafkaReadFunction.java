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
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.kryo.KryoSerialization;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportKafkaOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskKilledException;

import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.time.Duration;

public class KafkaReadFunction extends SpliceFlatMapFunction<ExportKafkaOperation, Integer, ExecRow> {
    private String topicName;
    private String bootstrapServers;

    private static final Logger LOG = Logger.getLogger(KafkaReadFunction.class);

    public KafkaReadFunction() {}

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
        LOG.trace( id+" KRF.call p "+partition );
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String consumer_id = "spark-consumer-dss-krf-"+UUID.randomUUID();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumer_id);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumer_id);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // MAX_POLL_RECORDS_CONFIG helped performance in standalone with lower values (default == 500).
        //  With high values, it spent too much time retrieving records from Kafka.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
//        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10485760"); // 10% of max == 5242880, default == 1048576

        KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<Integer, byte[]>(props);
        consumer.assign(Arrays.asList(new TopicPartition(topicName, partition)));

        KryoSerialization kryo = new KryoSerialization();
        kryo.init();

        return new Iterator<ExecRow>() {
            Iterator<ConsumerRecord<Integer, byte[]>> it = null;
            Message prevMessage = new Message();

            Predicate<ConsumerRecords<Integer, byte[]>> noRecords = records ->
                records == null || records.isEmpty();
            
            long totalCount = 0L;
            int maxRetries = 10;
            int retries = 0;
            final Duration shortTimeout = java.time.Duration.ofMillis(500L);
            final Duration longTimeout = java.time.Duration.ofMinutes(1L);
            
            private ConsumerRecords<Integer, byte[]> kafkaRecords(int maxAttempts, Duration timeout) throws TaskKilledException {
                int attempt = 1;
                ConsumerRecords<Integer, byte[]> records = null;
                do {
                    records = consumer.poll(timeout);
                    if (TaskContext.get().isInterrupted()) {
                        LOG.warn( id+" KRF.call kafkaRecords Spark TaskContext Interrupted");
                        //consumer.close();
                        throw new TaskKilledException();
                    }
                } while( noRecords.test(records) && attempt++ < maxAttempts );
                
                return records;
            }
            
            private boolean hasMoreRecords(int maxAttempts, Duration timeout) throws TaskKilledException {
                ConsumerRecords<Integer, byte[]> records = kafkaRecords(maxAttempts, timeout);
                if( noRecords.test(records) ) {
                    if( !prevMessage.last() && retries < maxRetries ) {
                        retries++;
                        Duration retryTimeout = longTimeout;
                        LOG.warn( id+" KRF.call Missed rcds, got "+totalCount+" retry "+retries+" for up to "+retryTimeout );
                        return hasMoreRecords(
                            maxAttempts,
                            retryTimeout
                        );
                    }
                    //consumer.close();
                    if( !prevMessage.last() ) {
                        LOG.error(id + " KRF.call Didn't get full batch after " + retries + " retries, got " + totalCount + " records");
                    }
                    return false;
                } else {
                    int ct = records.count();
                    totalCount += ct;
                    LOG.trace( id+" KRF.call p "+partition+" t "+topicName+" records "+ct );
                    retries = 0;
                    
                    it = records.iterator();
                    return it.hasNext();
                }
            }
            
            @Override
            public boolean hasNext() {
                boolean more = false;
                
                if (it != null && it.hasNext()) {
                    more = true;
                } else if (!prevMessage.last()) {
                    more = hasMoreRecords(1, shortTimeout);
                }

                if (!more) {
                    consumer.close();
                    kryo.close();
                }

                return more;
            }

            @Override
            public ExecRow next() {
                Message m = (Message)kryo.deserialize( it.next().value() );
                prevMessage = m;
                return m.vr();
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

    public static class Message implements Externalizable {

        private ValueRow vr = new ValueRow();
        private long id = -1;
        private long max = -1;
        
        public Message() {}

        public Message(ValueRow vr, long id, long max) {
            this.vr = vr;
            this.id = id;
            this.max = max;
        }

        public ValueRow vr()    { return vr; }
        public long id()        { return id; }
        public long max()       { return max; }
        
        public boolean last()   { return id == max && max > -1; }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            vr.writeExternal(out);
            //out.writeObject(vr);
            out.writeLong(id);
            out.writeLong(max);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            vr.readExternal(in);
            //vr = (ValueRow)in.readObject();
            id = in.readLong();
            max = in.readLong();
        }
    }
}
