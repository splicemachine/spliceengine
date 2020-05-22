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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.marshall.dvd.KryoDescriptorSerializer;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.StreamProtocol;
import com.splicemachine.stream.handlers.OpenHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hadoop.hive.ql.exec.spark.KryoSerializer;
import org.apache.hive.com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskKilledException;
import org.apache.spark.api.java.function.Function2;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by dgomezferro on 5/25/16.
 */
public class KafkaStreamer<T> implements Function2<Integer, Iterator<T>, Iterator<String>>, Serializable, Externalizable {
    private static final Logger LOG = Logger.getLogger(KafkaStreamer.class);

    private int numPartitions;
    private String bootstrapServers;
    private String topicName;
    private volatile TaskContext taskContext;

    // Serialization
    public KafkaStreamer(){
    }

    public KafkaStreamer(int numPartitions, String topicName) {
        this.bootstrapServers = SIDriver.driver().getConfiguration().getKafkaBootstrapServers();
        this.numPartitions = numPartitions;
        this.topicName = topicName;
    }

    public void noData() throws Exception {
        call(0, (Iterator<T>)Arrays.asList(new ValueRow(0)).iterator());
    }

    @Override
    public Iterator<String> call(Integer partition, Iterator<T> locatedRowIterator) throws Exception {
        taskContext = TaskContext.get();

        if (taskContext != null && taskContext.attemptNumber() > 0) {
            LOG.trace("KS.c attempts "+taskContext.attemptNumber());
            long entriesInKafka = KafkaUtils.messageCount(bootstrapServers, topicName, partition);
            LOG.trace("KS.c entries "+entriesInKafka);
            for (long i = 0; i < entriesInKafka; ++i) {
                locatedRowIterator.next();
            }
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-dss-ks-"+UUID.randomUUID() );
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ExternalizableSerializer.class.getName());
        KafkaProducer<Integer, Externalizable> producer = new KafkaProducer<>(props);
        int count = 0 ;
        while (locatedRowIterator.hasNext()) {
            T lr = locatedRowIterator.next();

            ProducerRecord<Integer, Externalizable> record = new ProducerRecord(topicName, count++, lr);
            producer.send(record);
            LOG.trace("KS.c sent "+partition.intValue()+" "+count+" "+lr);
        }
        LOG.trace("KS.c count "+partition.intValue()+" "+count);

        producer.close();
        // TODO Clean up
        return Arrays.asList("OK").iterator();
    }

    @Override
    public String toString() {
        return "KafkaStreamer{" +
                "numPartitions=" + numPartitions +
                ", bootstrapServers='" + bootstrapServers + '\'' +
                ", topicName=" + topicName +
                '}';
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(bootstrapServers);
        out.writeInt(numPartitions);
        out.writeUTF(topicName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bootstrapServers = in.readUTF();
        numPartitions = in.readInt();
        topicName = in.readUTF();
    }
}
