/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.stream.StreamProtocol;
import com.splicemachine.stream.handlers.OpenHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
    private String host;
    private int port;
    private String topicName;
    private int partition;
    private volatile TaskContext taskContext;

    // Serialization
    public KafkaStreamer(){
    }

    public KafkaStreamer(String host, int port, int numPartitions, String topicName) {
        this.host = host;
        this.port = port;
        this.numPartitions = numPartitions;
        this.topicName = topicName;
    }

    @Override
    public Iterator<String> call(Integer partition, Iterator<T> locatedRowIterator) throws Exception {
        this.partition = partition;
        taskContext = TaskContext.get();

        if (taskContext.attemptNumber() > 0) {
            // TODO handle retries

            int entriesInKafka = 0;
            // open connection to partition in Kafka
            // get number of entries there

            // TODO failed task retry
            for (int i = 0; i < entriesInKafka; ++i) {
                locatedRowIterator.next();
            }
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", host + ":" + 9092);
        props.put("client.id", "spark-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ExternalizableSerializer.class.getName());
        KafkaProducer<Integer, Externalizable> producer = new KafkaProducer<>(props);
        int count = 0 ;
        while (locatedRowIterator.hasNext()) {
            T lr = locatedRowIterator.next();

            ProducerRecord<Integer, Externalizable> record = new ProducerRecord(topicName,
                    partition.intValue(), count++, lr);
            producer.send(record);
        }

        ProducerRecord<Integer, Externalizable> record = new ProducerRecord(topicName,
                partition.intValue(), -1, new ValueRow());
        producer.send(record); // termination marker

        producer.close();
        // TODO Clean up
        return Arrays.asList("OK").iterator();
    }

    @Override
    public String toString() {
        return "KafkaStreamer{" +
                "numPartitions=" + numPartitions +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", topicName=" + topicName +
                '}';
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(host);
        out.writeInt(port);
        out.writeInt(numPartitions);
        out.writeUTF(topicName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        host = in.readUTF();
        port = in.readInt();
        numPartitions = in.readInt();
        topicName = in.readUTF();
    }
}
