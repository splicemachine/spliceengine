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

package com.splicemachine.spark2.splicemachine

import com.splicemachine.derby.stream.spark.ExternalizableDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.IntegerDeserializer
import java.io.Externalizable
import java.util
import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.spark.TaskContext
import org.apache.spark.TaskKilledException
import scala.collection.JavaConverters._
//import com.splicemachine.db.impl.sql.execute.ValueRow
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType

class KafkaToDF {

  def spark(): SparkSession = SparkSession.builder.getOrCreate  // TODO will this work in all envs?

  def df(topicName: String): Dataset[Row] = {
    val (rdd, schema) = rdd_schema(topicName)
    spark.createDataFrame( rdd , schema )
  }

  def rdd(topicName: String): RDD[Row] = rdd_schema(topicName)._1

  def rdd_schema(topicName: String): (RDD[Row], StructType) = {
    val props = new Properties()
    val consumerId = "spark-consumer-"+UUID.randomUUID()
    // TODO move broker addresses to config
    val brokers = "localhost:" + 9092

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ExternalizableDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[Integer, Externalizable](props)
    consumer.subscribe(util.Arrays.asList(topicName))
//        val ps = consumer.partitionsFor(topicName)
//        val partitions = new Array[Int](ps.size)
//        var i = 0
//        while ( {
//            i < ps.size
//        }) {
//            partitions.add(i)
//
//            i += 1
//        }
////        consumer.close
//        int partition = partitions.next
//        consumer.assign(java.util.Arrays.asList(new TopicPartition(topicName, partition)))

        //        consumer.close();
//        ConsumerRecords<Integer, Externalizable> records = null;
//        Iterator<ConsumerRecord<Integer, Externalizable>> it = null;
//        ConsumerRecord<Integer, Externalizable> next = null;

//        var records: ConsumerRecords[Integer, Externalizable] = null
//        while ( {
//            records == null || records.isEmpty
//        }) {
    val records = consumer.poll(20000).asScala  // TODO move timeout to config
    consumer.close
//            if (TaskContext.get.isInterrupted) {  // TODO: was giving null pointer exception
//                consumer.close
//                throw new TaskKilledException
//            }

    // records.isEmpty when consumer.poll times out
    //    a query that legitimately returns no rows in the db also seems to result in timeout here
    if (records.isEmpty) { ( spark.sparkContext.parallelize( Seq[Row]() ) , StructType(Nil) ) }
    else {
      val seqBuilder = Seq.newBuilder[Row]
      for (record <- records.iterator) {
        //              println(s"${record.value.getClass.getName}")
        //              println(s"offset = ${record.offset}, key = ${record.key}, value = ${record.value}")
        seqBuilder += record.value.asInstanceOf[Row]
      }

      val rows = seqBuilder.result
      val rdd = spark.sparkContext.parallelize(rows)
      (rdd, rows(0).schema)
    }

        //        return new Iterator<ExecRow>() {
//            ConsumerRecords<Integer, Externalizable> records = null;
//            Iterator<ConsumerRecord<Integer, Externalizable>> it = null;
//            ConsumerRecord<Integer, Externalizable> next = null;
//            boolean exhausted = false;
//
//            @Override
//            public boolean hasNext() {
//                if (exhausted) return false;
//                if (it == null) {
//                    while (records == null || records.isEmpty()) {
//                        records = consumer.poll(1000);
//                        if (TaskContext.get().isInterrupted()) {
//                            consumer.close();
//                            throw new TaskKilledException();
//                        }
//                    }
//                    it = records.iterator();
//                }
//                if (it.hasNext()) {
//                    return true;
//                }
//                else {
//                    consumer.close();
//                    return false;
//                }
//            }
//
//            @Override
//            public ExecRow next() {
//                return (ExecRow)it.next().value();
//            }
//        };
  }

}
