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

package com.splicemachine.nsds.kafka

import java.io.Externalizable
import java.util
import java.util.{Collections, Properties, UUID}

import com.splicemachine.derby.impl.kryo.KryoSerialization
import com.splicemachine.derby.stream.spark.KafkaReadFunction.Message
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, IntegerDeserializer}

import scala.collection.JavaConverters._

object KafkaUtils {
  private def getConsumer(bootstrapServers: String): KafkaConsumer[Integer, Array[Byte]] = {
    val props = new Properties
    val groupId = "spark-consumer-nsdsk-ku"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + UUID.randomUUID)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    new KafkaConsumer[Integer, Array[Byte]](props)
  }

  def messageCount(bootstrapServers: String, topicName: String): Long = {
    @transient lazy val consumer = getConsumer(bootstrapServers)

    val partitionInfo = consumer.partitionsFor(topicName).asScala
    val partitions = partitionInfo.map(pi => new TopicPartition(topicName, pi.partition()))
    consumer.assign(partitions.asJava)
    consumer.seekToEnd(Collections.emptySet())
    val endPartitions: Map[TopicPartition, Long] = partitions.map(p => p -> consumer.position(p))(collection.breakOut)

    consumer.seekToBeginning(Collections.emptySet())
    val count = partitions.map(p => endPartitions(p) - consumer.position(p)).sum

    consumer.close
    count
  }

  def messageCount(bootstrapServers: String, topicName: String, partition: Int): Long = {
    @transient lazy val consumer = getConsumer(bootstrapServers)

    val topicPartition = new TopicPartition(topicName, partition)
    val partitions = Seq(topicPartition)
    consumer.assign(partitions.asJava)
    consumer.seekToEnd(partitions.asJava)
    val nextOffset = consumer.position(topicPartition)

    consumer.seekToBeginning(partitions.asJava)
    val firstOffset = consumer.position(topicPartition)

    consumer.close
    nextOffset - firstOffset
  }
  
  def lastMessageOf(bootstrapServers: String, topicName: String, partition: Int): Option[Externalizable] = {
    @transient lazy val consumer = getConsumer(bootstrapServers)

    val topicPartition = new TopicPartition(topicName, partition)
    val partitions = Seq(topicPartition)
    consumer.assign(partitions.asJava)
    val end = consumer.endOffsets(partitions.asJava).get(topicPartition)
    if( end == 0L ) {
      None
    } else {
      consumer.seek(topicPartition, end-1)
      consumer.poll(java.time.Duration.ofMillis(1000L)).asScala
        .headOption.map( r => {
          val kryo = new KryoSerialization()
          kryo.init
          val m = kryo.deserialize(r.value).asInstanceOf[Message]
          kryo.close
          m
        })
    }
  }

  def messagesFrom(bootstrapServers: String, topicName: String, partition: Int): Seq[Externalizable] = {
    @transient lazy val consumer = getConsumer(bootstrapServers)

    consumer.assign(util.Arrays.asList(new TopicPartition(topicName, partition)))
    
    val expectedMsgCt = messageCount(bootstrapServers, topicName, partition)
    
    val timeout = java.time.Duration.ofMillis(1000L)
    var records = Iterable.empty[ConsumerRecord[Integer, Array[Byte]]]
    var newRecords = consumer.poll(timeout).asScala // newRecords: Iterable[ConsumerRecord[Integer, Array[Byte]]]
    records = records ++ newRecords

    var retries = 0
    val maxRetries = 10
    while(
      newRecords.nonEmpty ||
      (records.size < expectedMsgCt && retries < maxRetries)
    )
    {
      if( newRecords.isEmpty ) { retries += 1 }
      newRecords = consumer.poll(timeout).asScala
      records = records ++ newRecords
    }
    consumer.close
    
    //println( s"KafkaUtils.msgs record count: ${records.size}" )

    val seqBuilder = Seq.newBuilder[Externalizable]
    val kryo = new KryoSerialization()
    kryo.init
    for (record <- records.iterator) {
      seqBuilder += kryo.deserialize(record.value).asInstanceOf[Message]
    }
    kryo.close

    seqBuilder.result
  }
}
