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
import java.util.{Collections, Properties, UUID}

import com.splicemachine.derby.stream.spark.ExternalizableDeserializer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.IntegerDeserializer

import scala.collection.JavaConverters._

object KafkaUtils {

  private def getConsumer(bootstrapServers: String): KafkaConsumer[Integer, Externalizable] = {
    val props = new Properties
    val groupId = "spark-consumer-nsdsk-ku"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId +"-"+ UUID.randomUUID)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ExternalizableDeserializer].getName)
    new KafkaConsumer[Integer, Externalizable](props)
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
}
