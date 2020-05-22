package com.splicemachine.spark2.splicemachine

import java.io.Externalizable
import java.util.{Collections, Properties, UUID}

import com.splicemachine.derby.stream.spark.ExternalizableDeserializer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.IntegerDeserializer

import scala.collection.JavaConverters._

object KafkaUtils {
  def messageCount(bootstrapServers: String, topicName: String): Long = {
    val props = new Properties
    val groupId = "spark-consumer-s2s-ku"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + UUID.randomUUID)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ExternalizableDeserializer].getName)

    val consumer = new KafkaConsumer[Integer, Externalizable](props)
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
    val props = new Properties
    val groupId = "spark-consumer-s2s-ku"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + UUID.randomUUID)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ExternalizableDeserializer].getName)

    val consumer = new KafkaConsumer[Integer, Externalizable](props)

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
