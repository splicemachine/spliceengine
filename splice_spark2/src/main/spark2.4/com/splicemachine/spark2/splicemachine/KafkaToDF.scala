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
import org.apache.kafka.clients.consumer.ConsumerRecords
import scala.collection.JavaConverters._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType

class KafkaToDF(kafkaServers: String, pollTimeout: Long) {
  private[this] val destServers = kafkaServers
  private[this] val timeout = pollTimeout

  def spark(): SparkSession = SparkSession.builder.getOrCreate

  def df(topicName: String): Dataset[Row] = {
    val (rdd, schema) = rdd_schema(topicName)
    spark.createDataFrame( rdd , schema )
  }

  def rdd(topicName: String): RDD[Row] = rdd_schema(topicName)._1

  def rdd_schema(topicName: String): (RDD[Row], StructType) = {
    val props = new Properties()
    val consumerId = UUID.randomUUID()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, destServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-consumer-group-"+consumerId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "spark-consumer-"+consumerId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ExternalizableDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[Integer, Externalizable](props)
    consumer.subscribe(util.Arrays.asList(topicName))

    val records = consumer.poll( java.time.Duration.ofMillis(timeout) ).asScala  // records: ConsumerRecords[Integer, Externalizable]
    consumer.close

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
  }
}
