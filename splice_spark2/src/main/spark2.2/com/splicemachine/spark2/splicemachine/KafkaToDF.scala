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

import java.io.Externalizable
import java.util
import java.util.{Properties, UUID}

import com.splicemachine.derby.stream.spark.ExternalizableDeserializer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

import scala.collection.JavaConverters._

@SuppressFBWarnings(value = Array("NP_ALWAYS_NULL"), justification = "Fields 'row' and 'records' aren't always null, and null checks didn't eliminate Spotbugs error; see DB-9580.")
@SuppressFBWarnings(value = Array("SE_BAD_FIELD"), justification = "This class isn't serializable, and there's no field named 'outer'.")
class KafkaToDF(kafkaServers: String, pollTimeout: Long, querySchema: StructType) {
  val shortTimeout = if( pollTimeout <= 1000L ) pollTimeout else 1000L

  def spark(): SparkSession = SparkSession.builder.getOrCreate

  def df(topicName: String): Dataset[Row] = {
    val (rdd, schema) = rdd_schema(topicName)
    spark.createDataFrame( rdd , schema )
  }

  def rdd(topicName: String): RDD[Row] = rdd_schema(topicName)._1

  def rdd_schema(topicName: String): (RDD[Row], StructType) = {
    val props = new Properties()
    val groupId = "spark-consumer-s2s-ktdf"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId +"-"+ UUID.randomUUID())
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ExternalizableDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[Integer, Externalizable](props)
    consumer.subscribe(util.Arrays.asList(topicName))

    var records = Iterable.empty[ConsumerRecord[Integer, Externalizable]]
    var newRecords = consumer.poll(pollTimeout).asScala // records: Iterable[ConsumerRecord[Integer, Externalizable]]
    records = records ++ newRecords

    while( newRecords.nonEmpty ) {
      newRecords = consumer.poll(shortTimeout).asScala // records: Iterable[ConsumerRecord[Integer, Externalizable]]
      records = records ++ newRecords
    }
    consumer.close

    if (records.isEmpty) { throw new Exception(s"Kafka poll timed out after ${pollTimeout/1000.0} seconds.") }
    else if(records.size == 1) {
      val (row, schema) = rowFrom(records.head)
      ( spark.sparkContext.parallelize( if(row.size > 0) { Seq(row) } else { Seq[Row]() } ),
        schema
      )
    } else {
      val seqBuilder = Seq.newBuilder[Row]
      var schema = new StructType
      for (record <- records.iterator) {
        val rs = rowFrom(record)
        seqBuilder += rs._1
        schema = rs._2
      }

      val rows = seqBuilder.result
      val rdd = spark.sparkContext.parallelize(rows)
      (rdd, schema)
    }
  }
  
  // Needed for SSDS
  def rowFrom(record: ConsumerRecord[Integer, Externalizable]): (Row, StructType) = {
    val row = record.value.asInstanceOf[Row]
    val values = for (i <- 0 until row.length) yield { // convert each column of the row
      if( row.isNullAt(i) ) { null }
      else {
        querySchema(i).dataType match {
          case BinaryType => row.getAs[Array[Byte]](i)
          case BooleanType => row.getBoolean(i)
          case ByteType => row.getByte(i)
          case DateType => row.getDate(i)
          case t: DecimalType => row.getDecimal(i)
          case DoubleType => row.getDouble(i)
          case FloatType => row.getFloat(i)
          case IntegerType => row.getInt(i)
          case LongType => row.getLong(i)
          case ShortType => row.getShort(i)
          case StringType => row.getString(i)
          case TimestampType => row.getTimestamp(i)
          case _ => throw new IllegalArgumentException(s"Can't get data for ${row.schema(i).dataType.simpleString}")
        }
      }
    }
    (Row.fromSeq(values), querySchema)
  }
}
