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

import java.util.Properties
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.clients.admin.NewTopic
import com.splicemachine.test.LongerThanTwoMinutes
import org.junit.experimental.categories.Category

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
@Category(Array(classOf[LongerThanTwoMinutes]))
class KafkaMaintenanceIT extends FunSuite with Matchers with BeforeAndAfterAll {

  var kafka: KafkaServerStartable = _
  
  val id = "730"
  val kafkaPort = s"19$id"
  val kafkaServer = s"localhost:$kafkaPort"
  
  val admin = KafkaMaintenance.adminClient(kafkaServer)

  val tmpDir = System.getProperty("java.io.tmpdir", "target/tmp")
  val dataFile = Paths.get( s"$tmpDir/splice_spark2-${getClass.getSimpleName}-data-$id.txt" )

  override def beforeAll(): Unit = {
    val props = new Properties()
    props.put("zookeeper.connect", "localhost:2181")
    props.put("broker.id", id)
    props.put("port", kafkaPort)
    props.put("offsets.topic.replication.factor", "1")  // helps splice standalone work on Kafka 2.2
    props.put("log.dir", s"$tmpDir/kafka-logs-$id")

    kafka = new KafkaServerStartable(new KafkaConfig(props))
    kafka.startup
    
    clean
    
    println(s"${getClass.getSimpleName} may take a few minutes to run...")
  }

  override def afterAll(): Unit = {
    clean
    admin.close
    kafka.shutdown
  }
  
  def work(): Unit = {
    KafkaMaintenance.deleteOldTopics( kafkaServer , dataFile.toString , 1 )
    org.junit.Assert.assertTrue(
      s"File not found: $dataFile",
      Files.exists(dataFile)
    )
    Thread.sleep(1000)
  }
  
  def createTopics(topicNames: Seq[String]): Unit = {
    admin.createTopics(
      topicNames.map(new NewTopic(_,1,1)).asJava
    ).all.get
    Thread.sleep(1000)
  }

  def deleteAllTopics(): Unit = {
    admin.deleteTopics( kafkaTopics ).all.get
    Thread.sleep(1000)
  }

  def kafkaTopics(): util.Set[String] = admin.listTopics.names.get
  
  def clean(): Unit = {
    deleteAllTopics
    Files.deleteIfExists(dataFile)
  }
  
  def past(): Long = Instant.now.minusSeconds(61).toEpochMilli
  def now(): Long = Instant.now.toEpochMilli
  def makeTopicsOld(): Unit = Thread.sleep(61000)


  test("Test Create Empty Datafile") {
    clean
    
    work
    
    org.junit.Assert.assertTrue(
      s"Unexpected content in: $dataFile",
      Files.lines(dataFile).count == 0
    )
  }

  test("Test Unchanged Empty Datafile") {
    clean
    Files.createFile(dataFile)

    work

    org.junit.Assert.assertTrue(
      s"Unexpected content in: $dataFile",
      Files.lines(dataFile).count == 0
    )
  }

  test("Test Create Datafile") {
    clean
    createTopics(Seq("CD"))
    
    work

    val kt = kafkaTopics()

    org.junit.Assert.assertTrue(
      "Topics not found in Kafka",
      kt.contains("CD")
    )
    org.junit.Assert.assertTrue(
      s"Unexpected content in: $dataFile",
      Files.lines(dataFile).count == 1 && Files.readAllLines(dataFile).asScala.exists(_.startsWith("CD,"))
    )
  }

  test("Test Update Empty Datafile") {
    clean
    createTopics(Seq("UED"))
    Files.createFile(dataFile)

    work

    val kt = kafkaTopics()

    org.junit.Assert.assertTrue(
      "Topics not found in Kafka",
      kt.contains("UED")
    )
    org.junit.Assert.assertTrue(
      s"Unexpected content in: $dataFile",
      Files.lines(dataFile).count == 1 && Files.readAllLines(dataFile).asScala.exists(_.startsWith("UED,"))
    )
  }

  test("Test Unknown Topic Removal") {
    clean
    Files.write( dataFile, util.Arrays.asList(s"Unknown,$past") )

    val kt = kafkaTopics()

    org.junit.Assert.assertFalse(
      "Topics found in Kafka",
      kt.contains("Unknown")
    )
    
    work

    org.junit.Assert.assertTrue(
      s"Unexpected content in: $dataFile",
      Files.lines(dataFile).count == 0
    )
  }

  test("Test All Young Topics") {
    clean
    createTopics(Seq("AYT1","AYT2"))
    val in = util.Arrays.asList(s"AYT1,$now", s"AYT2,$now")
    Files.write( dataFile , in )

    work
    
    val kt = kafkaTopics()

    org.junit.Assert.assertTrue(
      "Topics not found in Kafka",
      kt.contains("AYT1") && kt.contains("AYT2")
    )
    org.junit.Assert.assertEquals(
      s"Unexpected content in: $dataFile",
      in.asScala.sorted.mkString("; "),
      Files.readAllLines(dataFile).asScala.sorted.mkString("; ")
    )
  }

  test("Test All Old Topics") {
    clean
    createTopics(Seq("AOT1","AOT2"))
    val in = util.Arrays.asList(s"AOT1,$now", s"AOT2,$now")
    Files.write( dataFile , in )

    var kt = kafkaTopics()

    org.junit.Assert.assertTrue(
      "Topics not found in Kafka",
      kt.contains("AOT1") && kt.contains("AOT2")
    )

    makeTopicsOld
    
    work

    kt = kafkaTopics()

    org.junit.Assert.assertFalse(
      "Topics found in Kafka",
      kt.contains("AOT1") || kt.contains("AOT2")
    )
    org.junit.Assert.assertTrue(
      s"Unexpected content in: $dataFile",
      Files.lines(dataFile).count == 0
    )
  }

  test("Test Some Old and Young Topics") {
    clean
    createTopics(Seq("SOT1","SOT2"))
    val o1 = s"SOT1,$now"
    val o2 = s"SOT2,$now"
    makeTopicsOld

    createTopics(Seq("SYT1","SYT2"))
    val y1 = s"SYT1,$now"
    val y2 = s"SYT2,$now"
    val young = util.Arrays.asList(y1, y2)
    val youngStr = young.asScala.sorted.mkString("; ")
    val in = util.Arrays.asList(y1, y2, o1, o2)
    Files.write( dataFile , in )

    var kt = kafkaTopics()

    org.junit.Assert.assertTrue(
      "Topics not found in Kafka pre-work",
      kt.contains("SOT1") && kt.contains("SOT2") && kt.contains("SYT1") && kt.contains("SYT2")
    )

    work

    kt = kafkaTopics()

    org.junit.Assert.assertFalse(
      "Topics found in Kafka",
      kt.contains("SOT1") || kt.contains("SOT2")
    )
    org.junit.Assert.assertTrue(
      "Topics not found in Kafka post-work",
      kt.contains("SYT1") && kt.contains("SYT2")
    )
    org.junit.Assert.assertEquals(
      s"Unexpected content in: $dataFile",
      youngStr,
      Files.readAllLines(dataFile).asScala.sorted.mkString("; ")
    )
  }

  test("Test All Young Topics with New Topics") {
    clean
    createTopics(Seq("AYTn1","AYTn2"))
    val in = util.Arrays.asList(s"AYTn1,$now", s"AYTn2,$now")
    Files.write( dataFile , in )
    createTopics(Seq("NTy1","NTy2"))

    work

    val kt = kafkaTopics()

    org.junit.Assert.assertTrue(
      "Topics not found in Kafka",
      kt.contains("AYTn1") && kt.contains("AYTn2") && kt.contains("NTy1") && kt.contains("NTy2")
    )
    org.junit.Assert.assertEquals(
      s"Unexpected content in: $dataFile",
      "AYTn1, AYTn2, NTy1, NTy2",
      Files.readAllLines(dataFile).asScala.sorted.map(_.split(",")(0)).mkString(", ")
    )
  }

  test("Test All Old Topics with New Topics") {
    clean
    createTopics(Seq("AOTn1","AOTn2"))
    val in = util.Arrays.asList(s"AOTn1,$now", s"AOTn2,$now")
    Files.write( dataFile , in )
    makeTopicsOld

    createTopics(Seq("NTo1","NTo2"))

    var kt = kafkaTopics()

    org.junit.Assert.assertTrue(
      "Topics not found in Kafka pre-work",
      kt.contains("AOTn1") && kt.contains("AOTn2") && kt.contains("NTo1") && kt.contains("NTo2")
    )

    work

    kt = kafkaTopics()

    org.junit.Assert.assertFalse(
      "Topics found in Kafka",
      kt.contains("AOTn1") || kt.contains("AOTn2")
    )
    org.junit.Assert.assertTrue(
      "Topics not found in Kafka post-work",
      kt.contains("NTo1") && kt.contains("NTo2")
    )
    org.junit.Assert.assertEquals(
      s"Unexpected content in: $dataFile",
      "NTo1, NTo2",
      Files.readAllLines(dataFile).asScala.sorted.map(_.split(",")(0)).mkString(", ")
    )
  }

  test("Test Some Old and Young Topics with New Topics") {
    clean
    createTopics(Seq("SOTn1","SOTn2"))
    val o1 = s"SOTn1,$now"
    val o2 = s"SOTn2,$now"
    makeTopicsOld

    createTopics(Seq("NToy1","NToy2"))
    createTopics(Seq("SYTn1","SYTn2"))
    val y1 = s"SYTn1,$now"
    val y2 = s"SYTn2,$now"
    val in = util.Arrays.asList(y1, y2, o1, o2)
    Files.write( dataFile , in )

    var kt = kafkaTopics()

    org.junit.Assert.assertTrue(
      "Topics not found in Kafka pre-work",
      kt.contains("SOTn1") && kt.contains("SOTn2") 
        && kt.contains("NToy1") && kt.contains("NToy2")
        && kt.contains("SYTn1") && kt.contains("SYTn2")
    )

    work

    kt = kafkaTopics()

    org.junit.Assert.assertFalse(
      "Topics found in Kafka",
      kt.contains("SOTn1") || kt.contains("SOTn2")
    )
    org.junit.Assert.assertTrue(
      "Topics not found in Kafka post-work",
      kt.contains("NToy1") && kt.contains("NToy2")
        && kt.contains("SYTn1") && kt.contains("SYTn2")
    )
    org.junit.Assert.assertEquals(
      s"Unexpected content in: $dataFile",
      "NToy1, NToy2, SYTn1, SYTn2",
      Files.readAllLines(dataFile).asScala.sorted.map(_.split(",")(0)).mkString(", ")
    )
  }
}
