package com.splicemachine.db.impl.sql.execute

import java.sql.Timestamp
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.common.TopicExistsException
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}


object StatementLogger {

  var zkUtils: Option[ZkUtils] = None
  var producer: Option[KafkaProducer[String, String]] = None
  var kafkaReady: Boolean = false

  //Properties for Kafka
  //TBD initialize these properly in EngineLifeCycle
  val numPartitions = 10
  val replicationFactor = 1
  val sessionTimeoutMs = 10000
  val connectionTimeoutMs = 10000

  val xidJSON: String = "XID: "
  val lccJSON: String = "SESSIONID: "
  val dbnameJSON: String = "DATABASE: "
  val drdaJSON: String = "DRDAID: "


  def init(): Unit = {

    println(s"""StatementLogger Init: zookeeperURL= $getZkURL , sessionTimeoutMs= $sessionTimeoutMs , connectionTimeoutMs= $connectionTimeoutMs """)
    try {
      // create a ZkUtils
      zkUtils match {
        case None => zkUtils = Option(ZkUtils.apply(getZkURL, sessionTimeoutMs, connectionTimeoutMs, false))
        case Some(_) =>
      }
    }
    catch {
      case exception =>
    }

    // TBD: MZ properties should be declarative and changeable upon install
    val props = new Properties
    props.put("bootstrap.servers", getKafkaBrokers)
    props.put("acks", "all")
    props.put("retries", new Integer(0))
    props.put("batch.size", new Integer(16384))
    props.put("linger.ms", new Integer(1))
    props.put("buffer.memory", new Integer(33554432))
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    // Create  topic propertie
    val topicConfig = new Properties

    try {
      println(s"""Creating TopicConfig: zkUtils= ${zkUtils.get.toString}, topicName= $getTopic , numPartitions= $numPartitions , replicationFactor= $replicationFactor , topicConfig= $topicConfig """)
      AdminUtils.createTopic(zkUtils.get, getTopic, numPartitions, replicationFactor, topicConfig)
    }
    catch {
      case exception: TopicExistsException =>
    }

    //Create Kafka producer
    producer = Option(new KafkaProducer[String, String](props))

    producer match {
      case Some(k: KafkaProducer[String, String]) => kafkaReady = true
      case None =>
    }
  }


  // TBD: MZ Service discovery required
  private def getZkURL: String = {
    "localhost:2181"
  }

  // TBD: MZ Service discovery required
  private def getKafkaBrokers: String = {
    "localhost:9092"
  }

  // TBD: MZ Service discovery required
  private def getTopic: String = {
    "myPrivateCluster"
  }

  def logStatement(stmt: String,
                   params: String,
                   lcc: String,
                   db: String,
                   drda: String,
                   txnId: String,
                   clkTime: Timestamp,
                   user:
                   String): Unit = {

    // initialize producer first time
    if (!kafkaReady) {
      init()
    }
    // handle null drda
    val drdaStr =
      drda

      match {
        case
          null => ""
        case _ => drda
      }

    if (kafkaReady) {
      //Consruct clock time : transaction Id as the key and a JSON value
      val keyString =
        s"""$clkTime:$txnId"""
      val paramsStr = params.toString
      val valueString =
        s"""{time: $clkTime,  txn: $txnId,  user: $user, stmt: $stmt, params:  $paramsStr, $drdaJSON: $drdaStr, dbnameJSON $db,  $lccJSON $lcc}"""
      val message = new ProducerRecord[String, String](
        getTopic,
        keyString,
        valueString)
      println(valueString)
      producer.get.send(message)
    }
  }
}

