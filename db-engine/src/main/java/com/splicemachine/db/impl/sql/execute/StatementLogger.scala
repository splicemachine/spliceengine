
package com.splicemachine.db.impl.sql.execute

import java.sql.Timestamp
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object StatementLogger {

  //Properties for zookeeper
  //TBD initialize these properly in EngineLifeCycle
  var zkUtils: ZkUtils = null
  var zookeeperURL = ""
  var brokers = ""
  var topicName = ""
  val numPartitions = 10
  val replicationFactor = 1
  val sessionTimeoutMs = 10000
  val connectionTimeoutMs = 10000


  def init(zkURL: String, zkBrokers: String, topic: String): Unit = {
    zookeeperURL = zkURL
    brokers = zkBrokers
    topicName = topic
    println(s"""zookeeperURL= $zookeeperURL , sessionTimeoutMs= $sessionTimeoutMs , connectionTimeoutMs= $connectionTimeoutMs """)
    // Create a ZooKeeper client
    zkUtils = ZkUtils.apply(zookeeperURL, sessionTimeoutMs, connectionTimeoutMs,
      false)

    // Create  topic
    val topicConfig = new Properties
    println(s"""zkUtils= ${zkUtils.toString} , topicName= $topicName , numPartitions= $numPartitions , replicationFactor= $replicationFactor  , topicConfig= $topicConfig """)
    AdminUtils.createTopic(zkUtils, topicName, numPartitions, replicationFactor, topicConfig)
  }

  private def getZkURL: String = {
    return "localhost:2181"
  }

  private def getKafkaBrokers: String = {
    return "localhost:9092"
  }

  private def getTopic: String = {
    return "myPrivateCluster"
  }

  def logStatement(stmt: String, txnId: String, clkTime: Timestamp, user: String): Unit = {


    if (zkUtils == null){
      init(getZkURL,getKafkaBrokers,getTopic)
    }

    //Add properties
    val props = new Properties
    props.put("bootstrap.servers", brokers)
    props.put("acks", "all")
    props.put("retries", new Integer(0))
    props.put("batch.size", new Integer(16384))
    props.put("linger.ms", new Integer(1))
    props.put("buffer.memory", new Integer(33554432))
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    //Create Kafka producer
    val producer = new KafkaProducer[String, String](props)

    //Consruct clock time : transaction Id as the key and a JSON value
    val keyString = s"""$clkTime:$txnId"""
    val valueString = s"""{time: $clkTime,  txn: $txnId,  user: $user , stmt: $stmt } """
    val message = new ProducerRecord[String, String](topicName, keyString, valueString)
    producer.send(message)
  }
}