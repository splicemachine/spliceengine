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
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.KafkaFuture
import scala.collection.JavaConverters._

class KafkaAdmin(kafkaServers: String) {
  val props = new Properties()
  props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
  val admin = AdminClient.create( props )
  
  def listTopics(): collection.mutable.Set[String] = admin.listTopics.names.get.asScala

  /**
   * Deletes topics from Kafka.
   * 
   * @param topicNames Set of names of topics to be deleted. A name is removed from the Set after its topic has been 
   *                   deleted, so the names of any topics that didn't get deleted will remain when the function is done.
   * @param timeoutMs Number of milliseconds in which the function must complete.
   */
  def deleteTopics(topicNames: collection.mutable.Set[String], timeoutMs: Long = 0): Unit = {
    val futures = admin.deleteTopics( topicNames.asJava )
    
    if( timeoutMs < 1 ) {
      try {
        futures.all.get
        topicNames.clear
      } catch {
        case e: Throwable => throw e
      }
    } else {
      val now = () => System.currentTimeMillis
      val deadline = now() + timeoutMs
      val futureForTopic: collection.mutable.Map[String,KafkaFuture[Void]] = futures.values.asScala
      while( !futureForTopic.isEmpty && now() < deadline ) {
        val (topicName, future) = futureForTopic.head
        try {
          future.get(deadline - now(), java.util.concurrent.TimeUnit.MILLISECONDS)
          futureForTopic -= topicName
          topicNames -= topicName
        } catch {
          case exeExce: java.util.concurrent.ExecutionException => {
            if( exeExce.getMessage.contains("org.apache.kafka.common.errors.UnknownTopicOrPartitionException") ) {
              futureForTopic -= topicName
              topicNames -= topicName
            }
            else {
              throw exeExce
            }
          }
          case others: Throwable => throw others
        }
      }
    }
  }
}
