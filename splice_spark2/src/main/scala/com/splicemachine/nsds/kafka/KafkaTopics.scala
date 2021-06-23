/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

import com.splicemachine.primitives.Bytes
import java.security.SecureRandom
import java.util.concurrent.LinkedTransferQueue
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

@SerialVersionUID(20200518241L)
@SuppressFBWarnings(value = Array("NP_ALWAYS_NULL"), justification = "Field 'unused' initialization is not null")
class KafkaTopics(
    kafkaServers: String, 
    defaultNumPartitions: Int = 1, 
    defaultRepFactor: Short = 1,
    continuousCleanup: Boolean = true
  ) extends Serializable
{
  private val admin = new KafkaAdmin(kafkaServers)

  private val unneeded = new LinkedTransferQueue[String]()
  private val processing = new AtomicBoolean(true)

  private val unused = collection.mutable.Queue.fill(5)(createTopic())

  private def deleteTopics(): Unit = {
    val toDelete = collection.mutable.Set.empty[String]
    unneeded.drainTo(toDelete.asJava)
    println(s"Deleting topics $toDelete")  // todo remove println
    admin.deleteTopics(toDelete, 60*1000)  // todo will admin.deleteTopics accept an empty toDelete when there are no topics?
    println(s"Delete done")  // todo remove println
  }
  
  if(continuousCleanup) {
    new Thread {
      override def run {
        while (processing.get) {
          deleteTopics
          Thread.sleep(60 * 1000)
        }
        deleteTopics
      }
    }.start
  }

  def create(): String = {
    unused.enqueue(createTopic())
    unused.dequeue
  }

  def createTopic(numPartitions: Int = defaultNumPartitions, repFactor: Short = defaultRepFactor): String = {
    val name = new Array[Byte](4)
    new SecureRandom().nextBytes(name)
    val topicName = s"SM-NSDSv2-${Bytes.toHex(name)}-${System.nanoTime()}"
    
    admin.createTopics(
      collection.mutable.HashSet( topicName ),
      numPartitions,
      repFactor
    )
    topicName
  }

  def delete(topicName: String): Unit = unneeded.put(topicName)

  def shutdown(): Unit = {
    processing.compareAndSet(true, false)
    if(!continuousCleanup) deleteTopics
  }
}
