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

import com.splicemachine.primitives.Bytes
import java.security.SecureRandom
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

@SerialVersionUID(20200518241L)
@SuppressFBWarnings(value = Array("NP_ALWAYS_NULL"), justification = "Field 'unused' initialization is not null")
class KafkaTopics(kafkaServers: String, defaultNumPartitions: Int = 1, defaultRepFactor: Short = 1) extends Serializable {
  val admin = new KafkaAdmin(kafkaServers)
  val unneeded = collection.mutable.HashSet[String]()

  val unused = collection.mutable.Queue.fill(5)(createTopic())
  
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

  def delete(topicName: String): Unit = unneeded += topicName

  def cleanup(timeoutMs: Long = 0): Unit = admin.deleteTopics(unneeded, timeoutMs)
}
