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

@SerialVersionUID(20200518241L)
class KafkaTopics(kafkaServers: String) extends Serializable {
  val unneeded = collection.mutable.HashSet[String]()

  def create(): String = {
    val name = new Array[Byte](32)
    new SecureRandom().nextBytes(name)
    Bytes.toHex(name)+"-"+System.nanoTime()
  }
  
  def delete(topicName: String): Unit = unneeded += topicName
  
  def cleanup(timeoutMs: Long = 0): Unit = new KafkaAdmin(kafkaServers).deleteTopics(unneeded, timeoutMs)
}
