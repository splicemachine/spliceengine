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

package com.splicemachine.spark.splicemachine

import java.io.{IOException, ObjectOutputStream}

import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
 * Partition for ShuffledPartitionsRDD.
 *
 * @param idx index of the partition
 * @param rdd the parent RDD this partition refers to
 * @param parentPartitionIndex index of the partition within the parent RDD
 *                                this partition refers to
 */
private[spark] class ShuffledPartition[T: ClassTag](
    idx: Int,
    @transient private val rdd: RDD[T],
    @transient private val parentPartitionIndex: Int)
  extends Partition {

  var parentPartition: Partition = rdd.partitions(parentPartitionIndex)

  def preferredLocations(): Seq[String] = rdd.preferredLocations(parentPartition)

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    parentPartition = rdd.partitions(parentPartitionIndex)
    oos.defaultWriteObject()
  }

  private def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        LogHolder.log.error("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        LogHolder.log.error("Exception encountered", e)
        throw new IOException(e)
    }
  }
}


object LogHolder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
