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

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class ShuffledPartitionsRDD[T: ClassTag]  (               @transient private var _parent: RDD[T],
                                            @transient private var _sc: SparkContext,
                                            @transient private var deps: Seq[Dependency[_]])
  extends RDD[T](_sc, deps) {

  def this ( prev: RDD[T]) = {
    this(prev, prev.context, List(new ShuffledDependency[T](prev)))
  }

  private var _partitions = {
    def dep = deps.head.asInstanceOf[ShuffledDependency[T]]
    def parentPartitions = firstParent[T].partitions
    val partitions = Array.ofDim[Partition](parentPartitions.length)
    for( i <- 0 to parentPartitions.length - 1) {
      partitions(i) = new ShuffledPartition[T](i, _parent, dep.partitions(i))
    }
    partitions
  }

  override def getPartitions: Array[Partition] = _partitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    def partition = split.asInstanceOf[ShuffledPartition[T]]
    firstParent[T].compute(partition.parentPartition, context)
  }

  override def clearDependencies() {
    super.clearDependencies()
    deps = null
    _parent = null
    _partitions = null
  }
  
}
