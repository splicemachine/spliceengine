package org.apache.spark.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{NarrowDependency, Partition, TaskContext}
import scala.reflect.ClassTag

private[spark] class RandomStageRDDPartition(idx: Int, val parentSplit: Partition)
  extends Partition {
  override val index = idx
}


/**
  * Represents a dependency between the PartitionPruningRDD and its parent. In this
  * case, the child RDD contains a subset of partitions of the parents'.
  */
private[spark] class RandomStageDependency[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean)
  extends NarrowDependency[T](rdd) {

  @transient
  val partitions: Array[Partition] = rdd.partitions
    .filter(s => partitionFilterFunc(s.index)).zipWithIndex
    .map { case(split, idx) => new RandomStageRDDPartition(idx, split) : Partition }

  override def getParents(partitionId: Int): List[Int] = {
    List(partitions(partitionId).asInstanceOf[RandomStageRDDPartition].parentSplit.index)
  }
}


/**
  * :: DeveloperApi ::
  * An RDD used to prune RDD partitions/partitions so we can avoid launching tasks on
  * all partitions. An example use case: If we know the RDD is partitioned by range,
  * and the execution DAG has a filter on the key, we can avoid launching tasks
  * on partitions that don't have the range covering the key.
  */
@DeveloperApi
class RandomStageRDD[T: ClassTag](
                                        prev: RDD[T],
                                        partitionFilterFunc: Int => Boolean)
  extends RDD[T](prev.context, List(new RandomStageDependency(prev, partitionFilterFunc))) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(
      split.asInstanceOf[RandomStageRDDPartition].parentSplit, context)
  }

  override protected def getPartitions: Array[Partition] =
    dependencies.head.asInstanceOf[RandomStageDependency[T]].partitions
}


@DeveloperApi
object RandomStageRDD {

  /**
    * Create a PartitionPruningRDD. This function can be used to create the PartitionPruningRDD
    * when its type T is not known at compile time.
    */
  def create[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean): RandomStageRDD[T] = {
    new RandomStageRDD[T](rdd, partitionFilterFunc)(rdd.elementClassTag)
  }
}

