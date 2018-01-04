package com.splicemachine.spark.splicemachine.streaming

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by jleach on 11/15/17.
  */
case class SpliceSink(
                       sqlContext: SQLContext,
                       parameters: Map[String, String],
                       partitionColumns: Seq[String],
                       outputMode: OutputMode) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    println(s"addBatch($batchId)")
    data.explain()
    // Why so many lines just to show the input DataFrame?
    data.sparkSession.createDataFrame(
      data.sparkSession.sparkContext.parallelize(data.collect()), data.schema)
      .show(10)
  }
}