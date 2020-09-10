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

import java.sql.SQLIntegrityConstraintViolationException

import scala.collection.immutable.IndexedSeq
import org.apache.spark.sql._
import org.junit.runner.RunWith
import org.junit.Assert._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.functions._
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}

import com.splicemachine.derby.impl.SpliceSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable.Queue

@RunWith(classOf[JUnitRunner])
class SparkStreamingIT extends FunSuite with TestStreamingContext with BeforeAndAfter with Matchers {
  val rowCount = 10
  var sqlContext : SQLContext = _
  var rows : IndexedSeq[(Int, Int, String, Long)] = _
  var session : SparkSession = _

  before {
    val rowCount = 10

    ssc = new StreamingContext(conf, Milliseconds(50))

    SpliceSpark.setContext(ssc.sparkContext)
    session = SpliceSpark.getSessionUnsafe
    splicemachineContext = new SplicemachineContext(defaultJDBCURL)
    
    sqlContext = SpliceSpark.getSessionUnsafe.sqlContext
    if (splicemachineContext.tableExists(internalTN)) {
      splicemachineContext.dropTable(internalTN)
    }

    val conn = JdbcUtils.createConnectionFactory(internalJDBCOptions)()
    conn.createStatement().execute("create table "+internalTN + this.allTypesCreateStringWithPrimaryKey)

    sqlContext.read.options(internalOptions).splicemachine.createOrReplaceTempView(table)
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }

  test("basic streaming working") {
    val queue:Queue[RDD[Row]] = Queue()

    val results = new ListenableQueue[Row]

    val dstream = ssc.queueStream(queue, true, null)
    enqueueRows(queue, 10)

    dstream.foreachRDD(rdd => results ++= rdd.collect() )

    ssc.start() // Start the computation
    results.waitFor(10) // block until there are 10 rows in results (or 1 minute passes)

    assert(results.size === 10)
  }

 test("multi streams working") {
   val queue:Queue[RDD[Row]] = Queue()

   val results = new ListenableQueue[Row]

   val dstream = ssc.queueStream(queue, true, null)
   dstream.foreachRDD(rdd => results ++= rdd.collect() )
   enqueueRows(queue, 10)

   ssc.start() // Start the computation
   results.waitFor(10) // block until there are 10 rows in results (or 1 minute passes)

   enqueueRows(queue, 10)
   enqueueRows(queue, 10)

   results.waitFor(30) // block until there are 30 rows in results (or 1 minute passes)

   assert(results.size === 30)
 }

 test("insertion") {
   val queue:Queue[RDD[Row]] = Queue()

   val oldDF = sqlContext.read.options(internalOptions).splicemachine
   assert(oldDF.count == 0)

   val results = new ListenableQueue[Boolean]

   val dstream = ssc.queueStream(queue, true, null)

   dstream.foreachRDD(rdd => {
     val df = sqlContext.createDataFrame(rdd, splicemachineContext.getSchema(internalTN))

     splicemachineContext.insert(df, internalTN)
     results.enqueue(true)
   })
   enqueueRows(queue, 10)

   ssc.start() // Start the computation
   results.waitFor(5) // block until there are 10 batches in results (or 1 minute passes)

   val newDF = sqlContext.read.options(internalOptions).splicemachine
   assert(newDF.count == 10)
 }

 test ("deletion") {
   val queue:Queue[RDD[Row]] = Queue()

   //insert initial rows

   insertInternalRows(10)

   val oldDF = sqlContext.read.options(internalOptions).splicemachine
   assert(oldDF.count == 10)

   val results = new ListenableQueue[Boolean]

   val dstream = ssc.queueStream(queue, true, null)

   dstream.foreachRDD(rdd => {
     val df = sqlContext.createDataFrame(rdd, splicemachineContext.getSchema(internalTN))

     val deleteDF = df.filter("c6_int < 5").select("C6_INT","C7_BIGINT")
     splicemachineContext.delete(deleteDF, internalTN)
     results.enqueue(true)
   })
   enqueueRows(queue, 10)

   ssc.start() // Start the computation
   results.waitFor(5) // block until there are 10 batches in results (or 1 minute passes)

   val newDF = sqlContext.read.options(internalOptions).splicemachine

   assert(newDF.count == 5)
 }

 test ("update") {
   val queue:Queue[RDD[Row]] = Queue()

   //insert initial rows
   insertInternalRows(10)

   val oldDF = sqlContext.read.options(internalOptions).splicemachine
   assert(oldDF.count == 10)

   val results = new ListenableQueue[Boolean]

   val dstream = ssc.queueStream(queue, true, null)

   dstream.foreachRDD(rdd => {
     val df = sqlContext.createDataFrame(rdd, splicemachineContext.getSchema(internalTN))

     val updatedDF = df
       .filter("C6_INT < 5")
       .select("C6_INT","C7_BIGINT","C8_FLOAT","C9_SMALLINT")
       .withColumn("C8_FLOAT", when(col("C8_FLOAT").leq(10.0), col("C8_FLOAT").plus(10.0)) )
     splicemachineContext.update(updatedDF, internalTN)
     results.enqueue(true)
   })
   enqueueRows(queue, 10)

   ssc.start() // Start the computation
   results.waitFor(5) // block until there are 10 batches in results (or 1 minute passes)

   val newDF = sqlContext.read.options(internalOptions).splicemachine

   assertEquals(5, newDF.filter("c8_float >= 10.0").count())
 }

  test ("join") {
    val queue:Queue[RDD[Row]] = Queue()

    //insert initial rows
    insertInternalRows(10)

    //create results table
    val conn = JdbcUtils.createConnectionFactory(internalJDBCOptions)()

    if (splicemachineContext.tableExists(resultTN)) {
      splicemachineContext.dropTable(resultTN)
    }
    conn.createStatement().execute("create table "+resultTN + this.resultTypesCreateString)

    val oldDF = sqlContext.read.options(internalOptions).splicemachine
    assert(oldDF.count == 10)

    val results = new ListenableQueue[Boolean]

    val dstream = ssc.queueStream(queue, true, null)

    dstream.foreachRDD(rdd => {
      val df = sqlContext.createDataFrame(rdd, splicemachineContext.getSchema(internalTN))

      val leftDF = df.filter("c6_int < 5").select("C6_INT","C7_BIGINT")
      val resultDF = leftDF.join(sqlContext.read.options(internalOptions).splicemachine, Seq("C6_INT","C7_BIGINT"))
        .select("C1_BOOLEAN", "C2_CHAR", "C3_DATE", "C4_DECIMAL")
      splicemachineContext.insert(resultDF, resultTN)
      results.enqueue(true)
    })
    enqueueRows(queue, 10)

    ssc.start() // Start the computation
    results.waitFor(5) // block until there are 5 batches in results (or 1 minute passes)

    val newDF = sqlContext.read.options(resultsOptions).splicemachine

    assert(newDF.count == 5)
  }

  test("conflicts") {
    val queue:Queue[RDD[Row]] = Queue()

    val oldDF = sqlContext.read.options(internalOptions).splicemachine
    assert(oldDF.count == 0)

    val results = new ListenableQueue[Boolean]
    val errors = new ListenableQueue[Exception]

    val dstream = ssc.queueStream(queue, true, null)

    dstream.foreachRDD(rdd => {
      try {
        val df = sqlContext.createDataFrame(rdd, splicemachineContext.getSchema(internalTN))

        splicemachineContext.insert(df, internalTN)
        results.enqueue(true)
      } catch {
        case e: Exception =>
          errors.enqueue(e)
      }
    })
    enqueueRows(queue, 10)
    enqueueRows(queue, 10)

    ssc.start() // Start the computation
    results.waitFor(5) // block until there are 10 batches in results (or 1 minute passes)
    errors.waitFor(5) // 5 batches raising exception

    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assert(newDF.count == 10)
  }

}

private class ListenableQueue[A] extends Queue[A] {

  var latch: CountDownLatch = null

  override def appendElem(elem: A): Unit = {
    synchronized {
      super.appendElem(elem)
      if (latch != null)
        latch.countDown()
    }
  }

  def waitFor(count:Int): Unit = {
    synchronized {
      latch = new CountDownLatch(count - size)
    }
    if (!latch.await(3, TimeUnit.MINUTES))
      throw new TimeoutException("Timeout reached")
  }
}
