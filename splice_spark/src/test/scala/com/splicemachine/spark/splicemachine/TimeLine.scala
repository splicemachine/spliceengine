/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
 */
package com.splicemachine.spark.splicemachine

import java.sql.{Connection, Timestamp}

import com.splicemachine.si.api.txn.WriteConflict
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.junit.Assert._

@RunWith(classOf[JUnitRunner])
class Timeline extends FunSuite with TimeLineWrapper with BeforeAndAfter with Matchers {

  val CHANGE_AT_ST = 0
  val CHANGE_AT_ET = 1
  val CHANGE_BETWEEN_ST_ET = 2

  val initialValue = 0
  var sqlContext: SQLContext = _

  val MAX_RETRIES: Integer = 2

  before {
    if (sqlContext == null)
      sqlContext = new SQLContext(sc)
    initialize(internalTN, firstId, initialValue)
    sqlContext.read.options(internalOptions).splicemachine.createOrReplaceTempView(table)
  }


  ignore("timeline is initialized") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    assert(splicemachineContext.tableExists(internalTN))
    assert(df.count === 1)
    assert(df.first().getTimestamp(1).equals(startOfTime) && df.first().getTimestamp(2).equals(endOfTime))
  }


  def intersection(id: Integer, t1: Timestamp, t2: Timestamp): DataFrame = {
    val df = sqlContext.sql(s"""SELECT $columnsInsertString FROM $table where Timeline_Id = $id and ((ST >= to_utc_timestamp('$t1','GMT') and ST < to_utc_timestamp('$t2','GMT')) or ((ST < to_utc_timestamp('$t1','GMT')) and (ET > to_utc_timestamp('$t1','GMT')))) """)
    df
  }

  ignore("initial timeline should overlap [12/1/2010 12/2/2010] with one interval") {
    val testST = Timestamp.valueOf("2010-12-01 00:00:00")
    val testET = Timestamp.valueOf("2010-12-10 00:00:00")
    val df = intersection(firstId, testST, testET)
    assert(df.count === 1)
  }


  /**
    * splitMiddle - The new delta interval is subsumed by one interval.
    *
    * ST------------ET
    * t1---t2         ==>   ST---t1 t1----t2 t2----ET
    *
    * Change the original interval to end at the start of the new delta interval
    * Create a new record for the delta and apply the delta value
    * Create a new record for the interval from the delta to the end of the original interval
    *
    * @param id          - the id of the timeline to update
    * @param t1          - the start of new delta
    * @param t2          - the end of the new delta
    * @param delta       - an integer increment to the timeline
    * @param persistence - CHANGE_AT_ST persists delta from t1 onwards
    *                    CHANGE_AT_ET persists delta from t2 onwards
    *                    CHANGE_BETWEEN_ST_ET persists delta during [t1 t2]
    *
    */
  def splitMiddle(id: Integer,
                  t1: Timestamp, t2: Timestamp,
                  delta: Long,
                  persistence: Int): Unit = {
    val df = sqlContext.read.options(internalOptions).splicemachine.where(s"TIMELINE_ID = $id AND ST < to_utc_timestamp('$t1','GMT') AND ET > to_utc_timestamp('$t2','GMT')")
    if (df.count() > 0) {

      /* Save old values */
      var oldVal = df.first().getLong(DF_VAL)
      var oldET = df.first().getTimestamp(DF_ET)

      /* Update containing interval to be the begin split */
      val updatedDF = df
        .filter(s"TIMELINE_ID = $id AND ST < to_utc_timestamp('$t1','GMT') AND ET > to_utc_timestamp('$t2','GMT')")
        .select("TIMELINE_ID", "ST", "ET", "VAL")
        .withColumn("ET", lit(t1))
      splicemachineContext.update(updatedDF, internalTN)

      /* calculate persistence */
      val firstValue: Long = persistence match {
        case CHANGE_AT_ST => oldVal + delta
        case CHANGE_AT_ET => oldVal
        case CHANGE_BETWEEN_ST_ET => oldVal + delta
        case _ => 0
      }

      /* Insert the two new splits */
      /* Note - the second new split will have delta added
                in the persistAfter method
       */
      val newDF = sqlContext.createDataFrame(Seq(
        (id, t1, t2, firstValue),
        (id, t2, oldET, oldVal)))
        .toDF("TIMELINE_ID", "ST", "ET", "VAL")
      splicemachineContext.insert(newDF, internalTN)
    }
  }

  ignore("split initial timeline with [12/1/2010 12/10/2010] CHANGE_AT_ET") {
    val testST = Timestamp.valueOf("2010-12-01 00:00:00")
    val testET = Timestamp.valueOf("2010-12-10 00:00:00")
    splitMiddle(0, testST, testET, 10, CHANGE_AT_ET);
    val df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    val vals = df.collectAsList().toString
    assertEquals("[[0], [0], [0]]", vals);
  }

  ignore("split initial timeline with [12/1/2010 12/10/2010] CHANGE_AT_ST") {
    val testST = Timestamp.valueOf("2010-12-01 00:00:00")
    val testET = Timestamp.valueOf("2010-12-10 00:00:00")
    splitMiddle(0, testST, testET, 10, CHANGE_AT_ST);
    val df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    val vals = df.collectAsList().toString
    assertEquals("[[0], [10], [0]]", vals);
  }

  ignore("split initial timeline with [12/1/2010 12/10/2010] CHANGE_BETWEEN_ST_ET ") {
    val testST = Timestamp.valueOf("2010-12-01 00:00:00")
    val testET = Timestamp.valueOf("2010-12-10 00:00:00")
    splitMiddle(0, testST, testET, 10, CHANGE_BETWEEN_ST_ET);
    val df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    val vals = df.collectAsList().toString
    assertEquals("[[0], [10], [0]]", vals);
  }

  /** *
    * splitAtEnd - Delta overlaps beginning of interval.
    *
    * ST------ET
    * t1---t2         ==>  ST---t2 t2----ET
    *
    * Change the interval to end at the end of the delta then add a split from end of delta to the end of interval
    *
    * @param id          - the id of the timeline to update
    * @param t1          - the start of new delta
    * @param t2          - the end of the new delta
    * @param delta       - an integer increment to the timeline
    * @param persistence - CHANGE_AT_ST persists delta from t1 onwards
    *                    CHANGE_AT_ET persists delta from t2 onwards
    *                    CHANGE_BETWEEN_ST_ET persists delta during [t1 t2]
    */
  def splitAtEnd(id: Integer,
                 t1: Timestamp, t2: Timestamp,
                 delta: Long,
                 persistence: Int): Unit = {
    val df = sqlContext.read.options(internalOptions).splicemachine
      .where(s"""TIMELINE_ID = $id AND ST >= to_utc_timestamp('$t1','GMT') AND ET > to_utc_timestamp('$t2','GMT') AND ST < to_utc_timestamp('$t2','GMT')""")

    if (df.count() > 0) {

      /* Save old values */
      var oldVal = df.first().getLong(DF_VAL)
      var oldST = df.first().getTimestamp(DF_ST)
      var oldET = df.first().getTimestamp(DF_ET)
      /* Update overlapping interval to be the begin split */

      /* calculate persistence */
      /* Note - the second new split will have delta added
          in the persistAfter method if required
 */
      val firstValue: Long = persistence match {
        case CHANGE_AT_ST => oldVal + delta
        case CHANGE_AT_ET => oldVal
        case CHANGE_BETWEEN_ST_ET => oldVal + delta
        case _ => 0
      }

      val updatedDF = df
        .filter(s"TIMELINE_ID = $id AND ST >= to_utc_timestamp('$t1','GMT') AND ET > to_utc_timestamp('$t2','GMT') AND ST < to_utc_timestamp('$t2','GMT')")
        .select("TIMELINE_ID", "ST", "ET", "VAL")
        .withColumn("ET", lit(t2))
        .withColumn("VAL", lit(firstValue))
      splicemachineContext.update(updatedDF, internalTN)

      /* Insert a new split after the delta */
      val newDF = sqlContext.createDataFrame(Seq((id, t2, oldET, oldVal))).toDF("TIMELINE_ID", "ST", "ET", "VAL")
      splicemachineContext.insert(newDF, internalTN)
    }
  }

  ignore("split at 12/1-12/10 and then from 11/1-12/5 - CHANGE_AT_ET") {
    val ST1 = Timestamp.valueOf("2010-12-01 00:00:00")
    val ET1 = Timestamp.valueOf("2010-12-10 00:00:00")
    val ST2 = Timestamp.valueOf("2010-11-01 00:00:00")
    val ET2 = Timestamp.valueOf("2010-12-05 00:00:00")
    splitMiddle(0, ST1, ET1, 10, CHANGE_AT_ET);
    /* [-inf 12/1 0] [12/1 12/10 0] [12/10 inf 0] */
    splitAtEnd(0, ST2, ET2, 10, CHANGE_AT_ET);
    /* [-inf 12/1 0] [12/1 12/5 0] [12/5 12/10 0] [12/10 inf 0] */
    val df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    val vals = df.collectAsList().toString
    assertEquals("[[0], [0], [0], [0]]", vals);
  }

  ignore("split at 12/1-12/10 and then from 11/1-12/5 - CHANGE_AT_ST") {
    val ST1 = Timestamp.valueOf("2010-12-01 00:00:00")
    val ET1 = Timestamp.valueOf("2010-12-10 00:00:00")
    val ST2 = Timestamp.valueOf("2010-11-01 00:00:00")
    val ET2 = Timestamp.valueOf("2010-12-05 00:00:00")
    splitMiddle(0, ST1, ET1, 10, CHANGE_AT_ST);
    /* [-inf 12/1 0] [12/1 12/10 10] [12/10 inf 0] */
    splitAtEnd(0, ST2, ET2, 10, CHANGE_AT_ST);
    /* [-inf 12/1 0] [12/1 12/05 20][12/05 12/10 10][12/10 inf 0] */
    val df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    val vals = df.collectAsList().toString
    assertEquals("[[0], [20], [10], [0]]", vals);
  }

  ignore("split at 12/1-12/10 and then from 11/1-12/5 - CHANGE_BETWEEN_ST_ET") {
    val ST1 = Timestamp.valueOf("2010-12-01 00:00:00")
    val ET1 = Timestamp.valueOf("2010-12-10 00:00:00")
    val ST2 = Timestamp.valueOf("2010-11-01 00:00:00")
    val ET2 = Timestamp.valueOf("2010-12-05 00:00:00")
    splitMiddle(0, ST1, ET1, 10, CHANGE_BETWEEN_ST_ET);
    /* [-inf 12/1 0] [12/1 12/10 10][12/10 inf 0] */
    splitAtEnd(0, ST2, ET2, 10, CHANGE_BETWEEN_ST_ET);
    /* [-inf 12/1 0] [12/1 12/05 20][12/05 12/10 10][12/10 inf 0] */
    val df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    val vals = df.collectAsList().toString
    assertEquals("[[0], [20], [10], [0]]", vals);
  }

  /** *
    * splitAtStart - Delta overlaps end of interval.
    *
    * ST-----ET
    * t1------t2         ==>    ST---t1 t1---ET
    *
    * Change the interval to end at the start of the delta then add a split from beginning of delta to the end of interval
    *
    * @param id          - the id of the timeline to update
    * @param t1          - the start of new delta
    * @param t2          - the end of the new delta
    * @param delta       - an integer increment to the timeline
    * @param persistence - CHANGE_AT_ST persists delta from t1 onwards
    *                    CHANGE_AT_ET persists delta from t2 onwards
    *                    CHANGE_BETWEEN_ST_ET persists delta during [t1 t2]
    */
  def splitAtStart(id: Integer,
                   t1: Timestamp, t2: Timestamp,
                   delta: Long, persistence: Int): Unit = {
    val df = sqlContext.read.options(internalOptions).splicemachine.where(s"TIMELINE_ID = $id AND ST < to_utc_timestamp('$t1','GMT') AND ET < to_utc_timestamp('$t2','GMT') AND ET > to_utc_timestamp('$t1','GMT')")
    if (df.count() > 0) {

      /* Save old values */
      var oldVal = df.first().getLong(DF_VAL)
      var oldST = df.first().getTimestamp(DF_ST)
      var oldET = df.first().getTimestamp(DF_ET)

      /* calculate persistence */
      val newValue: Long = persistence match {
        case CHANGE_AT_ST => oldVal + delta
        case CHANGE_AT_ET => oldVal
        case CHANGE_BETWEEN_ST_ET => oldVal
        case _ => 0
      }
      /* Update overlapping interval to be the begin split */
      val updatedDF = df
        .filter(s"TIMELINE_ID = $id AND ST < to_utc_timestamp('$t1','GMT') AND ET < to_utc_timestamp('$t2','GMT') AND ET > to_utc_timestamp('$t1','GMT')")
        .select("TIMELINE_ID", "ST", "ET", "VAL")
        .withColumn("ET", lit(t1))
        .withColumn("VAL", lit(newValue))
      splicemachineContext.update(updatedDF, internalTN)

      /* Insert a new split */
      val newDF = sqlContext.createDataFrame(Seq(
        (id, t1, oldET, oldVal)
      )).toDF("TIMELINE_ID", "ST", "ET", "VAL")
      splicemachineContext.insert(newDF, internalTN)
    }
  }

  ignore("split at 12/1-12/10 and then from 12/5-12/13 - CHANGE_AT_ET") {
    val ST1 = Timestamp.valueOf("2010-12-01 00:00:00")
    val ET1 = Timestamp.valueOf("2010-12-10 00:00:00")
    val ST2 = Timestamp.valueOf("2010-12-05 00:00:00")
    val ET2 = Timestamp.valueOf("2010-12-13 00:00:00")
    splitMiddle(0, ST1, ET1, 10, CHANGE_AT_ET);
    /* [-inf 12/1 0] [12/1 12/10 0][12/10 inf 0] */
    splitAtStart(0, ST2, ET2, -10, CHANGE_AT_ET);
    /* [-inf 12/1 0] [12/1 12/05 0][12/05 12/10 0][12/10 inf 0] */
    val df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    val vals = df.collectAsList().toString
    assertEquals("[[0], [0], [0], [0]]", vals);
  }

  /** *
    * changeNoSplit - Handles all intervals contained by delta
    *
    * ST-----ET
    * t1---------------t2
    *
    * No splits required since always initialized with infinite time, just need values changed
    *
    * @param id          - the id of the timeline to update
    * @param t1          - the start of new delta
    * @param t2          - the end of the new delta
    * @param delta       - an integer increment to the timeline
    * @param persistence - CHANGE_AT_ST persists delta from t1 onwards
    *                    CHANGE_AT_ET persists delta from t2 onwards
    *                    CHANGE_BETWEEN_ST_ET persists delta during [t1 t2]
    */
  def changeNoSplit(id: Integer,
                    t1: Timestamp, t2: Timestamp,
                    delta: Long,
                    persistence: Int): Unit = {
    val df = sqlContext.read.options(internalOptions).splicemachine
      .where(s"TIMELINE_ID = $id AND ST >= to_utc_timestamp('$t1','GMT') AND ET <= to_utc_timestamp('$t2','GMT')")

    /* Calculate persistence */
    val increment: Long = persistence match {
      case CHANGE_AT_ST => delta
      case CHANGE_AT_ET => 0
      case CHANGE_BETWEEN_ST_ET => delta
      case _ => 0
    }
    val updatedDF = df
      .filter(s"TIMELINE_ID = $id AND ST >= to_utc_timestamp('$t1','GMT') AND ET <= to_utc_timestamp('$t2','GMT')")
      .select("TIMELINE_ID", "ST", "ET", "VAL")
      .withColumn("VAL", col("VAL") + increment)

    splicemachineContext.update(updatedDF, internalTN)
  }


  /** *
    * persistAfter - changes the values for all intervals after delta
    *
    * t1---------------t2  ST-----ET
    *
    * No splits required since always initialized with infinite time, just need values changed
    *
    * @param id          - the id of the timeline to update
    * @param t1          - the start of new delta
    * @param t2          - the end of the new delta
    * @param delta       - an integer increment to the timeline
    * @param persistence - CHANGE_AT_ST persists delta from t1 onwards
    *                    CHANGE_AT_ET persists delta from t2 onwards
    *                    CHANGE_BETWEEN_ST_ET persists delta during [t1 t2]
    */
  def persistAfter(id: Integer,
                   t1: Timestamp, t2: Timestamp,
                   delta: Long,
                   persistence: Int): Unit = {

    /* Persist delta after new splits if necesary */
    if (persistence != CHANGE_BETWEEN_ST_ET) {
      val persistDF = sqlContext.read.options(internalOptions).splicemachine
        .filter(s"TIMELINE_ID = $id AND ST >= to_utc_timestamp('$t2','GMT')")
        .select("TIMELINE_ID", "ST", "ET", "VAL")
        .withColumn("VAL", col("VAL") + delta)
      splicemachineContext.update(persistDF, internalTN)
    }
  }

  /** *
    * update - increases/decreases the value for the interval
    * from the start, end or during the interval
    *
    * @param table       - the name of the timeline table
    * @param id          - the id of the timeline to update
    * @param t1          - the start of new delta
    * @param t2          - the end of the new delta
    * @param delta       - an integer increment to the timeline
    * @param persistence - CHANGE_AT_ST persists delta from t1 onwards
    *                    CHANGE_AT_ET persists delta from t2 onwards
    *                    CHANGE_BETWEEN_ST_ET persists delta during [t1 t2]
    */

  def update(table: String,
             id: Integer,
             t1: Timestamp, t2: Timestamp,
             delta: Long,
             persistence: Int): Unit = {
    changeNoSplit(id, t1, t2, delta, persistence)
    splitAtStart(id, t1, t2, delta, persistence)
    splitMiddle(id, t1, t2, delta, persistence)
    splitAtEnd(id, t1, t2, delta, persistence)
    persistAfter(id, t1, t2, delta, persistence)
  }

  ignore("updates") {
    val ST1 = Timestamp.valueOf("2010-12-01 00:00:00")
    val ET1 = Timestamp.valueOf("2010-12-10 00:00:00")
    val ST2 = Timestamp.valueOf("2010-12-05 00:00:00")
    val ET2 = Timestamp.valueOf("2010-12-07 00:00:00")
    val ST3 = Timestamp.valueOf("2010-5-05 00:00:00")
    val ET3 = Timestamp.valueOf("2010-9-07 00:00:00")

    update(internalTN, 0, ST1, ET1, 10, CHANGE_AT_ET)
    /* [-inf 12/1 0][12/1 12/10 0] [12/10 inf 10] */
    var df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    var vals = df.collectAsList().toString
    assertEquals("[[0], [0], [10]]", vals)

    update(internalTN, 0, ST2, ET2, -10, CHANGE_AT_ET)
    /* [-inf 12/1 0][12/1 12/5 0][12/5 12/7 0][12/7 12/10 -10] [12/10 inf 0] */
    df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    vals = df.collectAsList().toString
    assertEquals("[[0], [0], [0], [-10], [0]]", vals)

    update(internalTN, 0, ST2, ET2, 10, CHANGE_AT_ET)
    /* [-inf 12/1 0][12/1 12/5 0][12/5 12/7 10][12/7 12/10 0] [12/10 inf 10] */
    df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    vals = df.collectAsList().toString
    assertEquals("[[0], [0], [0], [0], [10]]", vals)

    update(internalTN, 0, ST3, ET3, 100, CHANGE_AT_ET)
    /* [-inf 5/5 0] [5/5 9/7 0] [9/7 12/1 100][12/1 12/5 100][12/5 12/7 100][12/7 12/10 100] [12/10 inf 110] */
    df = sqlContext.read.options(internalOptions).splicemachine
      .select("VAL")
    vals = df.collectAsList().toString
    assertEquals("[[0], [0], [100], [100], [100], [100], [110]]", vals)

  }


  /** Mini Application to demonstrate utility of Timelines
    *
    * purchaseOrder - create, delete, changeQty, changeDelivery
    * salesOrder - create, delete, changeQty, changeDelivery
    * transferOrder -create, delete, changeQty, changeDelivery
    *
    * To simplify we do not persist orders.
    * We simply track the inventory availabilty in TimeLines
    *
    */

  object Inventory {

    def create(id: Integer) {
        initialize(internalTN, id, 0)
    }
  }

  object PurchaseOrder {

    def create(part: Integer,
               orderDate: String,
               deliveryDate: String,
               qty: Long,
               retryCount: Integer = 0): Unit = {
      val conn: Connection = splicemachineContext.getConnection()
      try {
        conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(deliveryDate), qty, CHANGE_AT_ET)
        conn.commit()
      }
      catch {
          case exp: WriteConflict => {
            conn.rollback()
            conn.setAutoCommit(true)
            if (retryCount < MAX_RETRIES) {
              println("Retrying create PO" + part + " " + orderDate + " " + deliveryDate + " " + qty + " " + retryCount + 1)
              create(part, orderDate, deliveryDate, qty, retryCount + 1)
            }
            else {
              // put code here to handle PO not created
            }
          }
          case _: Throwable => println("Got some other kind of exception")
        }
      finally
      {
        conn.setAutoCommit(true)
      }
    }


    def delete(part: Integer,
               orderDate: String,
               deliveryDate: String,
               qty: Long,
               retryCount: Integer = 0): Unit = {
      val conn: Connection = splicemachineContext.getConnection()
      try {
        conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(deliveryDate), -qty, CHANGE_AT_ET)
        conn.commit()
      }
      catch {
        case exp: WriteConflict => {
          conn.rollback()
          conn.setAutoCommit(true)
          if (retryCount < MAX_RETRIES) {
            println("Retrying delete PO" + part + " " + orderDate + " " + deliveryDate + " " + qty + " " + retryCount + 1)
            delete(part, orderDate, deliveryDate, qty, retryCount + 1)
          }
          else {
            // put code here to handle too many re-tries
          }
        }
        case _: Throwable => println("Got some other kind of exception")
      }
      finally
      {
        conn.setAutoCommit(true)
      }

    }

    def changeQty(part: Integer,
                  orderDate: String,
                  deliveryDate: String,
                  originalQty: Long,
                  newQty: Long,
                  retryCount: Integer = 0): Unit = {
      val conn: Connection = splicemachineContext.getConnection()
      try {
        conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(deliveryDate), newQty - originalQty, CHANGE_AT_ET)
        conn.commit()
      }
      catch {
        case exp: WriteConflict => {
          conn.rollback()
          conn.setAutoCommit(true)
          if (retryCount < MAX_RETRIES) {

            changeQty(part, orderDate, deliveryDate, originalQty, newQty, retryCount + 1)
          }
          else {
            // put code here to handle too many re-tries
          }
        }
        case _: Throwable => println("Got some other kind of exception")
      }
      finally
      {
        conn.setAutoCommit(true)
      }
    }

    def changeDelivery(part: Integer,
                       orderDate: String,
                       originalDeliveryDate: String,
                       newDeliveryDate: String,
                       qty: Long,
                       retryCount: Integer = 0): Unit = {
      val conn: Connection = splicemachineContext.getConnection()
      try {
        conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(originalDeliveryDate), -qty, CHANGE_AT_ET)
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(newDeliveryDate), qty, CHANGE_AT_ET)
        conn.commit()
      }
      catch {
        case exp: WriteConflict => {
          conn.rollback()
          conn.setAutoCommit(true)
          if (retryCount < MAX_RETRIES) {
            changeDelivery(part, orderDate, originalDeliveryDate, newDeliveryDate, qty, retryCount + 1)
          }
          else {
            // put code here to handle PO not created
          }
        }
        case _: Throwable => println("Got some other kind of exception")
      }
      finally
      {
        conn.setAutoCommit(true)
      }
    }
  }

  object SalesOrder {

    def create(part: Integer,
               orderDate: String,
               deliveryDate: String,
               qty: Long,
               retryCount: Integer = 0): Unit = {
      val conn: Connection = splicemachineContext.getConnection()
      try {
        conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(deliveryDate), -qty, CHANGE_AT_ST)
        conn.commit()
      }
      catch {
        case exp: WriteConflict => {
          conn.rollback()
          conn.setAutoCommit(true)
          if (retryCount < MAX_RETRIES) {
            println("Retrying create PO" + part + " " + orderDate + " " + deliveryDate + " " + qty + " " + retryCount + 1)
            create(part, orderDate, deliveryDate, qty, retryCount + 1)
          }
          else {
            // put code here to handle PO not created
          }
        }
        case _: Throwable => println("Got some other kind of exception")
      }
      finally {
        conn.setAutoCommit(true)
      }

    }

    def delete(part: Integer,
               orderDate: String,
               deliveryDate: String,
               qty: Long,
               retryCount: Integer = 0): Unit = {
      val conn: Connection = splicemachineContext.getConnection()
      try {
        conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(deliveryDate), qty, CHANGE_AT_ET)
        conn.commit()
      }
      catch {
        case exp: WriteConflict => {
          conn.rollback()
          conn.setAutoCommit(true)
          if (retryCount < MAX_RETRIES) {
            println("Retrying delete PO" + part + " " + orderDate + " " + deliveryDate + " " + qty + " " + retryCount + 1)
            delete(part, orderDate, deliveryDate, qty, retryCount + 1)
          }
          else {
            // put code here to handle too many retries
          }
        }
        case _: Throwable => println("Got some other kind of exception")
      }
      finally {
        conn.setAutoCommit(true)
      }
    }

    def changeQty(part: Integer,
                  orderDate: String,
                  deliveryDate: String,
                  originalQty: Long,
                  newQty: Long,
                  retryCount: Integer = 0): Unit = {
      val conn: Connection = splicemachineContext.getConnection()
      try {
        conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(deliveryDate), originalQty - newQty, CHANGE_AT_ST)
        conn.commit()
      }
      catch {
        case exp: WriteConflict => {
          conn.rollback()
          conn.setAutoCommit(true)
          if (retryCount < MAX_RETRIES) {

            changeQty(part, orderDate, deliveryDate, originalQty, newQty, retryCount + 1)
          }
          else {
            // put code here to handle too many re-tries
          }
        }
        case _: Throwable => println("Got some other kind of exception")
      }
      finally {
        conn.setAutoCommit(true)
      }
    }

    def changeDelivery(part: Integer,
                       orderDate: String,
                       originalDeliveryDate: String,
                       newDeliveryDate: String,
                       qty: Long,
                       retryCount: Integer = 0): Unit = {
      val conn: Connection = splicemachineContext.getConnection()
      try {
        conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(originalDeliveryDate), qty, CHANGE_AT_ST)
        update(internalTN, part, Timestamp.valueOf(orderDate), Timestamp.valueOf(newDeliveryDate), -qty, CHANGE_AT_ST)
        conn.commit()
      }
      catch {
        case exp: WriteConflict => {
          conn.rollback()
          conn.setAutoCommit(true)
          if (retryCount < MAX_RETRIES) {
            changeDelivery(part, orderDate, originalDeliveryDate, newDeliveryDate, qty, retryCount + 1)
          }
          else {
            // put code here to too many re-tries
          }
        }
        case _: Throwable => println("Got some other kind of exception")
      }
      finally {
        conn.setAutoCommit(true)
      }
    }
  }

    object TransferOrder {

      var idCounter = 0;

      def createNoSave(source: Integer,
                 destination: Integer,
                 shippingDate: String,
                 deliveryDate: String,
                 qty: Long,
                 retryCount: Integer = 0): Unit = {
        val conn: Connection = splicemachineContext.getConnection()
        try {
          conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
          update(internalTN, source, Timestamp.valueOf(shippingDate), Timestamp.valueOf(deliveryDate), -qty, CHANGE_AT_ST)
          update(internalTN, destination, Timestamp.valueOf(shippingDate), Timestamp.valueOf(deliveryDate), qty, CHANGE_AT_ET)
          conn.commit()
        }
        catch {
          case exp: WriteConflict => {
            conn.rollback()
            conn.setAutoCommit(true)
            if (retryCount < MAX_RETRIES) {
              println("Retrying create TO" + source + " " + destination + " " + shippingDate + " " + deliveryDate + " " + qty + " " + retryCount + 1)
              createNoSave(source, destination, shippingDate, deliveryDate, qty, retryCount + 1)
            }
            else {
              // put code here to handle too many retries
            }
          }
          case _: Throwable => println("Got some other kind of exception")
        }
        finally {
          conn.setAutoCommit(true)
        }
      }

      def create(source: Integer,
                 destination: Integer,
                 shippingDate: String,
                 deliveryDate: String,
                 modDeliveryDate : String,
                 qty: Long,
                 retryCount: Integer = 0,
                 TO_Id: Integer,
                 supplier: String,
                 ASN: String,
                 container: String,
                 modeOfTransport: Integer,
                 carrier: Integer,
                 fromWeather: Integer,
                 toWeather: Integer,
                 latitude: Double,
                 longitude: Double,
                 sourceCity: Integer,
                 destinationCity: Integer,
                 PO_Id: Integer): Unit = {
        val conn: Connection = splicemachineContext.getConnection()
        try {
          conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
          update(internalTN, source, Timestamp.valueOf(shippingDate), Timestamp.valueOf(deliveryDate), -qty, CHANGE_AT_ST)
          update(internalTN, destination, Timestamp.valueOf(shippingDate), Timestamp.valueOf(deliveryDate), qty, CHANGE_AT_ET)
          save(source,destination,Timestamp.valueOf(shippingDate),Timestamp.valueOf(deliveryDate),Timestamp.valueOf(modDeliveryDate),qty,TO_Id,supplier,ASN,container,modeOfTransport,carrier,fromWeather,toWeather,latitude,longitude,sourceCity,destinationCity,PO_Id)
          conn.commit()
        }
        catch {
          case exp: WriteConflict => {
            conn.rollback()
            conn.setAutoCommit(true)
            if (retryCount < MAX_RETRIES) {
              println("Retrying create TO" + source + " " + destination + " " + shippingDate + " " + deliveryDate + " " + qty + " " + retryCount + 1)
              create(source, destination, shippingDate, deliveryDate,modDeliveryDate, qty, retryCount + 1, TO_Id,
                supplier,ASN,container,modeOfTransport,carrier,fromWeather,toWeather,latitude,longitude, sourceCity, destinationCity,PO_Id)
            }
            else {
              // put code here to handle too many retries
            }
          }
          case _: Throwable => println("Got some other kind of exception")
        }
        finally {
          conn.setAutoCommit(true)
        }
      }


      /**
        *
        * initialize (id startOfTime endOfTime value)
        * @return
        */
      def save(source: Integer,
               destination: Integer,
               shippingDate: Timestamp,
               deliveryDate: Timestamp,
               modDeliveryDate: Timestamp,
               qty: Long,
               TO_Id: Integer,
               supplier: String,
               ASN: String,
               container: String,
               modeOfTransport: Integer,
               carrier: Integer,
               fromWeather: Integer,
               toWeather: Integer,
               latitude: Double,
               longitude: Double,
               sourceCity: Integer,
               destinationCity: Integer,
               PO_Id: Integer): Unit = {

        val optionMap = Map(
          JDBCOptions.JDBC_TABLE_NAME -> TOTable,
          JDBCOptions.JDBC_URL -> defaultJDBCURL
        )
        val JDBCOps = new JDBCOptions(optionMap)
        val conn = JdbcUtils.createConnectionFactory(JDBCOps)()
        try {
          var ps = conn.prepareStatement("insert into " + TOTable + TOColumnsInsertString + TOColumnsInsertStringValues)
          ps.setLong(TO_TO_ID, TO_Id.toLong)
          ps.setLong(TO_PO_Id, PO_Id.toLong)
          ps.setLong(TO_ShipFrom, sourceCity.toLong)
          ps.setLong(TO_ShipTo, destinationCity.toLong)
          ps.setTimestamp(TO_ShipDate, shippingDate)
          ps.setTimestamp(TO_DeliveryDate, deliveryDate)
          ps.setTimestamp(TO_ModDeliveryDate, modDeliveryDate)
          ps.setLong(TO_SourceInventory, source.toLong)
          ps.setLong(TO_DestinationInventory, destination.toLong)
          ps.setLong(TO_Qty, qty)
          ps.setString(TO_Supplier, supplier)
          ps.setString(TO_ASN, ASN)
          ps.setString(TO_Container, container)
          ps.setShort(TO_TransportMode, modeOfTransport.toShort)
          ps.setLong(TO_Carrier, carrier.toLong)
          ps.setShort(TO_FromWeather, fromWeather.toShort)
          ps.setShort(TO_ToWeather, toWeather.toShort)
          ps.setDouble(TO_Latitude,latitude)
          ps.setDouble(TO_Longitude,longitude)
          ps.execute()
        } finally {
          conn.close()
        }
      }

      def delete(source: Integer,
                 destination: Integer,
                 shippingDate: String,
                 deliveryDate: String,
                 qty: Long,
                 retryCount: Integer = 0): Unit = {
        val conn: Connection = splicemachineContext.getConnection()
        try {
          conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
          update(internalTN, source, Timestamp.valueOf(shippingDate), Timestamp.valueOf(deliveryDate), qty, CHANGE_AT_ST)
          update(internalTN, destination, Timestamp.valueOf(shippingDate), Timestamp.valueOf(deliveryDate), -qty, CHANGE_AT_ET)
          conn.commit()
        }
        catch {
          case exp: WriteConflict => {
            conn.rollback()
            conn.setAutoCommit(true)
            if (retryCount < MAX_RETRIES) {
              delete(source, destination, shippingDate, deliveryDate, qty, retryCount + 1)
            }
            else {
              // put code here to handle too many retries
            }
          }
          case _: Throwable => println("Got some other kind of exception")
        }
        finally {
          conn.setAutoCommit(true)
        }
      }

      def changeQty(source: Integer,
                    destination: Integer,
                    shippingDate: String,
                    deliveryDate: String,
                    originalQty: Long,
                    newQty: Long,
                    retryCount: Integer = 0): Unit = {
        val conn: Connection = splicemachineContext.getConnection()
        try {
          conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
          update(internalTN, source, Timestamp.valueOf(shippingDate), Timestamp.valueOf(deliveryDate), originalQty - newQty, CHANGE_AT_ST)
          update(internalTN, destination, Timestamp.valueOf(shippingDate), Timestamp.valueOf(deliveryDate), originalQty - newQty, CHANGE_AT_ET)
          conn.commit()
        }
        catch {
          case exp: WriteConflict => {
            conn.rollback()
            conn.setAutoCommit(true)
            if (retryCount < MAX_RETRIES) {

              changeQty(source, destination, shippingDate, deliveryDate, originalQty, newQty, retryCount + 1)
            }
            else {
              // put code here to handle too many re-tries
            }
          }
          case _: Throwable => println("Got some other kind of exception")
        }
        finally {
          conn.setAutoCommit(true)
        }
      }

      def changeDeliveryNoSave(source: Integer,
                         destination: Integer,
                         shippingDate: String,
                         originalDeliveryDate: String,
                         newDeliveryDate: String,
                         qty: Long,
                         retryCount: Integer = 0): Unit = {
        val conn: Connection = splicemachineContext.getConnection()
        try {
          conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
          update(internalTN, source, Timestamp.valueOf(shippingDate), Timestamp.valueOf(originalDeliveryDate), qty, CHANGE_AT_ST)
          update(internalTN, destination, Timestamp.valueOf(shippingDate), Timestamp.valueOf(originalDeliveryDate), -qty, CHANGE_AT_ET)
          update(internalTN, source, Timestamp.valueOf(shippingDate), Timestamp.valueOf(newDeliveryDate), -qty, CHANGE_AT_ST)
          update(internalTN, destination, Timestamp.valueOf(shippingDate), Timestamp.valueOf(newDeliveryDate), qty, CHANGE_AT_ET)
          conn.commit()
        }
        catch {
          case exp: WriteConflict => {
            conn.rollback()
            conn.setAutoCommit(true)
            if (retryCount < MAX_RETRIES) {
              changeDeliveryNoSave(source, destination, shippingDate, originalDeliveryDate, newDeliveryDate, qty, retryCount + 1)
            }
            else {
              // put code here to too many re-tries
            }
          }
          case _: Throwable => println("Got some other kind of exception")
        }
        finally {
          conn.setAutoCommit(true)
        }
      }

      def changeDelivery(source: Integer,
                         destination: Integer,
                         shippingDate: String,
                         originalDeliveryDate: String,
                         newDeliveryDate: String,
                         qty: Long,
                         retryCount: Integer = 0,
                         TO_Id: Integer,
                         supplier: String,
                         modeOfTransport: Integer,
                         carrier: Integer,
                         fromWeather: Integer,
                         toWeather: Integer,
                         sourceCity: Integer,
                         destinationCity: Integer,
                         TO_event_Id: Integer): Unit = {
        val conn: Connection = splicemachineContext.getConnection()
        try {
          conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
          update(internalTN, source, Timestamp.valueOf(shippingDate), Timestamp.valueOf(originalDeliveryDate), qty, CHANGE_AT_ST)
          update(internalTN, destination, Timestamp.valueOf(shippingDate), Timestamp.valueOf(originalDeliveryDate), -qty, CHANGE_AT_ET)
          update(internalTN, source, Timestamp.valueOf(shippingDate), Timestamp.valueOf(newDeliveryDate), -qty, CHANGE_AT_ST)
          update(internalTN, destination, Timestamp.valueOf(shippingDate), Timestamp.valueOf(newDeliveryDate), qty, CHANGE_AT_ET)
          saveToDeliveryChgEvent(Timestamp.valueOf(originalDeliveryDate),Timestamp.valueOf(newDeliveryDate),TO_Id,supplier, modeOfTransport, carrier,fromWeather, toWeather, sourceCity, destinationCity, TO_event_Id)
          //TODO UpdateTODeliveryDate();
          conn.commit()
        }
        catch {
          case exp: WriteConflict => {
            conn.rollback()
            conn.setAutoCommit(true)
            if (retryCount < MAX_RETRIES) {
              changeDelivery(source, destination, shippingDate, originalDeliveryDate, newDeliveryDate, qty, retryCount + 1,
                TO_Id,
                supplier,
                modeOfTransport,
                carrier,
                fromWeather,
                toWeather,
                sourceCity: Integer,
                destinationCity: Integer,
                TO_event_Id)
            }
            else {
              // put code here to too many re-tries
            }
          }
          case _: Throwable => println("Got some other kind of exception")
        }
        finally {
          conn.setAutoCommit(true)
        }
      }


      /**
        *
        * initialize (id startOfTime endOfTime value)
        * @return
        */
      def saveToDeliveryChgEvent(
               deliveryDate: Timestamp,
               modDeliveryDate: Timestamp,
               TO_Id: Integer,
               supplier: String,
               modeOfTransport: Integer,
               carrier: Integer,
               fromWeather: Integer,
               toWeather: Integer,
               sourceCity: Integer,
               destinationCity: Integer,
               TO_event_Id: Integer): Unit = {

        val optionMap = Map(
          JDBCOptions.JDBC_TABLE_NAME -> TODelviraryChgEventTable,
          JDBCOptions.JDBC_URL -> defaultJDBCURL
        )
        val JDBCOps = new JDBCOptions(optionMap)
        val conn = JdbcUtils.createConnectionFactory(JDBCOps)()
        try {
          var ps = conn.prepareStatement("insert into " + TODelviraryChgEventTable + TODEColumnsInsertString + TODEColumnsInsertStringValues)
          ps.setLong(TODE_TO_Event_ID, TO_event_Id.toLong)
          ps.setLong(TODE_TO_Id, TO_Id.toLong)
          ps.setLong(TODE_ShipFrom, sourceCity.toLong)
          ps.setLong(TODE_ShipTo, destinationCity.toLong)
           ps.setTimestamp(TODE_DeliveryDate, deliveryDate)
          ps.setTimestamp(TODE_ModDeliveryDate, modDeliveryDate)
          ps.setString(TODE_Supplier, supplier)
          ps.setShort(TODE_TransportMode, modeOfTransport.toShort)
          ps.setLong(TODE_Carrier, carrier.toLong)
          ps.setShort(TODE_FromWeather, fromWeather.toShort)
          ps.setShort(TODE_ToWeather, toWeather.toShort)
          ps.execute()
        } finally {
          conn.close()
        }
      }

    }


    ignore("Order demo") {
      val dec1 = Timestamp.valueOf("2010-12-01 00:00:00")
      val dec10 = Timestamp.valueOf("2010-12-10 00:00:00")
      val dec5 = Timestamp.valueOf("2010-12-05 00:00:00")
      val dec7 = Timestamp.valueOf("2010-12-07 00:00:00")

      val may5 = Timestamp.valueOf("2010-5-05 00:00:00")
      val sep7 = Timestamp.valueOf("2010-9-07 00:00:00")
      val may10 = Timestamp.valueOf("2010-5-10 00:00:00")
      val sep10 = Timestamp.valueOf("2010-9-10 00:00:00")

      val umbrellasAtStore1 = 10;
      val umbrellasAtStore2 = 11;
      val umbrellasAtDC1 = 12;
      val umbrellasAtDC2 = 13;

      Inventory.create(umbrellasAtStore1);
      Inventory.create(umbrellasAtStore2);
      Inventory.create(umbrellasAtDC1);
      Inventory.create(umbrellasAtDC2);

      PurchaseOrder.create(umbrellasAtDC1, "2010-5-05 00:00:00", "2010-5-10 00:00:00", 10)
      PurchaseOrder.create(umbrellasAtDC2, "2010-5-05 00:00:00", "2010-5-10 00:00:00", 20)

      SalesOrder.create(umbrellasAtDC1, "2010-6-05 00:00:00", "2010-6-10 00:00:00", 2)
      SalesOrder.create(umbrellasAtDC2, "2010-6-05 00:00:00", "2010-6-10 00:00:00", 3)

      var df = sqlContext.read.options(internalOptions).splicemachine
        .filter(s"TIMELINE_ID = $umbrellasAtDC1 AND ST = to_utc_timestamp('2010-6-10 00:00:00','GMT')")
        .select("VAL")
      var vals = df.collectAsList().toString
      assertEquals("[[8]]", vals)

      df = sqlContext.read.options(internalOptions).splicemachine
        .filter(s"TIMELINE_ID = $umbrellasAtDC2 AND ST = to_utc_timestamp('2010-6-10 00:00:00','GMT')")
        .select("VAL")
      vals = df.collectAsList().toString
      assertEquals("[[17]]", vals)


      TransferOrder.create(umbrellasAtDC1, umbrellasAtDC2, "2010-7-10 00:00:00", "2010-7-15 00:00:00", "2010-7-15 00:00:00", 5, 2,
        100,"Supplier1","ASN100","Container100",1,1,1,1, cities(1).Latitude, cities(1).Longitude, 1,  2, 100)

      df = sqlContext.read.options(internalOptions).splicemachine
        .filter(s"TIMELINE_ID = $umbrellasAtDC2 AND ST = to_utc_timestamp('2010-7-15 00:00:00','GMT')")
        .select("VAL")
      vals = df.collectAsList().toString
      assertEquals("[[22]]", vals)

      df = sqlContext.read.options(internalOptions).splicemachine
        .filter(s"TIMELINE_ID = $umbrellasAtDC1 AND ST = to_utc_timestamp('2010-7-10 00:00:00','GMT')")
        .select("VAL")
      vals = df.collectAsList().toString
      assertEquals("[[3]]", vals)


      val optionMap = Map(
        JDBCOptions.JDBC_TABLE_NAME -> TOTable,
        JDBCOptions.JDBC_URL -> defaultJDBCURL
      )

      df = sqlContext.read.options(optionMap).splicemachine
        .filter(s"TO_ID = 100")
        .select("fromWeather")
      vals = df.collectAsList().toString
      assertEquals("[[1]]", vals)


    }

}