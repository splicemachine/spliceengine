/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import org.apache.spark.sql._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.immutable.IndexedSeq

@RunWith(classOf[JUnitRunner])
class NativeTransformationsTest extends FunSuite with TestContext with BeforeAndAfter with Matchers {
  val rowCount = 10
  var sqlContext : SQLContext = _
  var rows : IndexedSeq[(Int, Int, String, Long)] = _

  before {
    val rowCount = 10
    if (sqlContext == null)
      sqlContext = new SQLContext(sc)
    if (splicemachineContext.tableExists(internalTN)) {
      splicemachineContext.dropTable(internalTN)
    }
    if (splicemachineContext.tableExists(schema+"."+"T")) {
      splicemachineContext.dropTable(schema+"."+"T")
    }
    if (splicemachineContext.tableExists(schema+"."+"T2")) {
      splicemachineContext.dropTable(schema+"."+"T2")
    }
    insertInternalRows(rowCount)
    splicemachineContext.getConnection().commit()
    sqlContext.read.options(internalOptions).splicemachine.createOrReplaceTempView(table)
  }

  test("join over join") {
     val df = splicemachineContext.df(
       """select * from sys.systables s1
         |inner join sys.systables s2 --splice-properties joinStrategy=sortmerge
         |  on s1.tablename = s2.tablename
         |inner join sys.systables s3 --splice-properties joinStrategy=sortmerge
         |  on s1.tablename = s3.tablename
       """.stripMargin
     )

    val plan = df.queryExecution.sparkPlan.toString
    assert("ExistingRDD".r.findAllMatchIn(plan).length == 3)
    assert("SortMergeJoin".r.findAllMatchIn(plan).length == 2)
  }

  test("order by over join") {
    val df = splicemachineContext.df(
      """select * from sys.systables s1
        |inner join sys.systables s2 --splice-properties joinStrategy=sortmerge
        |  on s1.tablename = s2.tablename order by s1.tablename
      """.stripMargin
    )

    val plan = df.queryExecution.sparkPlan.toString
    assert("ExistingRDD".r.findAllMatchIn(plan).length == 2)
    assert("SortMergeJoin".r.findAllMatchIn(plan).length == 1)
    assert("Sort ".r.findAllMatchIn(plan).length == 1)
  }

  test("simple project on join over join") {
    val df = splicemachineContext.df(
      """select s1.tablename from sys.systables s1
        |inner join sys.systables s2 --splice-properties joinStrategy=sortmerge
        |  on s1.tablename = s2.tablename
        |inner join sys.systables s3 --splice-properties joinStrategy=sortmerge
        |  on s1.tablename = s3.tablename
      """.stripMargin
    )

    val plan = df.queryExecution.sparkPlan.toString
    assert("ExistingRDD".r.findAllMatchIn(plan).length == 3)
    assert("SortMergeJoin".r.findAllMatchIn(plan).length == 2)
  }

  test("complex project on join over join") {
    val df = splicemachineContext.df(
      """select s1.colsequence * 10 from sys.systables s1
        |inner join sys.systables s2 --splice-properties joinStrategy=sortmerge
        |  on s1.tablename = s2.tablename
        |inner join sys.systables s3 --splice-properties joinStrategy=sortmerge
        |  on s1.tablename = s3.tablename
      """.stripMargin
    )

    val plan = df.queryExecution.sparkPlan.toString
    assert("ExistingRDD".r.findAllMatchIn(plan).length == 3)
    assert("SortMergeJoin".r.findAllMatchIn(plan).length == 2)
  }

  test("aggregate on join over join") {
    val df = splicemachineContext.df(
      """select sum(s1.colsequence) from sys.systables s1
        |inner join sys.systables s2 --splice-properties joinStrategy=sortmerge
        |  on s1.tablename = s2.tablename
        |inner join sys.systables s3 --splice-properties joinStrategy=sortmerge
        |  on s1.tablename = s3.tablename
      """.stripMargin
    )

    val plan = df.queryExecution.sparkPlan.toString
    assert("ExistingRDD".r.findAllMatchIn(plan).length == 3)
    assert("SortMergeJoin".r.findAllMatchIn(plan).length == 2)
  }
}