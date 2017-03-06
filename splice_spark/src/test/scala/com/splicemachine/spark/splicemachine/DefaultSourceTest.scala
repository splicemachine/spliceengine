/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splicemachine.spark.splicemachine

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.util.control.NonFatal

import com.google.common.collect.ImmutableList

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType};
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DefaultSourceTest extends FunSuite with TestContext with BeforeAndAfter with Matchers {

  val rowCount = 10
  var sqlContext : SQLContext = _
  var rows : IndexedSeq[(Int, Int, String, Long)] = _
  var kuduOptions : Map[String, String] = _

  before {
    val rowCount = 10
    sqlContext = new SQLContext(sc)
  }
/*
  test("table creation") {
    val tableName = "testcreatetable"
    if (splicemachineContext.tableExists(schemaName,tableName)) {
      splicemachineContext.deleteTable(schemaName,tableName)
    }
  }
*/
  test("data frame creation") {
    splicemachineContext.splicemachineDataFrame("select * from sys.systables").show(100)
  }

}