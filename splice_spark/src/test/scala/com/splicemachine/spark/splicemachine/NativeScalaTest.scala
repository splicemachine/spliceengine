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

import java.util.Date

import com.splicemachine.derby.impl.SpliceSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.immutable.IndexedSeq

@RunWith(classOf[JUnitRunner])
class NativeScalaTest extends FunSuite with BeforeAndAfter with Matchers with BeforeAndAfterAll {
  val rowCount = 10
  var sqlContext : SQLContext = _
  var rows : IndexedSeq[(Int, Int, String, Long)] = _


  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString
  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  var session : SparkSession = null
  var sc : SparkContext = null

  override def beforeAll() {
    session = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    sc = session.sparkContext
  }


  before {
    val rowCount = 10
    if (sqlContext == null)
      sqlContext = new SQLContext(sc)

  }
  

  test("transform") {
    val mySpark = session

    val schemaString = "first second"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rawData = List(Row("Mr. Trump became president after winning the political election. Though he lost the support of some republican friends Trump is friends with President Putin", "President Trump says Putin had no political interference is the election outcome. He says it was a witchhunt by political parties. He claimed President Putin is a friend who had nothing to do with the election"))
    var df = session.createDataFrame(sqlContext.sparkContext.parallelize(rawData), schema)
    df = df.withColumn("counts1", Util.word_count_udf(df.col("first")))
    df = df.withColumn("counts2", Util.word_count_udf(df.col("second")))
    df = df.withColumn("cosine", Util.cosine_udf(df.col("counts1"), df.col("counts2")))
    df.show(false)
  }

}