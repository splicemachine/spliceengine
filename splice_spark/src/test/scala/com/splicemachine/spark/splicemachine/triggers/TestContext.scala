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
 */
package com.splicemachine.spark.splicemachine.triggers

import java.sql.Connection
import java.util.Date

import com.splicemachine.derby.impl.SpliceSpark
import com.splicemachine.spark.splicemachine._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestContext extends BeforeAndAfterAll { self: Suite =>
  var sc: SparkContext = null
  var spark: SparkSession = _
  var splicemachineContext: SplicemachineContext = _
  def table(): String = "triggerTable"
  val module = "splice_spark"
  val schema = s"${module}_TestContext_SplicemachineContext_schema"
  val internalTN = schema+"."+table

  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString
  val defaultJDBCURL = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"

  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName(s"$module.test.trigger").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  override def beforeAll() {
    spark = SpliceSpark.getSessionUnsafe
    spark.conf.set("spark.master", "local[*]")
    spark.conf.set("spark.app.name", "test")
    spark.conf.set("spark.ui.enabled", "false")
    spark.conf.set("spark.app.id", appID)
    sc = spark.sparkContext

    splicemachineContext = new SplicemachineContext(defaultJDBCURL)
  }

  override def afterAll() {
    if (spark != null) spark.stop()
    if (sc != null) sc.stop()
  }

  def rdd(rows: Seq[Row]): RDD[Row] = spark.sparkContext.parallelize(rows)

  def dataframe(rdd: RDD[Row], schema: StructType): Dataset[Row] = spark.createDataFrame( rdd , schema )

  def getConnection(): Connection = JdbcUtils.createConnectionFactory(
    new JDBCOptions(Map(
      JDBCOptions.JDBC_TABLE_NAME -> internalTN,
      JDBCOptions.JDBC_URL -> defaultJDBCURL
    ))
  )()

  def execute(sql: String): Unit = {
    val conn = getConnection()
    try {
      conn.createStatement().execute(sql)
    }
    finally {
      conn.close()
    }
  }

  def executeQuery(sql: String, processResultSet: java.sql.ResultSet => Any): Any = {
    val conn = getConnection()
    var rs: java.sql.ResultSet = null
    try {
      rs = conn.createStatement().executeQuery(sql)
      processResultSet(rs)
    }
    finally {
      rs.close
      conn.close
    }
  }
}
