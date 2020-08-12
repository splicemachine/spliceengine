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
package com.splicemachine.spark.splicemachine.performance

import java.sql.Connection
import java.util.Date

import com.splicemachine.spark.splicemachine._
import com.splicemachine.derby.impl.SpliceSpark
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait TestContext extends BeforeAndAfterAll { self: Suite =>
  var sc: SparkContext = null
  var spark: SparkSession = _
  var splicemachineContext: SplicemachineContext = _
  var internalTNDF: Dataset[Row] = _
  val module = "splice_spark"
  val schema = s"${module}_performance_TestContext"
  val tableRead = schema+".table_read"
  val tableWrite = schema+".table_write"
  val recordCount = 10001

  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString
  val defaultJDBCURL = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"
  val createStringWithPrimaryKey = "(" +
    "c1 int, " +
    "c2 varchar(36), " +
    "c3 varchar(36), " +
    "primary key (c1)" +
     ")"
  
  def dfSchema(): StructType =
    StructType(
      StructField("C1", IntegerType, false) ::
      StructField("C2", StringType, true) ::
      StructField("C3", StringType, true) ::
      Nil)
  
  val insertString = "(" +
    "c1, " +
    "c2, " +
    "c3 " +
    ") "
  val insertStringValues = "values (?,?,?)"


  val internalOptions = Map(
    JDBCOptions.JDBC_TABLE_NAME -> "placeholder",
    JDBCOptions.JDBC_URL -> defaultJDBCURL
  )

  val internalJDBCOptions = new JDBCOptions(internalOptions)

  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  override def beforeAll() {
    sc = new SparkContext(conf)
    SpliceSpark.setContext(sc)
    spark = SparkSession.builder.config(conf).getOrCreate
    splicemachineContext = new SplicemachineContext(defaultJDBCURL)
    var n = 0
    internalTNDF = dataframe(
      rdd(Seq.fill(recordCount)(
        Row.fromSeq( {n+=1; Seq(n, java.util.UUID.randomUUID.toString, java.util.UUID.randomUUID.toString)} )
      )),
      dfSchema()
    )
    createInternalTable(tableRead)
    insertInternalRows(tableRead, recordCount)
  }

  override def afterAll() {
    dropInternalTable(tableRead)
    dropInternalTable(tableWrite)
    dropSchema(schema)
    if (spark != null) spark.stop()
    if (sc != null) sc.stop()
  }

  def rdd(rows: Seq[Row]): RDD[Row] = {
    spark.sparkContext.parallelize(rows)
  }

  def dataframe(rdd: RDD[Row], schema: StructType): Dataset[Row] = spark.createDataFrame( rdd , schema )

  def getConnection(): Connection = JdbcUtils.createConnectionFactory(internalJDBCOptions)()

  def execute(sql: String): Unit = {
    val conn = getConnection()
    try {
      conn.createStatement().execute(sql)
    }
    finally {
      conn.close()
    }
  }
  
  def createInternalTable(table: String): Unit =
    if (!splicemachineContext.tableExists(table))
      execute("create table "+table + this.createStringWithPrimaryKey)

  def dropInternalTable(table: String): Unit =
    if (splicemachineContext.tableExists(table))
      execute("drop table "+table )

  def dropSchema(schemaToDrop: String): Unit = execute(s"drop schema $schemaToDrop restrict")
  
  /**
    *
    * Insert Splice Machine Row
    *
    * @param rowCount rows to return
    * @return
    */
  def insertInternalRows(table: String, rowCount: Integer): Unit = {
      val conn = getConnection()
      createInternalTable(table)
      try {
        Range(0, rowCount).map { i =>
          val ps = conn.prepareStatement("insert into " + table + insertString + insertStringValues)
          ps.setInt(1, i)
          ps.setString(2, java.util.UUID.randomUUID.toString)
          ps.setString(3, java.util.UUID.randomUUID.toString)
          ps.execute()
        }
      } finally {
        conn.close()
      }
  }
}
