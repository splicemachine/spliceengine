/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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
package com.splicemachine.spark2.splicemachine.transaction

import java.sql.Connection
import java.util.Date

import com.splicemachine.spark2.splicemachine._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestContext extends BeforeAndAfterAll { self: Suite =>
  var spark: SparkSession = _
  var splicemachineContext: SplicemachineContext = _
  var internalTNDF: Dataset[Row] = _
  var df300s: Dataset[Row] = _
  def table(): String = "transactionTable"
  val module = "splice_spark2"
  val schema = s"${module}_Transaction_schema"
  val internalTN = schema+"."+table

  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString
  val defaultJDBCURL = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"
  val allTypesCreateStringWithPrimaryKey = "(" +
    "\"A\" int, " +
    "\"B\" int, " +
    "primary key (\"A\")" +
     ")"

  def allTypesSchema(withPrimaryKey: Boolean): StructType =
    StructType(
      StructField("A", IntegerType, false) ::
      StructField("B", IntegerType, true) ::
      Nil)

  val allTypesInsertString = "(" +
    "\"A\", " +
    "\"B\" " +
    ") "
  val allTypesInsertStringValues = "values (?,?)"

  val primaryKeyDelete = "where A = ?"


  val internalOptions = Map(
    JDBCOptions.JDBC_TABLE_NAME -> internalTN,
    JDBCOptions.JDBC_URL -> defaultJDBCURL
  )

  val internalJDBCOptions = new JDBCOptions(internalOptions)

  val testRow = List(0,200)

  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  override def beforeAll() {
    spark = SparkSession.builder.config(conf).getOrCreate
    splicemachineContext = new SplicemachineContext(defaultJDBCURL)
    createInternalTable
    internalTNDF = dataframe(
      rdd( Seq( Row.fromSeq( testRow ) ) ),
      allTypesSchema(true)
    )
    df300s = dataframe(
      rdd( Seq( Row.fromSeq( List(0,300) ), Row.fromSeq( List(1,301) ) ) ),
      allTypesSchema(true)
    )
  }

  override def afterAll() {
    splicemachineContext.setAutoCommitOn
    dropInternalTable
    dropSchema(schema)
    if (spark != null) spark.stop()
  }

  def rdd(rows: Seq[Row]): RDD[Row] = {
    spark.sparkContext.parallelize(rows)
  }

  def dataframe(rdd: RDD[Row], schema: StructType): Dataset[Row] = spark.createDataFrame( rdd , schema )

  def getConnection(): Connection = JdbcUtils.createConnectionFactory(internalJDBCOptions)()

  def deleteInternalRow(key: Int): Unit = {
    val conn = getConnection()
    try {
      val ps = conn.prepareStatement(primaryKeyDelete)
      ps.setInt(1,key)
      ps.executeUpdate()
    } finally {
      conn.close()
    }
  }

  def execute(sql: String): Unit = {
    val conn = getConnection()
    try {
      conn.createStatement().execute(sql)
    }
    finally {
      conn.close()
    }
  }

  def createInternalTable(): Unit =
    if (!splicemachineContext.tableExists(internalTN))
      execute("create table "+internalTN + allTypesCreateStringWithPrimaryKey)

  def dropInternalTable(): Unit =
    if (splicemachineContext.tableExists(internalTN))
      execute("drop table "+internalTN )

  def dropSchema(schemaToDrop: String): Unit = execute(s"drop schema $schemaToDrop restrict")

  def truncateInternalTable(): Unit = {
    execute( s"TRUNCATE TABLE $internalTN" )
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

  def rowCount(table: String): Int =
    executeQuery(
      s"select count(*) from $table",
      rs => {
        rs.next
        rs.getInt(1)
      }
    ).asInstanceOf[Int]

  /**
    *
    * Insert Splice Machine Row
    *
    * @param rowCount rows to return
    * @return
    */
  def insertInternalRows(rowCount: Integer): Unit = {
      val conn = getConnection()
      createInternalTable()
      try {
        Range(0, rowCount).map { i =>
          val ps = conn.prepareStatement("insert into " + internalTN + allTypesInsertString + allTypesInsertStringValues)
          ps.setInt(1, i)
          ps.setInt(2, i+200)
          ps.execute()
        }
      }finally {
        conn.close()
      }
  }
}
