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

import java.math.BigDecimal
import java.sql.{Connection, Time, Timestamp}
import java.util.Date

import com.splicemachine.derby.impl.SpliceSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestContext extends BeforeAndAfterAll { self: Suite =>
  var spark: SparkSession = _
  var sc: SparkContext = null
  var splicemachineContext: SplicemachineContext = _
  var internalTNDF: Dataset[Row] = _
  val table = "test"
  val externalTable = "testExternal"
  val schema = "TestContext"
  val internalTN = schema+"."+table
  val externalTN = schema+"."+externalTable

  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString
  val defaultJDBCURL = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"
  val allTypesCreateStringWithPrimaryKey = "(" +
    "c1_boolean boolean, " +
    "c2_char char(5), " +
    "c3_date date, " +
    "c4_decimal numeric(15,2), " +
    "c5_double double, " +
    "c6_int int, " +
    "c7_bigint bigint, " +
    "c8_float float, " +
    "c9_smallint smallint, " +
    "c10_time time, " +
    "c11_timestamp timestamp, " +
    "c12_varchar varchar(56), " +
    "primary key (c6_int, c7_bigint)" +
     ")"
  val allTypesCreateStringWithoutPrimaryKey = "(" +
    "c1_boolean boolean, " +
    "c2_char char(5), " +
    "c3_date date, " +
    "c4_decimal numeric(15,2), " +
    "c5_double double, " +
    "c6_int int, " +
    "c7_bigint bigint, " +
    "c8_float float, " +
    "c9_smallint smallint, " +
    "c10_time time, " +
    "c11_timestamp timestamp, " +
    "c12_varchar varchar(56)" +
    ")"

  def allTypesSchema(withPrimaryKey: Boolean): StructType = {
    val c6 = StructField("C6_INT", IntegerType, ! withPrimaryKey)
    val c7 = StructField("C7_BIGINT", LongType, ! withPrimaryKey)

    StructType(
      StructField("C1_BOOLEAN", BooleanType, true) ::
        StructField("C2_CHAR", StringType, true) ::
        StructField("C3_DATE", DateType, true) ::
        StructField("C4_DECIMAL", DecimalType(15,2), true) ::
        StructField("C5_DOUBLE", DoubleType, true) ::
        c6 :: c7 ::
        StructField("C8_FLOAT", FloatType, true) ::
        StructField("C9_SMALLINT", ShortType, true) ::
        StructField("C10_TIME", TimestampType, true) ::
        StructField("C11_TIMESTAMP", TimestampType, true) ::
        StructField("C12_VARCHAR", StringType, true) ::
        Nil)
  }

  val primaryKeys = Seq("c6_int","c7_bigint")

  val allTypesInsertString = "(" +
    "c1_boolean, " +
    "c2_char, " +
    "c3_date, " +
    "c4_decimal, " +
    "c5_double, " +
    "c6_int, " +
    "c7_bigint, " +
    "c8_float, " +
    "c9_smallint, " +
    "c10_time, " +
    "c11_timestamp, " +
    "c12_varchar " +
    ") "
  val allTypesInsertStringValues = "values (?,?,?,?,?,?,?,?,?,?,?,?)"

  val primaryKeyDelete = "where c6_int = ? and c7_bigint = ?"


  val internalOptions = Map(
    JDBCOptions.JDBC_TABLE_NAME -> internalTN,
    JDBCOptions.JDBC_URL -> defaultJDBCURL
  )

  val internalExecutionOptions = Map(
    JDBCOptions.JDBC_TABLE_NAME -> internalTN,
    JDBCOptions.JDBC_URL -> defaultJDBCURL,
    SpliceJDBCOptions.JDBC_INTERNAL_QUERIES -> "true"
  )

  val statOptions = Map(
    JDBCOptions.JDBC_TABLE_NAME -> "SYSVW.SYSTABLESTATISTICS",
    JDBCOptions.JDBC_URL -> defaultJDBCURL
  )

  val externalOptions = Map(
    JDBCOptions.JDBC_TABLE_NAME -> externalTN,
    JDBCOptions.JDBC_URL -> defaultJDBCURL
  )

  val internalJDBCOptions = new JDBCOptions(internalOptions)

  val externalJDBCOptions = new JDBCOptions(externalOptions)

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
    internalTNDF = dataframe(
      rdd(Seq(
        Row.fromSeq( Seq(true, "abcde", java.sql.Date.valueOf("2013-09-04"), new BigDecimal("" + 4), 1.5, 6,
          7L, 1.8f, new java.lang.Short("9"), new java.sql.Timestamp(10), new java.sql.Timestamp(11), "Varchar C12") )
      )),
      allTypesSchema(true)
    )
  }

  override def afterAll() {
    if (spark != null) spark.stop()
    if (sc != null) sc.stop()
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
      ps.setLong(2,key)
      ps.executeUpdate()
    } finally {
      conn.close()
    }
  }
  
  def createInternalTable(): Unit =
    if (!splicemachineContext.tableExists(internalTN))
      getConnection.createStatement().execute("create table "+internalTN + this.allTypesCreateStringWithPrimaryKey)

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
          ps.setBoolean(1, i % 2==0)
          ps.setString(2, if (i < 8)"" + i else null)
          ps.setDate(3, if (i % 2==0) java.sql.Date.valueOf("2013-09-04") else java.sql.Date.valueOf("2013-09-05"))
          ps.setBigDecimal(4, new BigDecimal("" + i))
          ps.setDouble(5, i)
          ps.setInt(6, i)
          ps.setInt(7, i)
          ps.setFloat(8, i)
          ps.setShort(9, i.toShort)
          ps.setTime(10, new Time(i))
          ps.setTimestamp(11, new Timestamp(i))
          ps.setString(12, if (i < 8) "sometestinfo" + i else null)
          ps.execute()
        }
      }finally {
        conn.close()
      }
  }
}
