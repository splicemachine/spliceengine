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
package com.splicemachine.spark.splicemachine.permissions

import java.sql.Connection
import java.util.Date

import com.splicemachine.spark.splicemachine.SplicemachineContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestContext_Permissions extends BeforeAndAfterAll { self: Suite =>
  var spark: SparkSession = _
  var spcSplice: SplicemachineContext = _
  var internalTNDF: Dataset[Row] = _
  val module = "splice_spark"
  val schema = s"${module}_TestContext_Permissions_schema"
  val table = "table1"
  val internalTN = schema+"."+table
  
  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString
  
  val defaultJDBCURL = s"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"

  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName(s"${module}-permissions-test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  override def beforeAll() {
    spark = SparkSession.builder.config(conf).getOrCreate
    
    spcSplice = new SplicemachineContext(defaultJDBCURL)
    
    internalTNDF = dataframe(
      rdd(Seq(
        Row.fromSeq( Seq(9, 9) )
      )),
      StructType(
        StructField("A", IntegerType, false) ::
        StructField("B", IntegerType, true) ::
        Nil)
    )
    
    createInternalTable
    insertInternalRows(5)
  }

  override def afterAll() {
    dropInternalTable
    dropSchema(schema)
    if (spark != null) spark.stop()
  }

  def rdd(rows: Seq[Row]): RDD[Row] = {
    spark.sparkContext.parallelize(rows)
  }

  def dataframe(rdd: RDD[Row], schema: StructType): Dataset[Row] = spark.createDataFrame( rdd , schema )

  val internalOptions = Map(
    JDBCOptions.JDBC_TABLE_NAME -> internalTN,
    JDBCOptions.JDBC_URL -> defaultJDBCURL
  )

  val internalJDBCOptions = new JDBCOptions(internalOptions)

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

  val createStringWithPrimaryKey = "(" +
    "A int not null primary key,  " +
    "B int " +
    ")"

  def createInternalTable(): Unit =
    if ( ! spcSplice.tableExists(internalTN) )
      execute("create table " + internalTN + this.createStringWithPrimaryKey)

  def dropInternalTable(): Unit =
    if (spcSplice.tableExists(internalTN))
      execute("drop table "+internalTN )
  
  def dropSchema(schemaToDrop: String): Unit = execute(s"drop schema $schemaToDrop restrict")

  val insertString = "(" +
    "A, " +
    "B " +
    ") "
  
  val insertStringValues = "values (?,?)"
  
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
        val ps = conn.prepareStatement("insert into " + internalTN + insertString + insertStringValues)
        ps.setInt(1, i)
        ps.setInt(2, i)
        ps.execute()
      }
    } finally {
      conn.close()
    }
  }
}
