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

import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TimeLineWrapper extends BeforeAndAfterAll {
  self: Suite =>
  var sc: SparkContext = null
  var splicemachineContext: SplicemachineContext = null
  val table = "TimeLine_Int"
  val schema = "TimeLine"
  val internalTN = schema + "." + table
  val startOfTimeStr = "1678-01-01 00:00:00"
  val endOfTimeStr = "2261-12-31 00:00:00"
  val startOfTime = Timestamp.valueOf(startOfTimeStr)
  val endOfTime = Timestamp.valueOf(endOfTimeStr)
  val firstId = 0
  val SQL_ID = 1
  val SQL_ST = 2
  val SQL_ET = 3
  val SQL_VAL = 4
  val DF_ID = 0
  val DF_ST = 1
  val DF_ET = 2
  val DF_VAL = 3

  val TO_TO_ID = 1
  val TO_PO_Id = 2
  val TO_ShipFrom = 3
  val TO_ShipTo = 4
  val TO_ShipDate = 5
  val TO_DeliveryDate = 6
  val TO_SourceInventory = 7
  val TO_DestinationInventory = 8
  val TO_Qty = 9
  val TO_Supplier = 10
  val TO_ASN = 11
  val TO_Container = 12
  val TO_TransportMode = 13
  val TO_Carrier = 14
  val TO_Weather = 15
  val TO_Latitude = 16
  val TO_Longitude = 17



  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString

  val defaultJDBCURL = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"
  val columnsWithPrimaryKey = "(" +
    "Timeline_Id bigint, " +
    "ST timestamp, " +
    "ET timestamp, " +
    "Val bigint, " +
    "primary key (Timeline_ID, ST)" +
    ")"
  val columnsWithoutPrimaryKey = "(" +
    "Timeline_Id bigint, " +
    "ST timestamp, " +
    "ET timestamp, " +
    "Val bigint " +
    ")"

  val primaryKeys = Seq("Timeline_ID, ST")

  val columnsInsertString = "(" +
    "Timeline_Id, " +
    "ST, " +
    "ET, " +
    "Val" +
  ") "

  val columnsSelectString = "Timeline_Id, " +
    "ST, " +
    "ET, " +
    "Value"

  val columnsInsertStringValues = "values (?,?,?,?)"

  /* (t1<=ST and t2>ST) or (t1>ST and t1<ET)  (t1 t2 t1 t1 )*/
  val overlapCondition = "where Timeline_Id = ? and ((ST >=? and ST <?) or ((ST < ?) and (ET > >?)))"


  val internalOptions = Map(
    JDBCOptions.JDBC_TABLE_NAME -> internalTN,
    JDBCOptions.JDBC_URL -> defaultJDBCURL
  )

  val internalJDBCOptions = new JDBCOptions(internalOptions)

  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("Timeline").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  override def beforeAll() {
    sc = new SparkContext(conf)
    splicemachineContext = new SplicemachineContext(defaultJDBCURL)
    createTimeline(internalTN)
    dropAndCreateTable(TOTable,TOColumnsWithPrimaryKey,TOIndex1,TOIndex1Columns,TOIndex2,TOIndex2Columns)
  }

  override def afterAll() {
    if (sc != null) sc.stop()
  }

  /**
    *
    * Insert (id startOfTime endOfTime value)
    *
    * @param table table name of timeline
    * @return
    */
  def createTimeline(table: String): Unit = {
    val conn = JdbcUtils.createConnectionFactory(internalJDBCOptions)()
    if (splicemachineContext.tableExists(table)){
      conn.createStatement().execute("drop table " + table)
    }
    conn.createStatement().execute("create table " + table + columnsWithPrimaryKey)
  }

  /**
    *
    * initialize (id startOfTime endOfTime value)
    *
    * @param table table name of timeline
    * @param id id of timeline
    * @param value initial value of timeline
    * @return
    */
  def initialize(table: String, id: Integer, value: Integer): Unit = {
    val conn = JdbcUtils.createConnectionFactory(internalJDBCOptions)()
    val start: Timestamp = startOfTime
    val end: Timestamp = endOfTime
    try {
      var ps = conn.prepareStatement("delete from " + table + " where timeline_id = " + id)
      ps.execute()
      ps = conn.prepareStatement("insert into " + table + columnsInsertString + columnsInsertStringValues)
      ps.setInt(SQL_ID, id)
      ps.setTimestamp(SQL_ST, start)
      ps.setTimestamp(SQL_ET, end)
      ps.setInt(SQL_VAL, value)
      ps.execute()
    } finally {
      conn.close()
    }
  }


  /**
    *
    * DropAndCreateTable
    *
    * @param table table name of timeline
    * @param columns columns and types for table
    * @return
    */
  def dropAndCreateTable(table: String,
                         columns: String,
                         index1: String,
                         index1Columns: String,
                         index2: String,
                         index2Columns: String
                        ): Unit = {
    val optionMap = Map(
      JDBCOptions.JDBC_TABLE_NAME -> table,
      JDBCOptions.JDBC_URL -> defaultJDBCURL
    )
    val JDBCOps = new JDBCOptions(optionMap)
    val conn = JdbcUtils.createConnectionFactory(JDBCOps)()
    if (splicemachineContext.tableExists(table)){
      conn.createStatement().execute("drop table " + table)
    }
    conn.createStatement().execute("create table " + table + columns)
    if (index1.nonEmpty) {
      conn.createStatement().execute("create index " + index1 + " on " + table +  index1Columns)
    }
    if (index2.nonEmpty) {
      conn.createStatement().execute("create index " + index2 + " on " + table + index2Columns)
    }
  }

  val TOTable = schema + "." + "TransferOrders"
  val TOETable = schema + "." + "TOEvents"

  val TOColumnsWithPrimaryKey = "(" +
    "TO_Id bigint, " +
    "PO_Id bigint, " +
    "ShipFrom bigint, " +
    "ShipTo bigint, " +
    "ShipDate timestamp, " +
    "DeliveryDate timestamp, " +
    "SourceInventory bigint, " +
    "DestinationInventory bigint, " +
    "Qty bigint, " +
    "Supplier varchar(100), " +
    "ASN varchar(100), " +
    "Container varchar(100), " +
    "TransportMode smallint, " +
    "Carrier bigint, " +
    "Weather smallint, " +
    "Latitude double, " +
    "Longitude double, " +
    "primary key (TO_ID)" +
    ")"

  val TOIndex1 = schema + "." + "TOSTIDX"
  val TOIndex2 = schema + "." + "TOETIDX"

  val TOIndex1Columns = "(" +
    "ShipDate, " +
    "TO_Id" +
  ")"

  val TOIndex2Columns = "(" +
    "DeliveryDate, " +
    "TO_Id" +
    ")"

  val TOColumnsInsertString = "(" +
    "TO_Id, " +
    "PO_Id, " +
    "ShipFrom, " +
    "ShipTo, " +
    "ShipDate, " +
    "DeliveryDate, " +
    "SourceInventory, " +
    "DestinationInventory, " +
    "Qty, " +
    "Supplier, " +
    "ASN, " +
    "Container, " +
    "TransportMode, " +
    "Carrier, " +
    "Weather, " +
    "Latitude, " +
    "Longitude " +
    ") "

  val TOColumnsSelectString = "TO_Id, " +
    "PO_Id, " +
    "ShipFrom, " +
    "ShipTo, " +
    "ShipDate, " +
    "DeliveryDate, " +
    "SourceInventory, " +
    "DestinationInventory, " +
    "Qty, " +
    "Supplier, " +
    "ASN, " +
    "Container, " +
    "TransportMode, " +
    "Carrier, " +
    "Weather, " +
    "Latitude, " +
    "Longitude "

  val TOEventColumnsSelectString = "TOE_Id, " +
    "TO_Id, " +
    "SourceWeather, " +
    "DestinationWeather" +
    "OriginalDeliveryDate, " +
    "NewDeliveryDate, "




  val TOColumnsInsertStringValues = "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"


  case class City(name: String, Latitude: Double, Longitude: Double, state: String)

  val cities: Array[City] = Array(
    City("New York", 40.7127837, -74.0059413, "New York"),
    City("Los Angeles", 34.0522342, -118.2436849, "California"),
    City("Chicago", 41.8781136, -87.6297982, "Illinois"),
    City("Houston", 29.7604267, -95.3698028, "Texas"),
    City("Philadelphia", 39.9525839, -75.1652215, "Pennsylvania"),
    City("Phoenix", 33.4483771, -112.0740373, "Arizona"),
    City("San Antonio", 29.4241219, -98.49362819999999, "Texas"),
    City("San Diego", 32.715738, -117.1610838, "California"),
    City("Dallas", 32.7766642, -96.79698789999999, "Texas"),
    City("San Jose", 37.3382082, -121.8863286, "California")
  )




}