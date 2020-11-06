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
package com.splicemachine.spark2

import org.apache.spark.sql.execution.datasources.jdbc.{SplicemachineDialect2, SplicemachineDialectNoTime2}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimeType, TimestampType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SparkSession}

package object splicemachine {

  val dialect = new SplicemachineDialect2
  val dialectNoTime = new SplicemachineDialectNoTime2
  
  var quoteIdentifier: String => String = identity
  
  columnNamesCaseSensitive(false)

  def columnNamesCaseSensitive(caseSensitive: Boolean): Unit = {
    SparkSession.builder.getOrCreate.sqlContext.setConf("spark.sql.caseSensitive", caseSensitive.toString)
    if( caseSensitive )   { quoteIdentifier = dialect.quoteIdentifier }
    else                  { quoteIdentifier = identity }
  }

  /**
    * Adds a method, `splicemachine`, to DataFrameReader that allows you to read SpliceMachine tables using
    * the DataFrameReader.
    */
  implicit class SplicemachineDataFrameReader(reader: DataFrameReader) {
    def splicemachine: DataFrame = reader.format("com.splicemachine.spark2.splicemachine").load
  }

  /**
    * Adds a method, `splicemachine`, to DataFrameWriter that allows writes to Splicemachine using
    * the DataFileWriter
    */
  implicit class SplicemachineDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def splicemachine = writer.format("com.splicemachine.spark2.splicemachine").save
  }

  def modifySchema(schema: StructType, tableSchemaStr: String): StructType = {
    def hasTimeColumn: String => Boolean = schStr => schStr.contains("TIME,") || schStr.endsWith("TIME")
    if( hasTimeColumn(tableSchemaStr) ) {
      val tableColumns = tableSchemaStr.split(", ")
      var i = 0
      var timeSchema = StructType(Nil)
      for (field <- schema.iterator) {
        val dataType = if( hasTimeColumn(tableColumns(i)) ) { TimeType } else { field.dataType }
        timeSchema = timeSchema.add(field.name, dataType, field.nullable)
        i += 1
      }
      timeSchema
    } else {
      schema
    }
  }

  def schemaStringWithoutNullable(schema: StructType, url: String): String = 
    SpliceJDBCUtil.schemaWithoutNullableString(schema, url).replace("\"","")

  /** Schema string built from JDBC metadata. */
  def schemaString(columnInfo: Array[Seq[String]], schema: StructType = new StructType()): String = {
    val info = columnInfo
      .map(i => {
        val colName = quoteIdentifier(i(0))
        val sqlType = i(1)
        val size = sqlType match {
          case "VARCHAR" => s"(${i(2)})"
          case "DECIMAL" | "NUMERIC" => s"(${i(2)},${i(3)})"
          case _ => ""
        }
        s"$colName $sqlType$size"
      })

    if( schema.isEmpty ) {
      info.mkString(", ")
    } else {
      schema.map( field => {
        info.find( col => col.startsWith( quoteIdentifier(field.name)+" " ) ).getOrElse("")
      }).mkString(", ")
    }
  }

  /**
   *
   * Generate the schema string for create table.
   *
   * @param schema
   * @param url
   * @return
   */
  def schemaString(schema: StructType, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    schema.fields foreach { field =>
      val name = quoteIdentifier(field.name)
      val typ: String = getJdbcType(field.dataType, dialect).databaseTypeDefinition
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   *
   * Retrieve the JDBC type based on the Spark DataType and JDBC Dialect.
   *
   * @param dt
   * @param dialect
   * @return
   */
  def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType =
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))

  /**
   * Retrieve standard jdbc types.
   *
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The default JdbcType for this DataType
   */
  def getCommonJDBCType(dt: DataType): Option[JdbcType] =
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(
        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }

}