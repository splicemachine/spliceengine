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
package com.splicemachine.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{SplicemachineDialect, SplicemachineDialectNoTime}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}

package object splicemachine {

  val dialect = new SplicemachineDialect
  val dialectNoTime = new SplicemachineDialectNoTime
  
  var quoteIdentifier: String => String = _
  var norm: String => String = _
  
  columnNamesCaseSensitive(false)

  def columnNamesCaseSensitive(caseSensitive: Boolean): Unit = {
    SparkSession.builder.getOrCreate.sqlContext.setConf("spark.sql.caseSensitive", caseSensitive.toString)
    if( caseSensitive ) {
      quoteIdentifier = dialect.quoteIdentifier
      norm = identity
    } else {
      quoteIdentifier = identity
      norm = s => s.toUpperCase
    }
  }

  /**
    * Adds a method, `splicemachine`, to DataFrameReader that allows you to read SpliceMachine tables using
    * the DataFrameReader.
    */
  implicit class SplicemachineDataFrameReader(reader: DataFrameReader) {
    def splicemachine: DataFrame = reader.format("com.splicemachine.spark.splicemachine").load
  }

  /**
    * Adds a method, `splicemachine`, to DataFrameWriter that allows writes to Splicemachine using
    * the DataFileWriter
    */
  implicit class SplicemachineDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def splicemachine = writer.format("com.splicemachine.spark.splicemachine").save
  }

  def modifyRdd(rdd: RDD[Row], schemaStr: String): RDD[Row] =
    if( schemaStr.contains("TIME,") || schemaStr.endsWith("TIME") ) {
      var i = 0
      var tmIdx = collection.mutable.Seq.empty[Int]
      schemaStr.split(",").foreach(s => {
        if( s.endsWith("TIME") ) { tmIdx = tmIdx :+ i }
        i += 1
      })
      rdd.map(r => {
        val cols = r.toSeq
        tmIdx.foreach(i =>
          if( cols(i).isInstanceOf[java.sql.Timestamp] ) {
            cols.updated(i, java.sql.Time.valueOf( r.getTimestamp(i).toLocalDateTime.toLocalTime ))
          }
        )
        Row.fromSeq(cols)
      })
    } else {
      rdd
    }

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
        info.find( col => norm(col).startsWith( norm(quoteIdentifier(field.name))+" " ) )
          .getOrElse(
            throw new Exception( s"No column named ${field.name} found in the table. Verify column name and case sensitivity." )
          )
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
  private[this]def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  /**
   * Retrieve standard jdbc types.
   *
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The default JdbcType for this DataType
   */
  private[this] def getCommonJDBCType(dt: DataType) = {
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

}