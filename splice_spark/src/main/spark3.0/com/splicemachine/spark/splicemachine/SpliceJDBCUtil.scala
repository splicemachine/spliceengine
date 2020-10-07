package com.splicemachine.spark.splicemachine

import java.sql.{Connection,SQLException,ResultSet,Timestamp,Date}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils, JDBCOptions, JdbcOptionsInWrite, JDBCRDD}
import org.apache.spark.sql.jdbc.{JdbcType, JdbcDialect, JdbcDialects}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jleach on 4/10/17.
  */
object SpliceJDBCUtil {

  /**
    * `columns`, but as a String suitable for injection into a SQL query.
    */
  def listColumns(columns: Array[String]): String = {
    val sb = new StringBuilder()
    columns.foreach(x => sb.append(",").append(x))
    if (sb.isEmpty) "*" else sb.substring(1)
  }

  /**
    * Prune all but the specified columns from the specified Catalyst schema.
    *
    * @param schema - The Catalyst schema of the master table
    * @param columns - The list of desired columns
    * @return A Catalyst schema corresponding to columns in the given order.
    */
  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.metadata.getString("name") -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  /**
    * Create Where Clause Filter
    */
  def filterWhereClause(url: String, filters: Array[Filter]): String = {
    filters
      .flatMap(JDBCRDD.compileFilter(_, JdbcDialects.get(url)))
      .map(p => s"($p)").mkString(" AND ")
  }


  /**
    * Compute the schema string for this RDD.
    */
  def schemaWithoutNullableString(schema: StructType, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    schema.fields foreach { field =>
      val name =
        if (field.metadata.contains("name"))
          dialect.quoteIdentifier(field.metadata.getString("name"))
      else
          dialect.quoteIdentifier(field.name)
      val typ: String = getJdbcType(field.dataType, dialect).databaseTypeDefinition
      sb.append(s", $name $typ")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  def retrievePrimaryKeys(options: JdbcOptionsInWrite): Array[String] =
    retrieveMetaData(
      options,
      (conn,schema,tablename) => conn.getMetaData.getPrimaryKeys(null, schema, tablename),
      (conn,tablename) => conn.getMetaData.getPrimaryKeys(null, null, tablename),
      rs => Seq(rs.getString("COLUMN_NAME"))
    ).map(_(0))

  def retrieveColumnInfo(options: JdbcOptionsInWrite): Array[Seq[String]] =
    retrieveMetaData(
      options,
      (conn,schema,tablename) => conn.getMetaData.getColumns(null, schema.toUpperCase, tablename.toUpperCase, null),
      (conn,tablename) => conn.getMetaData.getColumns(null, null, tablename.toUpperCase, null),
      rs => Seq(
        rs.getString("COLUMN_NAME"),
        rs.getString("TYPE_NAME"),
        rs.getString("COLUMN_SIZE")
      )
    )

  def retrieveTableInfo(options: JdbcOptionsInWrite): Array[Seq[String]] =
    retrieveMetaData(
      options,
      (conn,schema,tablename) => conn.getMetaData.getTables(null, schema.toUpperCase, tablename.toUpperCase, null),
      (conn,tablename) => conn.getMetaData.getTables(null, null, tablename.toUpperCase, null),
      rs => Seq(
        rs.getString("TABLE_SCHEM"),
        rs.getString("TABLE_NAME"),
        rs.getString("TABLE_TYPE")
      )
    )

  private def retrieveMetaData(
    options: JdbcOptionsInWrite,
    getWithSchemaTablename: (Connection,String,String) => ResultSet,
    getWithTablename: (Connection,String) => ResultSet,
    getData: ResultSet => Seq[String]
  ): Array[Seq[String]] = {
    val table = options.table
    val conn: Connection = JdbcUtils.createConnectionFactory(options)()
    try {
      val rs: ResultSet =
        if (table.contains(".")) {
          val meta = table.split("\\.")
          getWithSchemaTablename(conn, meta(0), meta(1))
        }
        else {
          getWithTablename(conn, table)
        }
      val buffer = ArrayBuffer[Seq[String]]()
      while (rs.next()) {
        buffer += getData(rs)
      }
      buffer.toArray
    } finally {
      conn.close()
    }
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  /**
    * Retrieve standard jdbc types.
    *
    * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
    * @return The default JdbcType for this DataType
    */
  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
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


  /**
    * Turns a single Filter into a String representing a SQL expression.
    * Returns None for an unhandled filter.
    */
  def compileFilter(f: Filter, dialect: JdbcDialect): Option[String] = {
    def quote(colName: String): String = dialect.quoteIdentifier(colName)

    Option(f match {
      case EqualTo(attr, value) => s"${quote(attr)} = ${compileValue(value)}"
      case EqualNullSafe(attr, value) =>
        val col = quote(attr)
        s"(NOT ($col != ${compileValue(value)} OR $col IS NULL OR " +
          s"${compileValue(value)} IS NULL) OR ($col IS NULL AND ${compileValue(value)} IS NULL))"
      case LessThan(attr, value) => s"${quote(attr)} < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"${quote(attr)} > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${quote(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${quote(attr)} >= ${compileValue(value)}"
      case IsNull(attr) => s"${quote(attr)} IS NULL"
      case IsNotNull(attr) => s"${quote(attr)} IS NOT NULL"
      case StringStartsWith(attr, value) => s"${quote(attr)} LIKE '${value}%'"
      case StringEndsWith(attr, value) => s"${quote(attr)} LIKE '%${value}'"
      case StringContains(attr, value) => s"${quote(attr)} LIKE '%${value}%'"
      case In(attr, value) if value.isEmpty =>
        s"CASE WHEN ${quote(attr)} IS NULL THEN NULL ELSE FALSE END"
      case In(attr, value) => s"${quote(attr)} IN (${compileValue(value)})"
      case Not(f) => compileFilter(f, dialect).map(p => s"(NOT ($p))").getOrElse(null)
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

  /**
    * Converts value to SQL expression.
    */
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")


}
