package com.splicemachine.spark2.splicemachine

import java.sql.{Connection, JDBCType, ResultSet, SQLException}

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRDD, JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
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

  def retrievePrimaryKeys(options: JDBCOptions): Array[String] =
    retrieveMetaData(
      options,
      (conn,schema,tablename) => conn.getMetaData.getPrimaryKeys(null, schema, tablename),
      (conn,tablename) => conn.getMetaData.getPrimaryKeys(null, null, tablename),
      rs => Seq(rs.getString("COLUMN_NAME"))
    ).map(_(0))

  def retrieveColumnInfo(options: JDBCOptions): Array[Seq[String]] =
    retrieveMetaData(
      options,
      (conn,schema,tablename) => conn.getMetaData.getColumns(null, schema.toUpperCase, tablename.toUpperCase, null),
      (conn,tablename) => conn.getMetaData.getColumns(null, null, tablename.toUpperCase, null),
      rs => Seq(
        rs.getString("COLUMN_NAME"),
        rs.getString("TYPE_NAME"),
        rs.getString("COLUMN_SIZE"),
        rs.getString("DECIMAL_DIGITS")
      )
    )

  def retrieveTableInfo(options: JDBCOptions): Array[Seq[String]] =
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
    options: JDBCOptions,
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
   * Maps a JDBC type to a Catalyst type.  This function can be called when
   * the JdbcDialect class corresponding to your database driver returns null.
   *
   * @param sqlType - A field of java.sql.Types
   * @return The Catalyst type corresponding to sqlType.
   */
   def getCatalystType(
       sqlType: Int,
       precision: Int,
       scale: Int,
       signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.CLOB          => StringType
      case java.sql.Types.DATALINK      => null
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.DECIMAL       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT      => null
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT   => null
      case java.sql.Types.LONGNVARCHAR  => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR   => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NCLOB         => StringType
      case java.sql.Types.NULL          => null
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.OTHER         => null
      case java.sql.Types.REAL          => DoubleType
      case java.sql.Types.REF           => StringType
      case java.sql.Types.REF_CURSOR    => null
      case java.sql.Types.ROWID         => LongType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.SQLXML        => StringType
      case java.sql.Types.STRUCT        => StringType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE
      => null
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE
      => null
      case java.sql.Types.TINYINT       => IntegerType
      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR       => StringType
      case _                            =>
        throw new SQLException("Unrecognized SQL type " + sqlType)
      // scalastyle:on
    }

    if (answer == null) {
      throw new SQLException("Unsupported type " + JDBCType.valueOf(sqlType).getName)
    }
    answer
  }
}
