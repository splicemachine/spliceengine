package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.Types

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

/**
  * Created by jleach on 4/7/17.
  */
class SplicemachineDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:splice")

  override def getCatalystType(
                                sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.REAL)
      Option(FloatType)
    else if (sqlType == Types.SMALLINT)
      Option(ShortType)
    else if (sqlType == Types.TINYINT)
      Option(ByteType)
//    else if (sqlType == Types.ARRAY)
//      Option(DataTypes.createArrayType(null))

    //    else if (sqlType == Types.ARRAY) Option(ArrayType) Need to figure out array handling
    else None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Option(JdbcType("CLOB", java.sql.Types.CLOB))
    case ByteType => Option(JdbcType("TINYINT", java.sql.Types.TINYINT))
    case ShortType => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case BooleanType => Option(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))

    // 31 is the maximum precision and 5 is the default scale for a Derby DECIMAL
    case t: DecimalType if t.precision > 31 =>
      Option(JdbcType("DECIMAL(31,5)", java.sql.Types.DECIMAL))
    case _ => None
  }
}
