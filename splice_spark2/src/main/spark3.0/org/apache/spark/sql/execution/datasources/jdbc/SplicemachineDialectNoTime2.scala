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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.Types

import com.splicemachine.db.iapi.reference.Limits
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

/**
  * Created by jleach on 4/7/17.
  */
class SplicemachineDialectNoTime2 extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:splice")

  override def getCatalystType(
                                sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.REAL)
      Option(FloatType)
    else if (sqlType == Types.SMALLINT)
      Option(ShortType)
    else if (sqlType == Types.TINYINT)
      Option(ByteType)
    else if (sqlType == Types.TIME)
      Option(StringType)
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

    // 38 is the maximum precision and 5 is the default scale for a Derby DECIMAL
    case t: DecimalType if t.precision > Limits.DB2_MAX_DECIMAL_PRECISION_SCALE =>
      Option(JdbcType("DECIMAL(%d,5)".format(Limits.DB2_MAX_DECIMAL_PRECISION_SCALE), java.sql.Types.DECIMAL))
    case _ => None
  }
}
