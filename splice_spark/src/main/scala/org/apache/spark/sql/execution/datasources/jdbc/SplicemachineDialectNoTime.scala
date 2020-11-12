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

import org.apache.spark.sql.types._

/**
  * Created by jleach on 4/7/17.
  */
class SplicemachineDialectNoTime extends SplicemachineDialect {

  override def getCatalystType(
                                sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] =
    super.getCatalystType(sqlType, typeName, size, md).orElse(
      sqlType match {
        case Types.TIME => Some(StringType)
        case _ => None
      }
    )
}
