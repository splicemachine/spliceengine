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
package com.splicemachine.spark.splicemachine.column_case

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.Suite
import com.splicemachine.spark.splicemachine._

/** Table column names are in all caps, but dataframe fields are all lower case. */
trait OppositeCase_TestContext extends TestContext { self: Suite =>
  override def table(): String = "oppositeCaseColumnTable"

  override val allTypesCreateStringWithPrimaryKey = "(" +
    "\"A\" int, " +
    "\"B\" int, " +
    "primary key (\"A\")" +
    ")"

  override val allTypesInsertString = "(" +
    "\"A\", " +
    "\"B\" " +
    ") "
  
  override def allTypesSchema(withPrimaryKey: Boolean): StructType =
    StructType(
      StructField("a", IntegerType, false) ::
      StructField("b", IntegerType, true) ::
      Nil)

  override def beforeAll() {
    spark = SparkSession.builder.config(conf).getOrCreate
    splicemachineContext = new SplicemachineContext(defaultJDBCURL)
    internalTNDF = dataframe(
      rdd( Seq( Row.fromSeq( testRow ) ) ),
      allTypesSchema(true)
    )
  }

  override def afterAll() {
    dropInternalTable
    dropSchema(schema)
    if (spark != null) spark.stop()
  }
}
