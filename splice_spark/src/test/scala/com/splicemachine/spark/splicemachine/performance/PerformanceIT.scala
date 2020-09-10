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
package com.splicemachine.spark.splicemachine.performance

import java.sql.ResultSet

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

/** Verify the calls can handle a few thousand records as set by the recordCount field in TestContext */
@RunWith(classOf[JUnitRunner])
class PerformanceIT extends FunSuite with TestContext with Matchers {
  
  test("Get Dataframe") {
    val dfCount = splicemachineContext.df(s"select * from $tableRead").count
    org.junit.Assert.assertEquals(s"DF count from $tableRead != expected" , recordCount , dfCount)
  }

  test("Get RDD") {
    val rddCount = splicemachineContext.rdd( tableRead ).count
    org.junit.Assert.assertEquals(s"RDD count from $tableRead != expected" , recordCount , rddCount)
  }
  
  private def tableCount(table: String): Int = {
    val conn = getConnection()
    var rs: ResultSet = null
    try {
      rs = conn.createStatement().executeQuery(s"select count(*) from $table")
      rs.next
      rs.getInt(1)
    }
    finally {
      rs.close
      conn.close
    }
  }

  test("Insert") {
    dropInternalTable(tableWrite)
    createInternalTable(tableWrite)
    
    splicemachineContext.insert( internalTNDF , tableWrite )
    val count = tableCount(tableWrite)

    dropInternalTable(tableWrite)
    
    org.junit.Assert.assertEquals(s"Count from $tableWrite != expected", recordCount, count)
  }
}
