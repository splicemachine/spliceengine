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
package com.splicemachine.spark2.splicemachine.permissions

import com.splicemachine.spark2.splicemachine.SplicemachineContext
import org.scalatest.{FunSuite, Matchers}

abstract class PermissionsIT extends FunSuite with TestContext_Permissions with Matchers {
  var spcTest: SplicemachineContext = _
  def userid: String = ""
  def password: String = ""
  val testJDBCURL = s"jdbc:splice://localhost:1527/splicedb;user=${userid};password=${password}"
  
  val msgExceptionDF: String
  val msgExceptionTruncate: String
  val msgExceptionDelete: String
  val msgExceptionInsert: String
  val msgExceptionUpdate: String
  val msgExceptionUpdate2: String
  val msgExceptionGetSchema: String

  override def beforeAll(): Unit = {
    super.beforeAll
    execute(s"CALL SYSCS_UTIL.SYSCS_CREATE_USER('${userid}', '${password}')")
    spcTest = new SplicemachineContext(testJDBCURL)
  }

  override def afterAll(): Unit = {
    execute(s"CALL SYSCS_UTIL.SYSCS_DROP_USER('${userid}')")
    dropSchema(userid)
    super.afterAll
  }

  test("Test Connection Schema Name") {
    org.junit.Assert.assertEquals(
      "Connection Schema Name Failed!", 
      userid.toUpperCase, 
      spcTest.getConnection.getSchema.toUpperCase
    )
  }

  test("Test DF / Select") {
    var msg = ""
    try {
      spcTest.df(s"select * from $internalTN")
    } catch {
      case e: Exception => msg = e.toString
    }
    org.junit.Assert.assertEquals(
      "DF / Select Failed!",
      msgExceptionDF.toUpperCase,
      msg.toUpperCase
    )
  }

  test("Test Truncate") {
    var msg = ""
    try {
      spcTest.truncateTable(internalTN)
    } catch {
      case e: Exception => msg = e.toString
    }
    org.junit.Assert.assertEquals(
      "Truncate Failed!",
      msgExceptionTruncate.toUpperCase,
      msg.toUpperCase
    )
  }

  test("Test Delete") {
    var msg = ""
    try {
      spcTest.delete(internalTNDF, internalTN)
    } catch {
      case e: Exception => msg = e.toString
    }
    org.junit.Assert.assertEquals(
      "Delete Failed!",
      msgExceptionDelete.toUpperCase,
      msg.toUpperCase
    )
  }

  test("Test Insert") {
    var msg = ""
    try {
      spcTest.insert(internalTNDF, internalTN)
    } catch {
      case e: Exception => msg = e.toString
    }
    org.junit.Assert.assertEquals(
      "Insert Failed!",
      msgExceptionInsert.toUpperCase,
      msg.toUpperCase
    )
  }

  test("Test Update") {
    var msg = ""
    try {
      spcTest.update(internalTNDF, internalTN)
    } catch {
      case e: Exception => msg = e.toString
    }
    org.junit.Assert.assertTrue(
      msg,
      msg.toUpperCase.equals( msgExceptionUpdate.toUpperCase ) || 
        msg.toUpperCase.equals( msgExceptionUpdate2.toUpperCase )
    )
  }

  test("Test Get Schema") {
    var msg = ""
    try {
      spcTest.getSchema(internalTN)
    } catch {
      case e: Exception => msg = e.toString
    }
    org.junit.Assert.assertEquals(
      "Get Schema Failed!",
      msgExceptionGetSchema.toUpperCase,
      msg.toUpperCase
    )
  }
}
