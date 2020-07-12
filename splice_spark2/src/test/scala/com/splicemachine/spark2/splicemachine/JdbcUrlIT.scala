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
package com.splicemachine.spark2.splicemachine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class JdbcUrlIT extends FunSuite with Matchers {

  val defaultJDBCURL = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"
  val success = "*** SUCCESS ***"
  
  private def testJdbcUrl(url: String): String =
    try {
      new SplicemachineContext(url)
      success
    } catch {
      case e: Exception => e.toString
    }
  
  private def verify(url: String, result: String): Unit = {
    val res = testJdbcUrl( url )
    org.junit.Assert.assertTrue( res ,
      res.contains( result )
    )
  }
  
  test("JDBC Url Good") {
    verify(
      defaultJDBCURL ,
      success
    )
  }

  test("JDBC Url Empty String") {
    verify(
      "" ,
      "java.lang.Exception: JDBC Url is an empty string"
    )
  }

  test("JDBC Url Unknown Host") {
    verify(
      defaultJDBCURL.replace( "localhost" , "myhost" ) ,
      "java.net.UnknownHostException : Error connecting to server myhost"
    )
  }

  test("JDBC Url Bad Port") {
    verify(
      defaultJDBCURL.replace( "1527" , "3527" ) ,
      "java.net.ConnectException : Error connecting to server localhost on port 3527 with message Connection refused"
    )
  }

  val expInvUseridPwd = "java.sql.SQLNonTransientConnectionException: Connection authentication failure occurred.  Reason: userid or password invalid."

  test("JDBC Url Bad Userid") {
    verify(
      defaultJDBCURL.replace( "user=splice" , "user=none" ) ,
      expInvUseridPwd
    )
  }

  test("JDBC Url Bad Password") {
    verify(
      defaultJDBCURL.replace( "admin" , "badPwd" ) ,
      expInvUseridPwd
    )
  }

  test("JDBC Url Bad DB Name") {
    verify(
      defaultJDBCURL.replace( "splicedb" , "noDB" ) ,
      expInvUseridPwd
    )
  }

  test("JDBC Url Bad Driver Name") {
    verify(
      defaultJDBCURL.replace( "jdbc:splice" , "abc" ) ,
      "java.sql.SQLException: No suitable driver"
    )
  }
}
