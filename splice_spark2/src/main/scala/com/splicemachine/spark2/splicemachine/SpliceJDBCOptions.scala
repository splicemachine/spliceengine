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

package com.splicemachine.spark2.splicemachine


object SpliceJDBCOptions {
  private val jdbcOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    jdbcOptionNames += name.toLowerCase
    name
  }

  /**
    * JDBC Url including authentication mechanism (user/password, principal or principal/keytab)
    */
  val JDBC_URL = newOption("url")

  /**
    * Whether to create relations using SplicemachineContext.internalDf() by default. Defaults to "false"
    */
  val JDBC_INTERNAL_QUERIES = newOption("internal")


  /**
    * Temporary directory used by SplicemachineContext.internalDf() to hold temporary data. It has to be accessible by the client's user and SpliceMachine user
    */
  val JDBC_TEMP_DIRECTORY = newOption("tmp")
}
