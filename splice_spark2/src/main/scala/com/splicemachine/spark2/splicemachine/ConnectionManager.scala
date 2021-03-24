/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

import java.sql.{Connection, Savepoint}

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

@SerialVersionUID(20210317241L)
class ConnectionManager(url: String) extends Serializable {
  
  private val con: Connection = createConnection
  
  def autoCommitting(): Boolean = con.getAutoCommit
  def transactional(): Boolean = ! autoCommitting

  def getConnection(): Connection = con
  
  private def createConnection(): Connection =
    JdbcUtils.createConnectionFactory(
      new JDBCOptions( Map(
        JDBCOptions.JDBC_URL -> url,
        JDBCOptions.JDBC_TABLE_NAME -> "placeholder"
      ))
    )()
  
  def close(conToClose: Connection): Unit = {}
  def shutdown(): Unit = con.close
  
  def setAutoCommitOn(): Unit = con.setAutoCommit(true)
  
  def setAutoCommitOff(): Unit = con.setAutoCommit(false)
  
  def commit(): Unit = if( transactional ) {
    con.commit
  } else {
    throwNontransactionException("commit")
  }
  
  def rollback(): Unit = if( transactional ) {
    con.rollback
  } else {
    throwNontransactionException("rollback")
  }
  
  def rollback(savepoint: Savepoint): Unit = if( transactional ) {
    con.rollback(savepoint)
  } else {
    throwNontransactionException(s"rollback to savepoint")
  }

  def setSavepoint(): Savepoint = if( transactional ) {
    con.setSavepoint
  } else {
    throwNontransactionException("setSavepoint");
    null
  }
  
  def setSavepoint(name: String): Savepoint = if( transactional ) {
    con.setSavepoint(name)
  } else {
    throwNontransactionException(s"setSavepoint $name");
    null
  }
  
  def releaseSavepoint(savepoint: Savepoint): Unit = if( transactional ) {
    con.releaseSavepoint(savepoint)
  } else {
    throwNontransactionException(s"releaseSavepoint")
  }
  
  private def throwNontransactionException(op: String): Unit = throw new Exception(s"AutoCommit is on. Cannot $op")
}
