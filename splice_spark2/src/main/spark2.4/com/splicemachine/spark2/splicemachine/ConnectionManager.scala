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

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}

@SerialVersionUID(20210317241L)
class ConnectionManager(url: String) extends Serializable {
  
  private var con: Option[Connection] = None
  
  private def getJdbcOptionsInWrite(schemaTableName: String): JdbcOptionsInWrite =
    new JdbcOptionsInWrite( Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName
    ))

  def autoCommitting(): Boolean = con.isEmpty
  def transactional(): Boolean = ! autoCommitting

  def getConnection(schemaTableName: String): (Connection, JdbcOptionsInWrite) = if( autoCommitting ) {
    createConnection(schemaTableName)
  } else {
    (con.get, getJdbcOptionsInWrite(schemaTableName))
  }
  
  private def createConnection(schemaTableName: String): (Connection, JdbcOptionsInWrite) = {
    val jdbcOptionsInWrite = getJdbcOptionsInWrite( schemaTableName )
    val conn = JdbcUtils.createConnectionFactory( jdbcOptionsInWrite )()
    (conn, jdbcOptionsInWrite)
  }
  
  def close(conToClose: Connection): Unit = if( autoCommitting ) { conToClose.close }
  
  def setAutoCommitOn(): Unit = if( ! autoCommitting ) {
    con.get.setAutoCommit(true)
    con.get.close
    con = None
  }
  
  def setAutoCommitOff(): Unit = if( autoCommitting ) {
    val c = createConnection("placeholder")._1
    c.setAutoCommit(false)
    con = Some(c)
  }
  
  def commit(): Unit = if( transactional ) {
    con.get.commit
  } else {
    throwNontransactionException("commit")
  }
  
  def rollback(): Unit = if( transactional ) {
    con.get.rollback
  } else {
    throwNontransactionException("rollback")
  }
  
  def rollback(savepoint: Savepoint): Unit = if( transactional ) {
    con.get.rollback(savepoint)
  } else {
    throwNontransactionException(s"rollback to savepoint")
  }

  def setSavepoint(): Savepoint = if( transactional ) {
    con.get.setSavepoint
  } else {
    throwNontransactionException("setSavepoint");
    null
  }
  
  def setSavepoint(name: String): Savepoint = if( transactional ) {
    con.get.setSavepoint(name)
  } else {
    throwNontransactionException(s"setSavepoint $name");
    null
  }
  
  def releaseSavepoint(savepoint: Savepoint): Unit = if( transactional ) {
    con.get.releaseSavepoint(savepoint)
  } else {
    throwNontransactionException(s"releaseSavepoint")
  }
  
  private def throwNontransactionException(op: String): Unit = throw new Exception(s"AutoCommit is on. Cannot $op")
}
