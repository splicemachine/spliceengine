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
package com.splicemachine.spark2.splicemachine.triggers

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class TriggerIT extends FunSuite with TestContext with Matchers {

  def createInsertTrigger(): Unit =
    execute(s"""CREATE TRIGGER INS_TRIGGER
               |AFTER INSERT ON $t1
               |REFERENCING NEW_TABLE AS NEW_ROWS
               |INSERT INTO $t2 (i,c) SELECT i,c FROM NEW_ROWS""".stripMargin)

  def dropInsertTrigger(): Unit =
    execute( "DROP TRIGGER INS_TRIGGER" )
  
  def createUpdateRowTrigger(): Unit =
    execute(s"""create trigger upd_row_trigger
               |after update on $t1
               |referencing new as new
               |for each row
               |insert into $t2 values (new.i, new.c)""".stripMargin)

  def dropUpdateRowTrigger(): Unit =
    execute( "DROP TRIGGER upd_row_trigger" )

  def createUpdateStatementTrigger(): Unit =
    execute(s"""create trigger upd_st_trigger
               |after update on $t1
               |referencing new table as NT
               |for each statement
               |insert into $t2 select i,c from NT""".stripMargin)

  def dropUpdateStatementTrigger(): Unit =
    execute( "DROP TRIGGER upd_st_trigger" )

  def truncateTables(): Unit = {
    execute( s"TRUNCATE TABLE $t1" )
    execute( s"TRUNCATE TABLE $t2" )
  }

  def contentOf: String => String = table => executeQuery(
    s"select * from $table",
    rs => {
      var s = Seq.empty[String]
      while( rs.next ) {
        s = s :+ s"${rs.getInt(1)},${rs.getString(2)}"
      }
      s.sorted.mkString("; ")
    }
  ).asInstanceOf[String]

  val expectedT2ContentAfterUpdate = "1,won"

  test("Test Insert with Trigger") {  // Added for DB-10707
    truncateTables
    createInsertTrigger
    splicemachineContext.insert( df, t1 )
    dropInsertTrigger
    org.junit.Assert.assertEquals( contentOf(t1), contentOf(t2) )
  }

  test("Test Update with Row Trigger") {
    truncateTables
    createUpdateRowTrigger
    splicemachineContext.insert( df, t1 )
    splicemachineContext.update( dfUpd, t1 )
    dropUpdateRowTrigger
    org.junit.Assert.assertEquals( expectedT2ContentAfterUpdate, contentOf(t2) )
  }

  test("Test Update with Statement Trigger") {
    truncateTables
    createUpdateStatementTrigger
    splicemachineContext.insert( df, t1 )
    splicemachineContext.update( dfUpd, t1 )
    dropUpdateStatementTrigger
    org.junit.Assert.assertEquals( expectedT2ContentAfterUpdate, contentOf(t2) )
  }

  test("Test Merge Into with Insert Trigger") {
    truncateTables
    createInsertTrigger
    splicemachineContext.mergeInto( df, t1 )
    dropInsertTrigger
    org.junit.Assert.assertEquals( contentOf(t1), contentOf(t2) )
  }

  test("Test Merge Into with Update Row Trigger") {
    truncateTables
    createUpdateRowTrigger
    splicemachineContext.insert( df, t1 )
    splicemachineContext.mergeInto( dfUpd, t1 )
    dropUpdateRowTrigger
    org.junit.Assert.assertEquals( expectedT2ContentAfterUpdate, contentOf(t2) )
  }

  test("Test Merge Into with Update Statement Trigger") {
    truncateTables
    createUpdateStatementTrigger
    splicemachineContext.insert( df, t1 )
    splicemachineContext.mergeInto( dfUpd, t1 )
    dropUpdateStatementTrigger
    org.junit.Assert.assertEquals( expectedT2ContentAfterUpdate, contentOf(t2) )
  }
}
