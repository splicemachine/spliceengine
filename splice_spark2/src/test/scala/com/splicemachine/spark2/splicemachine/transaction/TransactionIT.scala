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
package com.splicemachine.spark2.splicemachine.transaction

import java.sql.SQLException

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class TransactionIT extends FunSuite with TestContext with Matchers {

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

  def exists: String => Boolean = table => executeQuery(
    s"select count(*) from sys.systables where tablename='${table.toUpperCase}'",
    rs => { rs.next; rs.getInt(1) > 0 }
  ).asInstanceOf[Boolean]

  val tableData = "0,200"

  test("Test Flags") {
    splicemachineContext.setAutoCommitOff
    org.junit.Assert.assertTrue( "1", splicemachineContext.transactional )
    org.junit.Assert.assertFalse( "2", splicemachineContext.autoCommitting )
    
    splicemachineContext.setAutoCommitOn
    org.junit.Assert.assertTrue( "3", splicemachineContext.autoCommitting )
    org.junit.Assert.assertFalse( "4", splicemachineContext.transactional )
  }

  test("Test Savepoint") {
    truncateInternalTable
    splicemachineContext.setAutoCommitOff

    splicemachineContext.insert(internalTNDF, internalTN)
    
    val savepoint = splicemachineContext.setSavepoint

    splicemachineContext.insert(df1, internalTN)
    
    splicemachineContext.rollback(savepoint)

    org.junit.Assert.assertEquals( "1", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "2", "", contentOf(internalTN) )

    splicemachineContext.releaseSavepoint(savepoint)

    var caught = false
    try{
      splicemachineContext.rollback(savepoint)
    } catch {
      case e: SQLException => caught = true
    }
    org.junit.Assert.assertTrue( "3", caught )

    splicemachineContext.setAutoCommitOn
  }
  
  test("Test Savepoint Name") {
    splicemachineContext.setAutoCommitOff
    val name = "name1"
    val savepoint = splicemachineContext.setSavepoint(name)
    org.junit.Assert.assertEquals( name , savepoint.getSavepointName )
    splicemachineContext.releaseSavepoint(savepoint)
    splicemachineContext.setAutoCommitOn
  }
  
  test("Test Insert Commit") {
    truncateInternalTable
    splicemachineContext.setAutoCommitOff

    splicemachineContext.insert(internalTNDF, internalTN)
    
    org.junit.Assert.assertEquals( "1", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "2", "", contentOf(internalTN) )
    
    splicemachineContext.commit

    org.junit.Assert.assertEquals( "3", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "4", tableData, contentOf(internalTN) )

    splicemachineContext.setAutoCommitOn
  }

  test("Test Insert Rollback") {
    truncateInternalTable
    splicemachineContext.setAutoCommitOff

    splicemachineContext.insert(internalTNDF, internalTN)

    org.junit.Assert.assertEquals( "1", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "2", "", contentOf(internalTN) )

    splicemachineContext.rollback

    org.junit.Assert.assertEquals( "3", 0,
      splicemachineContext.df(s"select * from $internalTN").count
    )
    org.junit.Assert.assertEquals( "4", "", contentOf(internalTN) )

    splicemachineContext.setAutoCommitOn
  }

  test("Test Delete Commit") {
    truncateInternalTable
    insertInternalRows(1)
    splicemachineContext.setAutoCommitOff
    
    splicemachineContext.delete(internalTNDF, internalTN)

    org.junit.Assert.assertEquals( "1", 0,
      splicemachineContext.df(s"select * from $internalTN").count
    )
    org.junit.Assert.assertEquals( "2", tableData, contentOf(internalTN) )

    splicemachineContext.commit

    org.junit.Assert.assertEquals( "3", 0,
      splicemachineContext.df(s"select * from $internalTN").count
    )
    org.junit.Assert.assertEquals( "4", "", contentOf(internalTN) )

    splicemachineContext.setAutoCommitOn
  }

  test("Test Delete Rollback") {
    truncateInternalTable
    insertInternalRows(1)
    splicemachineContext.setAutoCommitOff

    splicemachineContext.delete(internalTNDF, internalTN)

    org.junit.Assert.assertEquals( "1", 0,
      splicemachineContext.df(s"select * from $internalTN").count
    )
    org.junit.Assert.assertEquals( "2", tableData, contentOf(internalTN) )

    splicemachineContext.rollback

    org.junit.Assert.assertEquals( "3", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "4", tableData, contentOf(internalTN) )

    splicemachineContext.setAutoCommitOn
  }
  
  test("Test MergeInto Commit") {
    truncateInternalTable
    insertInternalRows(1)
    splicemachineContext.setAutoCommitOff

    splicemachineContext.mergeInto(df300s, internalTN)

    org.junit.Assert.assertEquals( "1", "[0,300],[1,301]",
      splicemachineContext.df(s"select * from $internalTN").take(2).mkString(",")
    )
    org.junit.Assert.assertEquals( "2", tableData, contentOf(internalTN) )

    splicemachineContext.commit

    org.junit.Assert.assertEquals( "3", "[0,300],[1,301]",
      splicemachineContext.df(s"select * from $internalTN").take(2).mkString(",")
    )
    org.junit.Assert.assertEquals( "4", "0,300; 1,301", contentOf(internalTN) )

    splicemachineContext.setAutoCommitOn
  }

  test("Test MergeInto Rollback") {
    truncateInternalTable
    insertInternalRows(1)
    splicemachineContext.setAutoCommitOff

    splicemachineContext.mergeInto(df300s, internalTN)

    org.junit.Assert.assertEquals( "1", "[0,300],[1,301]",
      splicemachineContext.df(s"select * from $internalTN").take(2).mkString(",")
    )
    org.junit.Assert.assertEquals( "2", tableData, contentOf(internalTN) )

    splicemachineContext.rollback

    org.junit.Assert.assertEquals( "3", "["+tableData+"]",
      splicemachineContext.df(s"select * from $internalTN").take(2).mkString(",")
    )
    org.junit.Assert.assertEquals( "4", tableData, contentOf(internalTN) )
    
    splicemachineContext.setAutoCommitOn
  }

  private val carTableName = "transactionCarTable"
  private val carSchemaTableName = schema+"."+carTableName
  private val carTableSchemaString = "StructType(StructField(NUMBER,IntegerType,false), StructField(MAKE,StringType,true), StructField(MODEL,StringType,true))"
  
  private def createCarTable(): Unit = {
    import org.apache.spark.sql.types._
    splicemachineContext.createTable(
      carSchemaTableName,
      StructType(
        StructField("NUMBER", IntegerType, false) ::
          StructField("MAKE", StringType, true) ::
          StructField("MODEL", StringType, true) :: Nil),
      Seq("NUMBER")
    )
  }

  private def dropCarTable(): Unit = splicemachineContext.dropTable(carSchemaTableName)

  test("Test CreateTable Commit") {
    splicemachineContext.setAutoCommitOff

    createCarTable

    org.junit.Assert.assertTrue( "1", splicemachineContext.tableExists( carSchemaTableName ) )
    org.junit.Assert.assertFalse( "2", exists(carTableName) )
    org.junit.Assert.assertEquals( "3", carTableSchemaString , splicemachineContext.getSchema( carSchemaTableName ).toString )

    splicemachineContext.commit

    org.junit.Assert.assertTrue( "4", splicemachineContext.tableExists( carSchemaTableName ) )
    org.junit.Assert.assertTrue( "5", exists(carTableName) )
    org.junit.Assert.assertEquals( "6", carTableSchemaString , splicemachineContext.getSchema( carSchemaTableName ).toString )

    splicemachineContext.setAutoCommitOn
    dropCarTable
  }

  test("Test CreateTable Rollback") {
    splicemachineContext.setAutoCommitOff

    createCarTable

    org.junit.Assert.assertTrue( "1", splicemachineContext.tableExists( carSchemaTableName ) )
    org.junit.Assert.assertFalse( "2", exists(carTableName) )
    org.junit.Assert.assertEquals( "3", carTableSchemaString , splicemachineContext.getSchema( carSchemaTableName ).toString )

    splicemachineContext.rollback

    org.junit.Assert.assertFalse( "4", splicemachineContext.tableExists( carSchemaTableName ) )
    org.junit.Assert.assertFalse( "5", exists(carTableName) )
    noSchema("6")

    splicemachineContext.setAutoCommitOn
  }
  
  private def noSchema(msg: String): Unit = {
    var caught = false
    try{
      splicemachineContext.getSchema( carSchemaTableName )
    } catch {
      case e: Exception => caught = e.getMessage.contains("does not exist")
    }
    org.junit.Assert.assertTrue( msg, caught )
  }

  test("Test DropTable Commit") {
    createCarTable
    splicemachineContext.setAutoCommitOff

    dropCarTable

    org.junit.Assert.assertFalse( "1", splicemachineContext.tableExists( carSchemaTableName ) )
    org.junit.Assert.assertTrue( "2", exists(carTableName) )
    noSchema("3")

    splicemachineContext.commit

    org.junit.Assert.assertFalse( "4", splicemachineContext.tableExists( carSchemaTableName ) )
    org.junit.Assert.assertFalse( "5", exists(carTableName) )
    noSchema("6")

    splicemachineContext.setAutoCommitOn
  }

  test("Test DropTable Rollback") {
    createCarTable
    splicemachineContext.setAutoCommitOff

    dropCarTable

    org.junit.Assert.assertFalse( "1", splicemachineContext.tableExists( carSchemaTableName ) )
    org.junit.Assert.assertTrue( "2", exists(carTableName) )
    noSchema("3")

    splicemachineContext.rollback

    org.junit.Assert.assertTrue( "4", splicemachineContext.tableExists( carSchemaTableName ) )
    org.junit.Assert.assertTrue( "5", exists(carTableName) )
    org.junit.Assert.assertEquals( "6", carTableSchemaString , splicemachineContext.getSchema( carSchemaTableName ).toString )

    splicemachineContext.setAutoCommitOn
    dropCarTable
  }

  test("Test Truncate Commit") {
    truncateInternalTable
    insertInternalRows(1)
    splicemachineContext.setAutoCommitOff

    splicemachineContext.truncateTable(internalTN)

    org.junit.Assert.assertEquals( "1", 0,
      splicemachineContext.df(s"select * from $internalTN").count
    )
    org.junit.Assert.assertEquals( "2", tableData, contentOf(internalTN) )

    splicemachineContext.commit

    org.junit.Assert.assertEquals( "3", 0,
      splicemachineContext.df(s"select * from $internalTN").count
    )
    org.junit.Assert.assertEquals( "4", "", contentOf(internalTN) )

    splicemachineContext.setAutoCommitOn
  }

  test("Test Truncate Rollback") {
    truncateInternalTable
    insertInternalRows(1)
    splicemachineContext.setAutoCommitOff

    splicemachineContext.truncateTable(internalTN)

    org.junit.Assert.assertEquals( "1", 0,
      splicemachineContext.df(s"select * from $internalTN").count
    )
    org.junit.Assert.assertEquals( "2", tableData, contentOf(internalTN) )

    splicemachineContext.rollback

    org.junit.Assert.assertEquals( "3", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "4", tableData, contentOf(internalTN) )

    splicemachineContext.setAutoCommitOn
  }

  test("Test Switch Modes") {
    truncateInternalTable
    splicemachineContext.setAutoCommitOn

    splicemachineContext.insert(internalTNDF, internalTN)

    org.junit.Assert.assertEquals( "1", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "2", tableData, contentOf(internalTN) )

    truncateInternalTable
    splicemachineContext.setAutoCommitOff

    splicemachineContext.insert(internalTNDF, internalTN)

    org.junit.Assert.assertEquals( "3", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "4", "", contentOf(internalTN) )

    splicemachineContext.commit

    org.junit.Assert.assertEquals( "5", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "6", tableData, contentOf(internalTN) )

    truncateInternalTable
    splicemachineContext.setAutoCommitOn

    splicemachineContext.insert(internalTNDF, internalTN)

    org.junit.Assert.assertEquals( "7", tableData,
      splicemachineContext.df(s"select * from $internalTN").first.mkString(",")
    )
    org.junit.Assert.assertEquals( "8", tableData, contentOf(internalTN) )
  }
}
