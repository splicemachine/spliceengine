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

import java.io._

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ColumnCaseIT extends FunSuite with TestContext with Matchers {
  
  val schemaNames = "A,a"

  test("Test Get Schema") {
    dropInternalTable
    createInternalTable
    val schema = splicemachineContext.getSchema(internalTN)
    org.junit.Assert.assertEquals(
      "Schema Changed!",
      schemaNames,
      schema.map(sf => sf.name).mkString(",")
    )
  }

  val df2Col = "[0,0], [1,1], [2,2], [3,3], [4,4], [5,5], [6,6], [7,7], [8,8], [9,9]"

  test("Test DF") {
    dropInternalTable
    insertInternalRows(10)
    val df = splicemachineContext.df(s"select * from $internalTN")
    org.junit.Assert.assertEquals(
      "DataFrame Changed!",
      df2Col,
      df.collect.map(r => s"[${r(0)},${r(1)}]").mkString(", ")
    )
  }

  test("Test Internal DF") {
    dropInternalTable
    insertInternalRows(10)
    val df = splicemachineContext.internalDf(s"select * from $internalTN")
    org.junit.Assert.assertEquals(
      "DataFrame Changed!",
      df2Col,
      df.collect.map(r => s"[${r(0)},${r(1)}]").mkString(", ")
    )
  }

  val rdd2Col = "List([0], [1], [2], [3], [4], [5], [6], [7], [8], [9])"

  test("Test Get RDD") {
    dropInternalTable
    insertInternalRows(10)
    val rdd = splicemachineContext.rdd(internalTN,Seq("A"))
    org.junit.Assert.assertEquals(
      "RDD Changed!",
      rdd2Col,
      rdd.collect.toList.map(r => s"[${r(0)}]").toString
    )
  }

  test("Test Get Internal RDD") {
    dropInternalTable
    insertInternalRows(10)
    val rdd = splicemachineContext.internalRdd(internalTN,Seq("a"))
    org.junit.Assert.assertEquals(
      "RDD Changed!",
      rdd2Col,
      rdd.collect.toList.map(r => s"[${r(0)}]").toString
    )
  }

  val rddAllCol5Row = """0, 0
                        |1, 1
                        |2, 2
                        |3, 3
                        |4, 4""".stripMargin

  test("Test Get RDD with Default Columns") {
    dropInternalTable
    insertInternalRows(10)
    val rdd = splicemachineContext.rdd(internalTN)
    org.junit.Assert.assertEquals(
      "RDD Changed!",
      rddAllCol5Row,
      rdd.map(_.toSeq)
        .map(_.mkString(", "))
        .takeOrdered(5)
        .reduce(_+"\n"+_)
    )
  }

  test("Test Get Internal RDD with Default Columns") {
    dropInternalTable
    insertInternalRows(10)
    val rdd = splicemachineContext.internalRdd(internalTN)
    org.junit.Assert.assertEquals(
      "RDD Changed!",
      rddAllCol5Row,
      rdd.map(_.toSeq)
        .map(_.mkString(", "))
        .takeOrdered(5)
        .reduce(_+"\n"+_)
    )
  }

  val carTableName = getClass.getSimpleName + "_TestCreateTable"
  val carSchemaTableName = schema+"."+carTableName

  def createCarTable(): Unit = {
    import org.apache.spark.sql.types._
    splicemachineContext.createTable(
      carSchemaTableName,
      StructType(
        StructField("NUMBER", IntegerType, false) ::
          StructField("ID", IntegerType, false) ::
          StructField("id", IntegerType, false) ::
          StructField("MAKE", StringType, true) ::
          StructField("MODEL", StringType, true) :: Nil),
      Seq("NUMBER")
    )
  }

  private def dropCarTable(): Unit = splicemachineContext.dropTable(carSchemaTableName)

  test("Test Create Table") {
    if( splicemachineContext.tableExists(carSchemaTableName) ) {
      splicemachineContext.dropTable(carSchemaTableName)
    }
    org.junit.Assert.assertFalse( splicemachineContext.tableExists(carSchemaTableName) )
    createCarTable
    org.junit.Assert.assertTrue( splicemachineContext.tableExists(carSchemaTableName) )
    dropCarTable
  }

  test("Test Delete") {
    dropInternalTable
    insertInternalRows(1)

    splicemachineContext.delete(internalTNDF, internalTN)

    org.junit.Assert.assertEquals("Delete Failed!", 0, rowCount(internalTN))
  }

  test("Test Insert") {
    dropInternalTable
    createInternalTable

    splicemachineContext.insert(internalTNDF, internalTN)

    org.junit.Assert.assertEquals("Insert Failed!", 1, rowCount(internalTN))
  }

  test("Test Insert Duplicate") {
    dropInternalTable
    createInternalTable

    splicemachineContext.insert(internalTNDF, internalTN)
    var msg = ""
    try {
      splicemachineContext.insert(internalTNDF, internalTN)
    } catch {
      case e: Exception => msg = e.toString
    }

    org.junit.Assert.assertTrue( msg.contains("duplicate key value") )
  }

  test("Test Insert with Trigger") {
    import org.apache.spark.sql.types._

    val sch = StructType(
      StructField("I", IntegerType, false) ::
      StructField("C", StringType, true) :: Nil
    )

    val t1 = s"$schema.foo"
    val t2 = s"$schema.bar"
    splicemachineContext.createTable(t1, sch, Seq("I") )
    splicemachineContext.createTable(t2, sch, Seq("I") )
    
    execute(s"""CREATE TRIGGER FOO_TRIGGER
               |AFTER INSERT ON $t1
               |REFERENCING NEW_TABLE AS NEW_ROWS
               |INSERT INTO $t2 (i,c) SELECT i,c FROM NEW_ROWS""".stripMargin)

    splicemachineContext.insert(
      dataframe(
        rdd( Seq( 
          Row.fromSeq( Seq(1,"one") ),
          Row.fromSeq( Seq(2,"two") ),
          Row.fromSeq( Seq(3,"three") )
        ) ),
        sch
      ),
      t1
    )

    def res: String => String = table => executeQuery(
      s"select * from $table",
      rs => {
        var s = Seq.empty[String]
        while( rs.next ) {
          s = s :+ s"${rs.getInt(1)},${rs.getString(2)}"
        }
        s.sorted.mkString("; ")
      }
    ).asInstanceOf[String]
    
    val res1 = res(t1)
    val res2 = res(t2)

    splicemachineContext.dropTable(t1)
    splicemachineContext.dropTable(t2)

    org.junit.Assert.assertEquals(res1, res2)
  }

  test("Test Bulk Import") {
    dropInternalTable
    createInternalTable

    val bulkImportDirectory = new File( System.getProperty("java.io.tmpdir")+s"/$module-ColumnCaseIT/bulkImport" )
    bulkImportDirectory.mkdirs()

    splicemachineContext.bulkImportHFile(internalTNDF, internalTN,
      collection.mutable.Map("bulkImportDirectory" -> bulkImportDirectory.getAbsolutePath)
    )

    org.junit.Assert.assertEquals("Bulk Import Failed!", 1, rowCount(internalTN))
  }

  test("Test SplitAndInsert") {
    dropInternalTable
    createInternalTable

    splicemachineContext.splitAndInsert(internalTNDF, internalTN, 0.5)

    org.junit.Assert.assertEquals("SplitAndInsert Failed!", 1, rowCount(internalTN))
  }

  test("Test Update") {
    dropInternalTable
    insertInternalRows(1)

    splicemachineContext.update(internalTNDF, internalTN)

    org.junit.Assert.assertEquals("Update Failed!",
      testRow.mkString(", "),
      executeQuery(
        s"select * from $internalTN",
        rs => {
          rs.next
          List(
            rs.getInt(1),
            rs.getInt(2)
          ).mkString(", ")
        }
      ).asInstanceOf[String]
    )
  }

  test("Test Upsert as Update") {
    dropInternalTable
    insertInternalRows(1)

    splicemachineContext.upsert(internalTNDF, internalTN)

    org.junit.Assert.assertEquals("Upsert Failed!",
      testRow.mkString(", "),
      executeQuery(
        s"select * from $internalTN",
        rs => {
          rs.next
          List(
            rs.getInt(1),
            rs.getInt(2)
          ).mkString(", ")
        }
      ).asInstanceOf[String]
    )
  }

  test("Test Upsert as Insert") {
    dropInternalTable
    createInternalTable

    splicemachineContext.upsert(internalTNDF, internalTN)

    org.junit.Assert.assertEquals("Upsert Failed!",
      testRow.mkString(", "),
      executeQuery(
        s"select * from $internalTN",
        rs => {
          rs.next
          List(
            rs.getInt(1),
            rs.getInt(2)
          ).mkString(", ")
        }
      ).asInstanceOf[String]
    )
  }

  test("Test Inserting Null") {
    dropInternalTable
    createInternalTable

    val insDf = dataframe(
      rdd( Seq(
        Row.fromSeq( Seq(
          106, null
        ))
      )),
      allTypesSchema(true)
    )

    splicemachineContext.insert(insDf, internalTN)

    val rs = getConnection.createStatement.executeQuery("select * from "+internalTN)
    rs.next

    val nonNull = collection.mutable.ListBuffer[String]()
    def check(i: Int, getResult: Int => Any): Unit ={
      val v = getResult(i)
      if( ! rs.wasNull ) nonNull += s"C$i == $v"
    }

    // primary key will not be null, skip 1
    check(2, rs.getInt)

    rs.close

    org.junit.Assert.assertEquals(
      "Not All Null",
      "\n\n",
      nonNull.mkString("\n","\n","\n")
    )
  }
}
