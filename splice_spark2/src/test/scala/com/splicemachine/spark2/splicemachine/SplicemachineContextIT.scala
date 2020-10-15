/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splicemachine.spark2.splicemachine

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class SplicemachineContextIT extends FunSuite with TestContext with Matchers {

  private def serialize(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    try {
      oos.writeObject(value)
      stream.toByteArray
    } finally {
      oos.close
    }
  }

  private def deserialize(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try {
      ois.readObject
    } finally {
      ois.close
    }
  }

  test("Test SplicemachineContext serialization") {
    val serialized = serialize(splicemachineContext)
    val deserialized = deserialize(serialized).asInstanceOf[SplicemachineContext]
    deserialized.tableExists("foo")
  }

  test("Test Get Schema") {
    dropInternalTable
    createInternalTable
    val schema = splicemachineContext.getSchema(internalTN)
    org.junit.Assert.assertEquals(
      "Schema Changed!",
      ThisVersionSpecificItems.schema,
      schema.json
    )
  }
  
  val rdd2Col = "List([0    ,0], [1    ,1], [2    ,2], [3    ,3], [4    ,4], [5    ,5], [6    ,6], [7    ,7], [null,8], [null,9])"

  test("Test Get RDD") {
    dropInternalTable
    insertInternalRows(10)
    val rdd = splicemachineContext.rdd(internalTN,Seq("C2_CHAR","C7_BIGINT"))
    org.junit.Assert.assertEquals(
      "RDD Changed!",
      rdd2Col,
      rdd.collect.toList.map(r => s"[${r(0)},${r(1)}]").toString
    )
  }

  test("Test Get Internal RDD") {
    dropInternalTable
    insertInternalRows(10)
    val rdd = splicemachineContext.internalRdd(internalTN,Seq("C2_CHAR","C7_BIGINT"))
    org.junit.Assert.assertEquals(
      "RDD Changed!",
      rdd2Col,
      rdd.collect.toList.map(r => s"[${r(0)},${r(1)}]").toString
    )
  }

  val rddAllCol5Row = """false, 1    , 2013-09-05, 1, 1.0, 1, 1, 1.0, 1, 00:00:01.0, 1970-01-01 00:00:00.001, sometestinfo1, 1, 1, long varchar sometestinfo1, 1.0, 1
                        |false, 3    , 2013-09-05, 3, 3.0, 3, 3, 3.0, 3, 00:00:03.0, 1970-01-01 00:00:00.003, sometestinfo3, 3, 3, long varchar sometestinfo3, 3.0, 3
                        |false, 5    , 2013-09-05, 5, 5.0, 5, 5, 5.0, 5, 00:00:05.0, 1970-01-01 00:00:00.005, sometestinfo5, 5, 5, long varchar sometestinfo5, 5.0, 5
                        |false, 7    , 2013-09-05, 7, 7.0, 7, 7, 7.0, 7, 00:00:07.0, 1970-01-01 00:00:00.007, sometestinfo7, 7, 7, long varchar sometestinfo7, 7.0, 7
                        |false, null, 2013-09-05, 9, 9.0, 9, 9, 9.0, 9, 00:00:09.0, 1970-01-01 00:00:00.009, null, 9, 9, null, 9.0, 9""".stripMargin

  test("Test Get RDD with Default Columns") {
    dropInternalTable
    insertInternalRows(10)
    val rdd = splicemachineContext.rdd(internalTN)
    org.junit.Assert.assertEquals(
      "RDD Changed!",
      rddAllCol5Row,
      rdd.map(_.toSeq)  // remove current date from field 9 to avoid hard-coding current date in rddAllCol5Row
        .map(sq => (sq.slice(0,9) :+ sq(9).toString.split(" ")(1)) ++ sq.slice(10,18))
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
      rdd.map(_.toSeq)  // remove current date from field 9 to avoid hard-coding current date in rddAllCol5Row
        .map(sq => (sq.slice(0,9) :+ sq(9).toString.split(" ")(1)) ++ sq.slice(10,18))
        .map(_.mkString(", "))
        .takeOrdered(5)
        .reduce(_+"\n"+_)
    )
  }

  val carTableName = getClass.getSimpleName + "_TestCreateTable"
  val carSchemaTableName = schema+"."+carTableName

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

  test("Test Create Table") {
    if( splicemachineContext.tableExists(carSchemaTableName) ) {
      splicemachineContext.dropTable(carSchemaTableName)
    }
    org.junit.Assert.assertFalse( splicemachineContext.tableExists(carSchemaTableName) )
    createCarTable
    org.junit.Assert.assertTrue( splicemachineContext.tableExists(carSchemaTableName) )
    dropCarTable
  }

  test("Test Table Exists (One Param)") {
    createCarTable
    org.junit.Assert.assertTrue(
      carSchemaTableName+" doesn't exist",
      splicemachineContext.tableExists(carSchemaTableName)
    )
    dropCarTable
  }

  test("Test Table Exists (Two Params)") {
    createCarTable
    org.junit.Assert.assertTrue(
      carSchemaTableName+" doesn't exist",
      splicemachineContext.tableExists(schema, carTableName)
    )
    dropCarTable
  }

  test("Test Table doesn't Exist (One Param)") {
    val name = schema+".testNonexistentTable"
    org.junit.Assert.assertFalse(
      name+" exists",
      splicemachineContext.tableExists(name)
    )
  }

  test("Test Table doesn't Exist (One Param, No Schema)") {
    val name = "testNonexistentTable"
    org.junit.Assert.assertFalse(
      name+" exists",
      splicemachineContext.tableExists(name)
    )
  }

  test("Test Table doesn't Exist (Two Params)") {
    val name = "testNonexistentTable"
    org.junit.Assert.assertFalse(
      schema+"."+name+" exists",
      splicemachineContext.tableExists(schema, name)
    )
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

  test("Test Bulk Import") {
    dropInternalTable
    createInternalTable

    val bulkImportDirectory = new File( System.getProperty("java.io.tmpdir")+s"/${module}-SplicemachineContextIT/bulkImport" )
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
      (testRow.slice(0,9) ::: new java.sql.Time(1000) :: testRow.slice(10,18)).mkString(", "),
      executeQuery(
        s"select * from $internalTN",
        rs => {
          rs.next
          List(
            rs.getBoolean(1),
            rs.getString(2),
            rs.getDate(3),
            rs.getBigDecimal(4),
            rs.getDouble(5),
            rs.getInt(6),
            rs.getInt(7),
            rs.getFloat(8),
            rs.getShort(9),
            rs.getTime(10),
            rs.getTimestamp(11),
            rs.getString(12),
            rs.getBigDecimal(13),
            rs.getInt(14),
            rs.getString(15),
            rs.getFloat(16),
            rs.getInt(17)
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
          null, null, null, null, null,
          106, 107L,
          null, null, null, null, null,
          null, null, null, null, null
        ))
      )),
      allTypesSchema(true)
    )

    splicemachineContext.insert(insDf, internalTN)

    val rs = getConnection.createStatement.executeQuery("select * from "+internalTN)
    rs.next

    val nonNull = collection.mutable.ListBuffer[String]()
    def check(i: Int, getResult: Int => Any): Unit = {
      var v = getResult(i)
      if( ! rs.wasNull ) nonNull += s"C$i == $v"
    }

    check(1, rs.getBoolean)
    check(2, rs.getString)
    check(3, rs.getDate)
    check(4, rs.getBigDecimal)
    check(5, rs.getDouble)
    // primary keys will not be null, skip 6 & 7
    check(8, rs.getFloat)
    check(9, rs.getShort)
    check(10, rs.getTime)
    check(11, rs.getTimestamp)
    check(12, rs.getString)
    check(13, rs.getBigDecimal)
    check(14, rs.getInt)
    check(15, rs.getString)
    check(16, rs.getFloat)
    check(17, rs.getInt)

    rs.close

    org.junit.Assert.assertEquals(
      "Not All Null",
      "\n\n",
      nonNull.mkString("\n","\n","\n")
    )
  }
}
