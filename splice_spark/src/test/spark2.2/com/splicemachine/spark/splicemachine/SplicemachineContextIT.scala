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
package com.splicemachine.spark.splicemachine

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.{Row, SQLContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class SplicemachineContextIT extends FunSuite with TestContext with Matchers {
  val rowCount = 10

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
    insertInternalRows(rowCount)
    val sqlContext = new SQLContext(sc)
    val schema = splicemachineContext.getSchema(internalTN)
    org.junit.Assert.assertEquals(
      "Schema Changed!",
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"C1_BOOLEAN\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{\"name\":\"C1_BOOLEAN\",\"scale\":0}},{\"name\":\"C2_CHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"name\":\"C2_CHAR\",\"scale\":0}},{\"name\":\"C3_DATE\",\"type\":\"date\",\"nullable\":true,\"metadata\":{\"name\":\"C3_DATE\",\"scale\":0}},{\"name\":\"C4_NUMERIC\",\"type\":\"decimal(15,2)\",\"nullable\":true,\"metadata\":{\"name\":\"C4_DECIMAL\",\"scale\":2}},{\"name\":\"C5_DOUBLE\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"name\":\"C5_DOUBLE\",\"scale\":0}},{\"name\":\"C6_INT\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"name\":\"C6_INT\",\"scale\":0}},{\"name\":\"C7_BIGINT\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"name\":\"C7_BIGINT\",\"scale\":0}},{\"name\":\"C8_FLOAT\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"name\":\"C8_FLOAT\",\"scale\":0}},{\"name\":\"C9_SMALLINT\",\"type\":\"short\",\"nullable\":true,\"metadata\":{\"name\":\"C9_SMALLINT\",\"scale\":0}},{\"name\":\"C10_TIME\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{\"name\":\"C10_TIME\",\"scale\":0}},{\"name\":\"C11_TIMESTAMP\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{\"name\":\"C11_TIMESTAMP\",\"scale\":9}},{\"name\":\"C12_VARCHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"name\":\"C12_VARCHAR\",\"scale\":0}},{\"name\":\"C13_DECIMAL\",\"type\":\"decimal(4,1)\",\"nullable\":true,\"metadata\":{\"name\":\"C13_DECIMAL\",\"scale\":1}},{\"name\":\"C14_BIGINT\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"name\":\"C14_BIGINT\",\"scale\":0}},{\"name\":\"C15_LONGVARCHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"name\":\"C15_LONGVARCHAR\",\"scale\":0}},{\"name\":\"C16_REAL\",\"type\":\"float\",\"nullable\":true,\"metadata\":{\"name\":\"C16_REAL\",\"scale\":0}},{\"name\":\"C17_INT\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"name\":\"C17_INT\",\"scale\":0}}]}",
      schema.json
    )
  }

  test("Test Get RDD") {
    dropInternalTable
    insertInternalRows(rowCount)
    val sqlContext = new SQLContext(sc)
    val rdd = splicemachineContext.rdd(internalTN,Seq("C2_CHAR","C7_BIGINT"))
    org.junit.Assert.assertEquals("RDD Changed!","List([0    ,0], [1    ,1], [2    ,2], [3    ,3], [4    ,4], [5    ,5], [6    ,6], [7    ,7], [null,8], [null,9])",rdd.collect().toList.toString)
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

    val rs = getConnection.createStatement.executeQuery("select count(*) from "+internalTN)
    rs.next
    org.junit.Assert.assertEquals("Insert Failed!", 1, rs.getInt(1))
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

    val rs = getConnection.createStatement.executeQuery("select count(*) from "+internalTN)
    rs.next
    org.junit.Assert.assertEquals("Bulk Import Failed!", 1, rs.getInt(1))
  }

  test("Test SplitAndInsert") {
    dropInternalTable
    createInternalTable

    splicemachineContext.splitAndInsert(internalTNDF, internalTN, 0.5)

    val rs = getConnection.createStatement.executeQuery("select count(*) from "+internalTN)
    rs.next
    org.junit.Assert.assertEquals("SplitAndInsert Failed!", 1, rs.getInt(1))
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
