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

  private def dropInternalTable(): Unit =
    if (splicemachineContext.tableExists(internalTN)) {
      splicemachineContext.dropTable(internalTN)
    }

  test("Test SplicemachineContext serialization") {
    val serialized = serialize(splicemachineContext)
    val deserialized = deserialize(serialized).asInstanceOf[SplicemachineContext]
    deserialized.tableExists("foo")
  }

  test("Test Get Schema") {
    dropInternalTable
    insertInternalRows(rowCount)
    val schema = splicemachineContext.getSchema(internalTN)
    // SPARK-22002 removed metadata from StructFields in JdbcUtils.getSchema() (since Spark 2.3)
    org.junit.Assert.assertEquals("Schema Changed!",schema.json,"{\"type\":\"struct\",\"fields\":[{\"name\":\"C1_BOOLEAN\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C2_CHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C3_DATE\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C4_DECIMAL\",\"type\":\"decimal(15,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C5_DOUBLE\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C6_INT\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"C7_BIGINT\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"C8_FLOAT\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C9_SMALLINT\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C10_TIME\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C11_TIMESTAMP\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C12_VARCHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}")
  }

  test("Test Get RDD") {
    dropInternalTable
    insertInternalRows(rowCount)
    val rdd = splicemachineContext.rdd(internalTN,Seq("C2_CHAR","C7_BIGINT"))
    org.junit.Assert.assertEquals("RDD Changed!","List([0    ,0], [1    ,1], [2    ,2], [3    ,3], [4    ,4], [5    ,5], [6    ,6], [7    ,7], [null,8], [null,9])",rdd.collect.toList.map(r => s"[${r(0)},${r(1)}]").toString)
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

    val bulkImportDirectory = new File( System.getProperty("java.io.tmpdir")+"/splice_spark2-SplicemachineContextIT/bulkImport" )
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
}
