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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ FunSuite, Matchers, Ignore}

@RunWith(classOf[JUnitRunner])
@Ignore //("DB-9562")
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
    if (splicemachineContext.tableExists(internalTN)) {
      splicemachineContext.dropTable(internalTN)
    }
    insertInternalRows(rowCount)
    val sqlContext = new SQLContext(sc)
    val schema = splicemachineContext.getSchema(internalTN)
    // SPARK-22002 removed metadata from StructFields in JdbcUtils.getSchema() (since Spark 2.3)
    org.junit.Assert.assertEquals("Schema Changed!",schema.json,"{\"type\":\"struct\",\"fields\":[{\"name\":\"C1_BOOLEAN\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C2_CHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C3_DATE\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C4_DECIMAL\",\"type\":\"decimal(15,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C5_DOUBLE\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C6_INT\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C7_BIGINT\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C8_FLOAT\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C9_SMALLINT\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C10_TIME\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C11_TIMESTAMP\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"C12_VARCHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}")
  }

  test("Test Get RDD") {
    if (splicemachineContext.tableExists(internalTN)) {
      splicemachineContext.dropTable(internalTN)
    }
    insertInternalRows(rowCount)
    val sqlContext = new SQLContext(sc)
    val rdd = splicemachineContext.rdd(internalTN,Seq("C2_CHAR","C7_BIGINT"))
    org.junit.Assert.assertEquals("Schema Changed!","List([0    ,0], [1    ,1], [2    ,2], [3    ,3], [4    ,4], [5    ,5], [6    ,6], [7    ,7], [null,8], [null,9])",rdd.collect().toList.toString)
  }

}
