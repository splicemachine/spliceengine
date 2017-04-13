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
import org.scalatest.{Ignore, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class SplicemachineContextTest extends FunSuite with TestContext with Matchers {
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

  /*
  test("Test Basic DataFrame") {
    if (splicemachineContext.tableExists(internalTN)) {
      splicemachineContext.dropTable(internalTN)
    }
    insertInternalRows(rowCount)
    val sqlContext = new SQLContext(sc)
    val dataDF = splicemachineContext.df("select * from "+internalTN)
    org.junit.Assert.assertEquals("Schema Changed!",dataDF.collectAsList().toString,"[[true,0    ,2013-09-04,0.00,0.0,0,0,0.0,0,2017-04-10 18:00:00.0,1969-12-31 18:00:00.0,1,sometestinfo0], [true,1    ,2013-09-04,1.00,1.0,1,1,1.0,1,2017-04-10 18:00:00.0,1969-12-31 18:00:00.001,1,sometestinfo1], [true,2    ,2013-09-04,2.00,2.0,2,2,2.0,2,2017-04-10 18:00:00.0,1969-12-31 18:00:00.002,1,sometestinfo2], [true,3    ,2013-09-04,3.00,3.0,3,3,3.0,3,2017-04-10 18:00:00.0,1969-12-31 18:00:00.003,1,sometestinfo3], [true,4    ,2013-09-04,4.00,4.0,4,4,4.0,4,2017-04-10 18:00:00.0,1969-12-31 18:00:00.004,1,sometestinfo4], [true,5    ,2013-09-04,5.00,5.0,5,5,5.0,5,2017-04-10 18:00:00.0,1969-12-31 18:00:00.005,1,sometestinfo5], [true,6    ,2013-09-04,6.00,6.0,6,6,6.0,6,2017-04-10 18:00:00.0,1969-12-31 18:00:00.006,1,sometestinfo6], [true,7    ,2013-09-04,7.00,7.0,7,7,7.0,7,2017-04-10 18:00:00.0,1969-12-31 18:00:00.007,1,sometestinfo7], [true,8    ,2013-09-04,8.00,8.0,8,8,8.0,8,2017-04-10 18:00:00.0,1969-12-31 18:00:00.008,1,sometestinfo8], [true,9    ,2013-09-04,9.00,9.0,9,9,9.0,9,2017-04-10 18:00:00.0,1969-12-31 18:00:00.009,1,sometestinfo9]]")
  }
  */

  test("Test Get Schema") {
    if (splicemachineContext.tableExists(internalTN)) {
      splicemachineContext.dropTable(internalTN)
    }
    insertInternalRows(rowCount)
    val sqlContext = new SQLContext(sc)
    val schema = splicemachineContext.getSchema(internalTN)
    org.junit.Assert.assertEquals("Schema Changed!",schema.json,"{\"type\":\"struct\",\"fields\":[{\"name\":\"C1_BOOLEAN\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{\"name\":\"C1_BOOLEAN\",\"scale\":0}},{\"name\":\"C2_CHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"name\":\"C2_CHAR\",\"scale\":0}},{\"name\":\"C3_DATE\",\"type\":\"date\",\"nullable\":true,\"metadata\":{\"name\":\"C3_DATE\",\"scale\":0}},{\"name\":\"C4_DECIMAL\",\"type\":\"decimal(15,2)\",\"nullable\":true,\"metadata\":{\"name\":\"C4_DECIMAL\",\"scale\":2}},{\"name\":\"C5_DOUBLE\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"name\":\"C5_DOUBLE\",\"scale\":0}},{\"name\":\"C6_INT\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"name\":\"C6_INT\",\"scale\":0}},{\"name\":\"C7_BIGINT\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"name\":\"C7_BIGINT\",\"scale\":0}},{\"name\":\"C8_FLOAT\",\"type\":\"double\",\"nullable\":true,\"metadata\":{\"name\":\"C8_FLOAT\",\"scale\":0}},{\"name\":\"C9_SMALLINT\",\"type\":\"short\",\"nullable\":true,\"metadata\":{\"name\":\"C9_SMALLINT\",\"scale\":0}},{\"name\":\"C10_TIME\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{\"name\":\"C10_TIME\",\"scale\":0}},{\"name\":\"C11_TIMESTAMP\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{\"name\":\"C11_TIMESTAMP\",\"scale\":9}},{\"name\":\"C12_VARCHAR\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"name\":\"C12_VARCHAR\",\"scale\":0}}]}")
  }

  test("Test Get RDD") {
    if (splicemachineContext.tableExists(internalTN)) {
      splicemachineContext.dropTable(internalTN)
    }
    insertInternalRows(rowCount)
    val sqlContext = new SQLContext(sc)
    val rdd = splicemachineContext.rdd(internalTN,Seq("C2_CHAR","C7_BIGINT"))
    org.junit.Assert.assertEquals("Schema Changed!",rdd.collect().toList.toString,"List([0    ,0], [1    ,1], [2    ,2], [3    ,3], [4    ,4], [5    ,5], [6    ,6], [7    ,7], [8    ,8], [9    ,9])")
  }

}
