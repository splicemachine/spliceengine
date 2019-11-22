/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.spark.splicemachine

import java.io.File
import java.math.BigDecimal
import java.nio.file.{Files, Path}
import java.sql.{Time, Timestamp}
import java.util.Date

import com.splicemachine.derby.vti.SpliceDatasetVTI
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects

import scala.collection.immutable.IndexedSeq
import org.apache.spark.sql._
import org.junit.runner.RunWith
import org.junit.Assert._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.functions._
import java.sql.Connection

@RunWith(classOf[JUnitRunner])
class DefaultSourceTest extends FunSuite with TestContext with BeforeAndAfter with Matchers {
  val rowCount = 10
  var sqlContext : SQLContext = _
  var rows : IndexedSeq[(Int, Int, String, Long)] = _

  before {
    val rowCount = 10
    if (sqlContext == null)
      sqlContext = new SQLContext(sc)
    if (splicemachineContext.tableExists(internalTN)) {
      splicemachineContext.dropTable(internalTN)
    }
    if (splicemachineContext.tableExists(schema+"."+"T")) {
      splicemachineContext.dropTable(schema+"."+"T")
    }
    if (splicemachineContext.tableExists(schema+"."+"T2")) {
      splicemachineContext.dropTable(schema+"."+"T2")
    }
    insertInternalRows(rowCount)
    splicemachineContext.getConnection().commit()
    sqlContext.read.options(internalOptions).splicemachine.createOrReplaceTempView(table)
  }

  test("read from datasource api") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    assert(splicemachineContext.tableExists(internalTN))
    assert(df.count == 10)
  }


  test("read from internal execution datasource api") {
    val df = sqlContext.read.options(internalExecutionOptions).splicemachine
    df.printSchema()
    assert(splicemachineContext.getSchema(internalTN).equals(df.schema))
    assert(splicemachineContext.tableExists(internalTN))
    assert(df.count == 10)
    val result = df.collect()
    assert(result.length == 10)
  }

  test("read from internal execution with non-escaped characters") {
    val conn = JdbcUtils.createConnectionFactory(internalJDBCOptions)()
    try {
      val ps = conn.prepareStatement("insert into " + internalTN + allTypesInsertString + allTypesInsertStringValues)
      ps.setBoolean(1, false)
      ps.setString(2, "\n")
      ps.setDate(3, java.sql.Date.valueOf("2013-09-05"))
      ps.setBigDecimal(4, new BigDecimal("11"))
      ps.setDouble(5, 11)
      ps.setInt(6, 11)
      ps.setInt(7, 11)
      ps.setFloat(8, 11)
      ps.setShort(9, 11.toShort)
      ps.setTime(10, new Time(11))
      ps.setTimestamp(11, new Timestamp(11))
      ps.setString(12, "somet\nestinfo" + 11)
      ps.execute()
    }finally {
      conn.close()
    }

    val df = sqlContext.read.options(internalExecutionOptions).splicemachine
    df.printSchema()
    assert(splicemachineContext.getSchema(internalTN).equals(df.schema))
    assert(splicemachineContext.tableExists(internalTN))
    assert(df.count == 11)
    val result = df.collect()
    assert(result.length == 11)
  }

  test("insertion") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(10)) )
    splicemachineContext.insert(changedDF, internalTN)
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assert(newDF.count == 20)
  }

  test("insertion with sampling") {
    val userDir: String = System.getProperty("user.dir")
    val dataDir = userDir+"/src/test/data/lineitem.csv";
    val conn = JdbcUtils.createConnectionFactory(internalJDBCOptions)()
    conn.createStatement().execute("create table TestContext.T(id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), c1 double, c2 double, c3 double, primary key(id))")
    conn.createStatement().execute("insert into TestContext.T(c1,c2,c3) values (100, 100, 100), (200, 200, 200), (300, 300, 300), (400, 400, 400)");
    for (i <- 0 to 20) {
      conn.createStatement().execute("insert into TestContext.T(c1,c2,c3) select c1,c2,c3 from TestContext.t")
    }
    conn.createStatement().execute("create table TestContext.T2(id int, c1 double, c2 double, c3 double, primary key(id))")
    val options = Map(
      JDBCOptions.JDBC_TABLE_NAME -> (schema+"."+"T"),
      JDBCOptions.JDBC_URL -> defaultJDBCURL
    )
    val df = sqlContext.read.options(options).splicemachine

    val options2 = Map(
      JDBCOptions.JDBC_TABLE_NAME -> (schema+"."+"T2"),
      JDBCOptions.JDBC_URL -> defaultJDBCURL
    )
    splicemachineContext.splitAndInsert(df, schema+"."+"T2", 0.001)
    val newDF = sqlContext.read.options(options2).splicemachine
    assert(newDF.count == 8388608)
  }

  test("insertion using RDD") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(10)))
    splicemachineContext.insert(changedDF.rdd, changedDF.schema, internalTN)
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assert(newDF.count == 20)
  }

  test("insertion reading from internal execution") {
    val df = sqlContext.read.options(internalExecutionOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(10)))
    splicemachineContext.insert(changedDF.rdd, changedDF.schema, internalTN)
    val newDF = sqlContext.read.options(internalExecutionOptions).splicemachine
    assert(newDF.count == 20)
    val result = df.collect()
    assert(result.length == 20)
  }


  test("commit insertion") {
    val conn : Connection = splicemachineContext.getConnection()
    conn.setAutoCommit(false);
    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(10)) )
    splicemachineContext.insert(changedDF, internalTN)
    conn.commit();
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    conn.setAutoCommit(true);
    assert(newDF.count == 20)
  }

  test("rollback  insertion") {
    val conn: Connection = splicemachineContext.getConnection()
    conn.setAutoCommit(false);
    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(10)))
    splicemachineContext.insert(changedDF, internalTN)
    conn.rollback();
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    conn.setAutoCommit(true);
    assert(newDF.count == 10)
  }

  test("insertion with bad records file") {
    val statusDirectory = createBadDirectory("DST_bulkImport2")
    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(10)) )
    val doubleIt = changedDF.union(df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(10))))
    splicemachineContext.insert(doubleIt, internalTN,statusDirectory.getAbsolutePath,100)
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assert(newDF.count == 20)
    assert(sqlContext.sparkContext.textFile(statusDirectory.getAbsolutePath).count()==10)
  }

  test("insertion with bad records file using RDD") {
    val statusDirectory = createBadDirectory("DST_bulkImport3")
    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(10)))
    val doubleIt = changedDF.union(df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(10))))
    splicemachineContext.insert(doubleIt.rdd, changedDF.schema, internalTN,statusDirectory.getAbsolutePath,100)
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assert(newDF.count == 20)
    assert(sqlContext.sparkContext.textFile(statusDirectory.getAbsolutePath).count()==10)
  }

  test("upsert") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(5)) )
    .withColumn("C7_BIGINT", when(col("C7_BIGINT").leq(10), col("C7_BIGINT").plus(5)) )
    splicemachineContext.upsert(changedDF, internalTN)
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assert(newDF.count == 15)
  }

  test("upsert using rdd") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(5)) )
      .withColumn("C7_BIGINT", when(col("C7_BIGINT").leq(10), col("C7_BIGINT").plus(5)) )
    splicemachineContext.upsert(changedDF.rdd, changedDF.schema, internalTN)
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assert(newDF.count == 15)
  }

  test("truncate table") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    assert(df.count == 10)
    splicemachineContext.truncateTable(internalTN)
    val df2 = sqlContext.read.options(internalOptions).splicemachine
    assert(df2.count == 0)
  }

  test("analyze table") {
      splicemachineContext.analyzeTable(internalTN)
      val df = sqlContext.read.options(statOptions).splicemachine.filter(s"SCHEMANAME = '${schema.toUpperCase}' and TABLENAME = '${table.toUpperCase}'")
      assert(df.count == 1)
  }

  test("analyze table with sampling") {
    splicemachineContext.analyzeTable(internalTN,true) // 10% default
    val df = sqlContext.read.options(statOptions).splicemachine.filter(s"SCHEMANAME = '${schema.toUpperCase}' and TABLENAME = '${table.toUpperCase}'")
    assert(df.count == 1)
  }

  test("bulkImportHFile") {
    val bulkImportOptions = scala.collection.mutable.Map(
      "useSpark" -> "true",
      "skipSampling" -> "true"
    )
    val tmpDir: String = System.getProperty("java.io.tmpdir");
    val bulkImportDirectory: File = new File(tmpDir, "bulkImport")
    bulkImportDirectory.mkdirs()
    val statusDirectory: File = new File(bulkImportDirectory, "BAD")
    statusDirectory.mkdir()

    bulkImportOptions += ("bulkImportDirectory" -> bulkImportDirectory.getAbsolutePath);
    bulkImportOptions += ("statusDirectory" -> statusDirectory.getAbsolutePath);

    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(20)) )

    splicemachineContext.bulkImportHFile(changedDF, internalTN, bulkImportOptions)
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assert(newDF.count == 20)
  }

  test("bulkImportHFile using rdd") {
    val bulkImportOptions = scala.collection.mutable.Map(
      "useSpark" -> "true",
      "skipSampling" -> "true"
    )
    val tmpDir: String = System.getProperty("java.io.tmpdir");
    val bulkImportDirectory: File = new File(tmpDir, "bulkImport")
    bulkImportDirectory.mkdirs()
    val statusDirectory: File = new File(bulkImportDirectory, "BAD")
    statusDirectory.mkdir()

    bulkImportOptions += ("bulkImportDirectory" -> bulkImportDirectory.getAbsolutePath);
    bulkImportOptions += ("statusDirectory" -> statusDirectory.getAbsolutePath);

    val df = sqlContext.read.options(internalOptions).splicemachine
    val changedDF = df.withColumn("C6_INT", when(col("C6_INT").leq(10), col("C6_INT").plus(20)) )

    splicemachineContext.bulkImportHFile(changedDF.rdd, changedDF.schema, internalTN, bulkImportOptions)
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assert(newDF.count == 20)
  }


  test("binary export") {
    val tmpDir: String = System.getProperty("java.io.tmpdir");
    val outDirectory: Path = Files.createTempDirectory("exportBinary")


    val df = sqlContext.read.options(internalOptions).splicemachine
    splicemachineContext.exportBinary(df, outDirectory.toString, false, "parquet")


    val newDF = sqlContext.read.parquet(outDirectory.toString)
    assert(newDF.count == 10)
  }

  test ("deletion") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    val deleteDF = df.filter("c6_int < 5").select("C6_INT","C7_BIGINT")
    splicemachineContext.delete(deleteDF, internalTN)
    // read the data back
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assertEquals(5, newDF.filter("c6_int < 10").count())
  }

  test ("deletion using rdd") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    val deleteDF = df.filter("c6_int < 5").select("C6_INT","C7_BIGINT")
    splicemachineContext.delete(deleteDF.rdd, deleteDF.schema, internalTN)
    // read the data back
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assertEquals(5, newDF.filter("c6_int < 10").count())
  }

  test ("deletion no primary keys") {
    val conn = JdbcUtils.createConnectionFactory(externalJDBCOptions)()
    if (!splicemachineContext.tableExists(externalTN))
      conn.createStatement().execute("create table "+externalTN + this.allTypesCreateStringWithoutPrimaryKey)
    val df = sqlContext.read.options(externalOptions).splicemachine
    val deleteDF = df.filter("c6_int < 5").select("C6_INT","C7_BIGINT")
    val thrown = intercept[UnsupportedOperationException] {
      splicemachineContext.delete(deleteDF, externalTN)
    }
    assert(thrown.getMessage == "Primary Key Required for the Table to Perform Deletes")
  }

  test ("deletion no primary keys using rdd") {
    val conn = JdbcUtils.createConnectionFactory(externalJDBCOptions)()
    if (!splicemachineContext.tableExists(externalTN))
      conn.createStatement().execute("create table "+externalTN + this.allTypesCreateStringWithoutPrimaryKey)
    val df = sqlContext.read.options(externalOptions).splicemachine
    val deleteDF = df.filter("c6_int < 5").select("C6_INT","C7_BIGINT")
    val thrown = intercept[UnsupportedOperationException] {
      splicemachineContext.delete(deleteDF.rdd, deleteDF.schema, externalTN)
    }
    assert(thrown.getMessage == "Primary Key Required for the Table to Perform Deletes")
  }

  test ("update") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    val updatedDF = df
      .filter("C6_INT < 5")
      .select("C6_INT","C7_BIGINT","C8_FLOAT","C9_SMALLINT")
    .withColumn("C8_FLOAT", when(col("C8_FLOAT").leq(10.0), col("C8_FLOAT").plus(10.0)) )
    splicemachineContext.update(updatedDF, internalTN)
    // read the data back
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assertEquals(5, newDF.filter("c8_float >= 10.0").count())
  }

  test ("update without primary key throws Exception") {
    val conn = JdbcUtils.createConnectionFactory(externalJDBCOptions)()
    if (!splicemachineContext.tableExists(externalTN))
      conn.createStatement().execute("create table "+externalTN + this.allTypesCreateStringWithoutPrimaryKey)
    val df = sqlContext.read.options(externalOptions).splicemachine
    val updatedDF = df
      .filter("C6_INT < 5")
      .select("C6_INT","C7_BIGINT","C8_FLOAT","C9_SMALLINT")
      .withColumn("C8_FLOAT", when(col("C8_FLOAT").leq(10.0), col("C8_FLOAT").plus(10.0)) )
    val thrown = intercept[UnsupportedOperationException] {
      splicemachineContext.update(updatedDF, externalTN)
    }
    assert(thrown.getMessage == "Primary Key Required for the Table to Perform Updates")
  }

  test ("update without primary key throws Exception for RDD") {
    val conn = JdbcUtils.createConnectionFactory(externalJDBCOptions)()
    if (!splicemachineContext.tableExists(externalTN))
      conn.createStatement().execute("create table "+externalTN + this.allTypesCreateStringWithoutPrimaryKey)
    val df = sqlContext.read.options(externalOptions).splicemachine
    val updatedDF = df
      .filter("C6_INT < 5")
      .select("C6_INT","C7_BIGINT","C8_FLOAT","C9_SMALLINT")
      .withColumn("C8_FLOAT", when(col("C8_FLOAT").leq(10.0), col("C8_FLOAT").plus(10.0)) )
    val thrown = intercept[UnsupportedOperationException] {
      splicemachineContext.update(updatedDF.rdd, updatedDF.schema, externalTN)
    }
    assert(thrown.getMessage == "Primary Key Required for the Table to Perform Updates")
  }

  test ("update using rdd") {
    val df = sqlContext.read.options(internalOptions).splicemachine
    val updatedDF = df
      .filter("C6_INT < 5")
      .select("C6_INT","C7_BIGINT","C8_FLOAT","C9_SMALLINT")
      .withColumn("C8_FLOAT", when(col("C8_FLOAT").leq(10.0), col("C8_FLOAT").plus(10.0)) )
    splicemachineContext.update(updatedDF.rdd, updatedDF.schema, internalTN)
    // read the data back
    val newDF = sqlContext.read.options(internalOptions).splicemachine
    assertEquals(5, newDF.filter("c8_float >= 10.0").count())
  }

  test("table scan") {
      val results = sqlContext.sql(s"SELECT * FROM $table").collectAsList()
      assert(results.size() == rowCount)
      assert(results.get(1).get(5).equals(1))
  }
  
  test("table scan with projection") {
    assertEquals("[[0], [1], [2], [3], [4], [5], [6], [7], [8], [9]]", sqlContext.sql(s"""SELECT c6_int FROM $table""").collectAsList().toString)
  }

  test("partitions shuffle") {
    val rdd = generateRows(40, 20)

    var i = 0

    rdd.toLocalIterator.foreach ( r => {
      assertEquals(i, r(5))
      i += 1
    })

    i = 0
    var same = true
    ShuffleUtils.shuffle(rdd).toLocalIterator.foreach( r => {
      if (i != r(5))
        same = false
      i += 1
    })
    assertFalse("Rows were in order", same)
  }

  def generateRows(rowCount: Int, batches: Int): RDD[Row] = {
    val rows = Range(0, rowCount).map ( i => {
      val a:Boolean = i % 2==0
      val b:String = if (i < 8)"" + i else null
      val c:Date = if (i % 2==0) java.sql.Date.valueOf("2013-09-04") else java.sql.Date.valueOf("2013-09-05")
      val d:BigDecimal = new BigDecimal("" + i)
      val e:Double = i
      val f:Int = i
      val g:Long = i
      val h:Double = i
      val j:Short = i.toShort
      val k:Timestamp = new Timestamp(i)
      val l:Timestamp = new Timestamp(i)
      val m:String = if (i < 8) "sometestinfo" + i else null
      Row(a,b,c,d,e,f,g,h,j,k,l,m)
    })
    sqlContext.sparkContext.parallelize(rows, batches)
  }
  
  test("table scan with projection and predicate bool") {
    assertEquals("[[true], [true], [true], [true], [true]]",
      sqlContext.sql(s"""SELECT c1_boolean FROM $table where c1_boolean = true""").collectAsList().toString)
  }

  test("table scan with projection and predicate char with no padding") {
    assertEquals("[[5    ]]",
      sqlContext.sql(s"""SELECT c2_char FROM $table where c2_char = "5"""").collectAsList().toString)
  }

  test("table scan with projection and predicate date") {
    assertEquals("[[2013-09-04], [2013-09-04], [2013-09-04], [2013-09-04], [2013-09-04]]",
      sqlContext.sql(s"""SELECT c3_date FROM $table where c3_date < "2013-09-05"""").collectAsList().toString)
  }

  test("table scan with projection and predicate numeric") {
    assertEquals("[[0.00], [1.00], [2.00], [3.00], [4.00]]",
      sqlContext.sql(s"""SELECT c4_decimal FROM $table where c4_decimal < 5.0000""").collectAsList().toString)
  }

  test("table scan with projection and predicate double") {
    assertEquals("[[0.0], [1.0], [2.0], [3.0], [4.0]]",
      sqlContext.sql(s"""SELECT c5_double FROM $table where c5_double < 5.0000""").collectAsList().toString)
  }

  test("table scan with projection and predicate int") {
    assertEquals("[[0], [1], [2], [3], [4]]",
      sqlContext.sql(s"""SELECT c6_int FROM $table where c6_int < 5""").collectAsList().toString)
  }

  test("table scan with projection and predicate bigint") {
    assertEquals("[[0], [1], [2], [3], [4]]",
      sqlContext.sql(s"""SELECT c7_bigint FROM $table where c7_bigint < 5""").collectAsList().toString)
  }

  test("table scan with projection and predicate float") {
    assertEquals("[[0.0], [1.0], [2.0], [3.0], [4.0], [5.0]]",
      sqlContext.sql(s"""SELECT c8_float FROM $table where c8_float < 5.23""").collectAsList().toString)
  }

  test("table scan with projection and predicate smallint") {
    assertEquals("[[0], [1], [2], [3], [4]]",
      sqlContext.sql(s"""SELECT c9_smallint FROM $table where c9_smallint < 5""").collectAsList().toString)
  }
  test("table scan with projection and predicate timestamp") {
    val ts0 = new Timestamp(0)
    val ts1 = new Timestamp(1)
    val ts2 = new Timestamp(2)
    val ts3 = new Timestamp(3)
    val ts4 = new Timestamp(4)
    val ts5 = new Timestamp(5)

    val results = String.format("[[%s], [%s], [%s], [%s], [%s]]", ts0, ts1, ts2, ts3, ts4)
    assertEquals(results,
      sqlContext.sql(s"""SELECT c11_timestamp FROM $table where c11_timestamp < "$ts5"""").collectAsList().toString)
  }


  test("table scan with projection and predicate varchar") {
    assertEquals("[[sometestinfo5]]",
      sqlContext.sql(s"""SELECT c12_varchar FROM $table where c12_varchar = "sometestinfo5"""").collectAsList().toString)
  }

  test("table scan with 2 predicates") {
    assertEquals("[[3], [4]]",
      sqlContext.sql(s"""SELECT c6_int FROM $table where c8_float < 5.0 and c5_double > 2.0""").collectAsList().toString)
  }

  test("table scan with in list predicates") {
    val keys = Array(1, 5, 7)
    assertEquals("[[1], [5], [7]]",
      sqlContext.sql(s"""SELECT c6_int FROM $table where c6_int in (${keys.mkString(", ")})""").collectAsList().toString)
  }

  test("table scan with in list predicates on string") {
    val keys = Array("sometestinfo1", "sometestinfo5", "sometestinfo7")
    assertEquals("[[sometestinfo1], [sometestinfo5], [sometestinfo7]]",
      sqlContext.sql(s"""SELECT c12_varchar FROM $table where c12_varchar in (${keys.mkString("'", "', '", "'")})""").collectAsList().toString)
  }

  test("table scan with in list and comparison predicate") {
    val keys = Array(1, 5, 7)
    assertEquals("[[1], [5]]",
      sqlContext.sql(s"""SELECT c6_int FROM $table where c6_int < 7 and c6_int in (${keys.mkString("'", "', '", "'")})""").collectAsList().toString)
  }

    test("Test SparkSQL StringStartsWith filters") {
      assertEquals("[[0], [1], [2], [3], [4], [5], [6], [7]]",
        sqlContext.sql(s"""SELECT c6_int FROM $table where c12_varchar like "sometest%"""").collectAsList().toString)
    }

    test("Test SparkSQL IS NULL predicate") {
      assertEquals("[[8], [9]]",
        sqlContext.sql(s"""SELECT c6_int FROM $table where c12_varchar is NULL""").collectAsList().toString)

      assertEquals("[]",
        sqlContext.sql(s"""SELECT c6_int FROM $table where c6_int is NULL""").collectAsList().toString)
    }

  test("Test SparkSQL IS NOT NULL predicate") {
    assertEquals("[[0], [1], [2], [3], [4], [5], [6], [7]]",
      sqlContext.sql(s"""SELECT c6_int FROM $table where c12_varchar is NOT NULL""").collectAsList().toString)

    assertEquals("[[0], [1], [2], [3], [4], [5], [6], [7], [8], [9]]",
      sqlContext.sql(s"""SELECT c6_int FROM $table where c6_int is NOT NULL""").collectAsList().toString)
  }

  test("export") {
    val tmpDir: String = System.getProperty("java.io.tmpdir");
    val outDirectory: Path = Files.createTempDirectory("export")
    val df = sqlContext.read.options(internalOptions).splicemachine
    splicemachineContext.export(df, outDirectory.toString, false, 1, null, null, null)
    val newDF = sqlContext.read.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").csv(outDirectory.toString)
    assert(newDF.count == 10)
  }

  def createBadDirectory(directoryName: String): File = {
    val tmpDir: String = System.getProperty("java.io.tmpdir");
    val bulkImportDirectory: File = new File(tmpDir, directoryName)
    bulkImportDirectory.mkdirs()
    val statusDirectory: File = new File(bulkImportDirectory, "BAD")
    if (statusDirectory.exists())
      FileUtils.deleteDirectory(statusDirectory)
    statusDirectory.mkdir()
    statusDirectory
  }

}
