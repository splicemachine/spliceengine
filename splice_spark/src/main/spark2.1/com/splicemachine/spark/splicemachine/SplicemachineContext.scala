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

import java.security.{PrivilegedExceptionAction, SecureRandom}
import java.sql.Connection

import com.splicemachine.EngineDriver
import com.splicemachine.client.SpliceClient
import com.splicemachine.db.impl.jdbc.EmbedConnection
import com.splicemachine.derby.impl.SpliceSpark
import com.splicemachine.derby.stream.spark.SparkUtils
import com.splicemachine.derby.vti.SpliceDatasetVTI
import com.splicemachine.derby.vti.SpliceRDDVTI
import com.splicemachine.tools.EmbedConnectionMaker
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import java.util.Properties

import com.splicemachine.access.HConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}

object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
class SplicemachineContext(options: Map[String, String]) extends Serializable {
  val url = options.get(JDBCOptions.JDBC_URL).get

  def this(url: String) {
    this(Map(JDBCOptions.JDBC_URL -> url));
  }

  @transient var credentials = UserGroupInformation.getCurrentUser().getCredentials()
  JdbcDialects.registerDialect(new SplicemachineDialect)

  private[this] def initConnection() = {
    Holder.log.info(f"Creating internal connection")

    SpliceSpark.setupSpliceStaticComponents()
    val engineDriver = EngineDriver.driver
    assert(engineDriver != null, "Not booted yet!")
    // Create a static statement context to enable nested connections
    val maker = new EmbedConnectionMaker
    val dbProperties = new Properties
    dbProperties.put("useSpark", "true")
    dbProperties.put(EmbedConnection.INTERNAL_CONNECTION, "true")
    maker.createNew(dbProperties)
  }

  def getConnection(): Connection = {
    internalConnection
  }

  @transient val internalConnection : Connection = {
    Holder.log.debug("Splice Client in SplicemachineContext "+SpliceClient.isClient())
    SpliceClient.connectionString = url
    SpliceClient.setClient(HConfiguration.getConfiguration.getAuthenticationTokenEnabled, SpliceClient.Mode.MASTER)

    val principal = System.getProperty("spark.yarn.principal")
    val keytab = System.getProperty("spark.yarn.keytab")

    if (principal != null && keytab != null) {
      Holder.log.info(f"Authenticating as ${principal} with keytab ${keytab}")

      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
      UserGroupInformation.setLoginUser(ugi)

      ugi.doAs(new PrivilegedExceptionAction[Connection] {
        override def run(): Connection = {

          val connection = initConnection()
          connection
        }
      })
    } else {
      Holder.log.info(f"Authentication disabled, principal=${principal}; keytab=${keytab}")
      initConnection()
    }
  }

  def tableExists(schemaTableName: String): Boolean = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      JdbcUtils.tableExists(conn, jdbcOptions.url, jdbcOptions.table)
    } finally {
      conn.close()
    }
  }

  def tableExists(schemaName: String, tableName: String): Boolean = {
    tableExists(schemaName + "." + tableName)
  }

  def dropTable(schemaName: String, tableName: String): Unit = {
    dropTable(schemaName + "." + tableName)
  }


  def dropTable(schemaTableName: String): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      JdbcUtils.dropTable(conn, jdbcOptions.table)
    } finally {
      conn.close()
    }
  }

  def createTable(tableName: String,
                  structType: StructType,
                  keys: Seq[String],
                  createTableOptions: String): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> tableName)
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      val schemaString = JdbcUtils.schemaString(structType, jdbcOptions.url)
      val keyArray = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
      val primaryKeyString = new StringBuilder()
      val dialect = JdbcDialects.get(jdbcOptions.url)
      keyArray foreach { field =>
        val name = dialect.quoteIdentifier(field)
        primaryKeyString.append(s", $name")
      }
      val sql = s"CREATE TABLE $tableName ($schemaString) $primaryKeyString"
      val statement = conn.createStatement
      statement.executeUpdate(sql)
    } finally {
      conn.close()
    }
  }

  def executeUpdate(sql: String): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> "dismiss"
    )
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      val statement = conn.createStatement
      statement.executeUpdate(sql)
    } finally {
      conn.close()
    }
  }

  def execute(sql: String): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> "dismiss"
    )
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      val statement = conn.createStatement
      statement.execute(sql)
    } finally {
      conn.close()
    }
  }

  def truncateTable(tableName: String): Unit = {
    executeUpdate(s"TRUNCATE TABLE $tableName")
  }

  def analyzeSchema(schemaName: String): Unit = {
    execute(s"ANALYZE SCHEMA $schemaName")
  }

  def analyzeTable(tableName: String, estimateStatistics: Boolean = false, samplePercent: Double = 0.10 ): Unit = {
    if (!estimateStatistics)
      execute(s"ANALYZE TABLE $tableName")
    else
      execute(s"ANALYZE TABLE $tableName ESTIMATE STATISTICS SAMPLE $samplePercent PERCENT")
  }

  lazy val tempDirectory = {
    val root = options.getOrElse(SpliceJDBCOptions.JDBC_TEMP_DIRECTORY, "/tmp")
    HConfiguration.getConfiguration.authenticationNativeCreateCredentialsDatabase()
    val fs = FileSystem.get(HConfiguration.unwrapDelegate())
    val path = new Path(root, getRandomName())
    fs.mkdirs(path, FsPermission.createImmutable(Integer.parseInt("711", 8).toShort))

    SpliceSpark.getSession.sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        Holder.log.info("Removing " + path)
        fs.delete(path, true)
      }
    })
    path.toString
  }

  def getRandomName(): String = {
    val name = new Array[Byte](32)
    new SecureRandom().nextBytes(name)
    Bytes.toHex(name)+"-"+System.nanoTime()
  }

  /**
    * SQL to Dataframe translation.  (Lazy)
    *
    * @param sql
    * @return
    */
  def df(sql: String): Dataset[Row] = {
    SparkUtils.resultSetToDF(internalConnection.createStatement().executeQuery(sql));
  }

  /**
    * SQL to Dataframe translation.  (Lazy)
    * Runs the query inside Splice Machine and sends the results to the Spark Adapter app
    *
    * @param sql
    * @return
    */
  def internalDf(sql: String): Dataset[Row] = {
    val configuration = HConfiguration.getConfiguration
    val connection = SpliceClient.getConnectionPool(
      configuration.getAuthenticationTokenDebugConnections, configuration.getAuthenticationTokenMaxConnections).getConnection
    try {
      val id = getRandomName()

      val fs = FileSystem.get(HConfiguration.unwrapDelegate())
      fs.mkdirs(new Path(tempDirectory, id), FsPermission.createImmutable(Integer.parseInt("777", 8).toShort))
      fs.setPermission(new Path(tempDirectory, id), FsPermission.createImmutable(Integer.parseInt("777", 8).toShort))

      val schema = resolveQuery(connection, sql, false)

      connection.prepareStatement(s"EXPORT_BINARY('$tempDirectory/$id', false, 'parquet') " + sql).execute()
      // spark-2.2.0: commons-lang3-3.3.2 does not support 'XXX' timezone, specify 'ZZ' instead
      var df = SpliceSpark.getSession.read.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").schema(schema).parquet(fs.getUri + s"$tempDirectory/$id")
      df
    } finally {
      connection.close()
    }
  }

  def internalRdd(schemaTableName: String,
          columnProjection: Seq[String] = Nil): RDD[Row] = {
    val columnList = SpliceJDBCUtil.listColumns(columnProjection.toArray)
    val sqlText = s"SELECT $columnList FROM ${schemaTableName}"
    internalDf(sqlText).rdd
  }

  def rdd(schemaTableName: String,
          columnProjection: Seq[String] = Nil): RDD[Row] = {
    val columnList = SpliceJDBCUtil.listColumns(columnProjection.toArray)
    val sqlText = s"SELECT $columnList FROM ${schemaTableName}"
    df(sqlText).rdd
  }

  def insert(dataFrame: DataFrame, schemaTableName: String): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  /**
    *
    * Sample the dataframe, split the table, and insert a dataFrame into a table (schema.table).  This corresponds to an
    *
    * insert into from select statement
    *
    * @param dataFrame
    * @param schemaTableName
    * @param sampleFraction
    */
  def splitAndInsert(dataFrame: DataFrame, schemaTableName: String, sampleFraction: Double): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "insert into " + schemaTableName +
      " (" + columnList + ") --splice-properties useSpark=true, sampleFraction=" +
      sampleFraction + "\n select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
    SpliceRDDVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(rdd))
    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
      "as SpliceRDDVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  /**
    *
    * Insert a dataFrame into a table (schema.table).  This corresponds to an
    *
    * insert into from select statement
    *
    * @param dataFrame
    * @param schemaTableName
    */
  def insert(dataFrame: DataFrame, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ")" +
      " --splice-properties useSpark=true, insertMode=INSERT, statusDirectory=" + statusDirectory + ", badRecordsAllowed=" + badRecordsAllowed + "\n " +
      "select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  /**
    * Insert a RDD into a table (schema.table).  The schema is required since RDD's do not have schema.
    *
    * @param rdd
    * @param schema
    * @param schemaTableName
    */
  def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Unit = {
    SpliceRDDVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(rdd))
    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ")" +
      " --splice-properties useSpark=true, insertMode=INSERT, statusDirectory=" + statusDirectory + ", badRecordsAllowed=" + badRecordsAllowed + "\n " +
      " select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
      "as SpliceRDDVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  def upsert(dataFrame: DataFrame, schemaTableName: String): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") --splice-properties insertMode=UPSERT\n select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  def upsert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
    SpliceRDDVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(rdd))
    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") --splice-properties insertMode=UPSERT\n select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
      "as SpliceRDDVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  def delete(dataFrame: DataFrame, schemaTableName: String): Unit = {
    val jdbcOptions = new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
    if (keys.length == 0)
      throw new UnsupportedOperationException("Primary Key Required for the Table to Perform Deletes")
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "delete from " + schemaTableName + " where exists (select 1 from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SDVTI (" + schemaString + ") where "
    val dialect = JdbcDialects.get(url)
    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) +
      " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
    val combinedText = sqlText + whereClause + ")"
    internalConnection.createStatement().executeUpdate(combinedText)
  }

  def delete(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
    val jdbcOptions = new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceRDDVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(rdd))
    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
    if (keys.length == 0)
      throw new UnsupportedOperationException("Primary Key Required for the Table to Perform Deletes")
    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
    val sqlText = "delete from " + schemaTableName + " where exists (select 1 from " +
      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
      "as SDVTI (" + schemaString + ") where "
    val dialect = JdbcDialects.get(url)
    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) +
      " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
    val combinedText = sqlText + whereClause + ")"
    internalConnection.createStatement().executeUpdate(combinedText)
  }

  def update(dataFrame: DataFrame, schemaTableName: String): Unit = {
    val jdbcOptions = new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
    if (keys == 0)
      throw new UnsupportedOperationException("Primary Key Required for the Table to Perform Updates")
    val prunedFields = dataFrame.schema.fieldNames.filter((p: String) => keys.indexOf(p) == -1)
    val columnList = SpliceJDBCUtil.listColumns(prunedFields)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "update " + schemaTableName + " " +
      "set (" + columnList + ") = (" +
      "select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SDVTI (" + schemaString + ") where "
    val dialect = JdbcDialects.get(url)
    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) +
      " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
    val combinedText = sqlText + whereClause + ")"
    internalConnection.createStatement().executeUpdate(combinedText)
  }

  def update(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
    val jdbcOptions = new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceRDDVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(rdd))
    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
    if (keys.length == 0)
      throw new UnsupportedOperationException("Primary Key Required for the Table to Perform Updates")
    val prunedFields = schema.fieldNames.filter((p: String) => keys.indexOf(p) == -1)
    val columnList = SpliceJDBCUtil.listColumns(prunedFields)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
    val sqlText = "update " + schemaTableName + " " +
      "set (" + columnList + ") = (" +
      "select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
      "as SDVTI (" + schemaString + ") where "
    val dialect = JdbcDialects.get(url)
    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) +
      " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
    val combinedText = sqlText + whereClause + ")"
    internalConnection.createStatement().executeUpdate(combinedText)
  }

  def bulkImportHFile(dataFrame: DataFrame, schemaTableName: String,
                      options: scala.collection.mutable.Map[String, String]): Unit = {

    val bulkImportDirectory = options.get("bulkImportDirectory")
    if (bulkImportDirectory == null) {
      throw new IllegalArgumentException("bulkImportDirectory cannot be null")
    }
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    var properties = "--SPLICE-PROPERTIES "
    options foreach (option => properties += option._1 + "=" + option._2 + ",")
    properties = properties.substring(0, properties.length - 1)

    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") " + properties + "\n" +
      "select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  def bulkImportHFile(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String,
                      options: scala.collection.mutable.Map[String, String]): Unit = {

    val bulkImportDirectory = options.get("bulkImportDirectory")
    if (bulkImportDirectory == null) {
      throw new IllegalArgumentException("bulkImportDirectory cannot be null")
    }
    SpliceRDDVTI.datasetThreadLocal.set(rdd)
    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
    var properties = "--SPLICE-PROPERTIES "
    options foreach (option => properties += option._1 + "=" + option._2 + ",")
    properties = properties.substring(0, properties.length - 1)

    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") " + properties + "\n" +
      "select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
      "as SpliceRDDVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  def getSchema(schemaTableName: String): StructType = {
    val newSpliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    JDBCRDD.resolveTable(new JDBCOptions(newSpliceOptions))
  }

  val dialect = new SplicemachineDialect
  val dialectNoTime = new SplicemachineDialectNoTime
  def resolveQuery(connection: Connection, sql: String, noTime: Boolean): StructType = {
    try {
      val rs = connection.prepareStatement(s"select * from ($sql) a where 1=0 ").executeQuery()

      try {
        if (noTime)
          JdbcUtils.getSchema(rs, dialectNoTime)
        else
          JdbcUtils.getSchema(rs, dialect)
      } finally {
        rs.close()
      }
    }
  }

  /**
  * Export a dataFrame in binary format
  *
  * @param location  - Destination directory
  * @param compression - Whether to compress the output or not
  * @param format - Binary format to be used, currently only 'parquet' is supported
  */
  def exportBinary(dataFrame: DataFrame, location: String,
                   compression: Boolean, format: String): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = s"export_binary ( '$location', $compression, '$format') select " + columnList + " from " +
       s"new com.splicemachine.derby.vti.SpliceDatasetVTI() as SpliceDatasetVTI ($schemaString)"
    internalConnection.createStatement().execute(sqlText)
  }

  /**
    * Prune all but the specified columns from the specified Catalyst schema.
    *
    * @param schema  - The Catalyst schema of the master table
    * @param columns - The list of desired columns
    * @return A Catalyst schema corresponding to columns in the given order.
    */
  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.metadata.getString("name") -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }
  
  /**
    * Export a dataFrame in CSV
    *
    * @param location  - Destination directory
    * @param compression - Whether to compress the output or not
    * @param replicationCount - Replication used for HDFS write
    * @param fileEncoding - fileEncoding or null, defaults to UTF-8
    * @param fieldSeparator - fieldSeparator or null, defaults to ','
    * @param quoteCharacter - quoteCharacter or null, defaults to '"'
    *
    */
  def export(dataFrame: DataFrame, location: String,
             compression: Boolean, replicationCount: Int,
             fileEncoding: String,
             fieldSeparator: String,
             quoteCharacter: String): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val encoding = quotedOrNull(fileEncoding)
    val separator = quotedOrNull(fieldSeparator)
    val quoteChar = quotedOrNull(quoteCharacter)
    val sqlText = s"export ( '$location', $compression, $replicationCount, $encoding, $separator, $quoteChar) select " + columnList + " from " +
      s"new com.splicemachine.derby.vti.SpliceDatasetVTI() as SpliceDatasetVTI ($schemaString)"
    internalConnection.createStatement().execute(sqlText)
  }

  private[this] def quotedOrNull(value: String) = {
    if (value == null) "null" else s"'$value"
  }

}
