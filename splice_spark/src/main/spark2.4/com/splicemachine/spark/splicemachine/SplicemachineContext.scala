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
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import java.util.Properties

import com.splicemachine.access.HConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

import scala.collection.JavaConverters._

@SerialVersionUID(20200513241L)
private object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
/**
  *
  * Context for Splice Machine.
  *
  * @param options Supported options are SpliceJDBCOptions.JDBC_URL, SpliceJDBCOptions.JDBC_INTERNAL_QUERIES, SpliceJDBCOptions.JDBC_TEMP_DIRECTORY
  */
@SerialVersionUID(20200513242L)
@SuppressFBWarnings(value = Array("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"), justification = "Need to set SpliceClient.connectionString")
@SuppressFBWarnings(value = Array("NP_ALWAYS_NULL"), justification = "These fields usually are not null")
@SuppressFBWarnings(value = Array("EI_EXPOSE_REP2"), justification = "The prunedFields value is needed and is used read-only")
class SplicemachineContext(options: Map[String, String]) extends Serializable {
  private[this] val url = options.get(JDBCOptions.JDBC_URL).get
  
  /**
    *
    * Context for Splice Machine, specifying only the JDBC url.
    *
    * @param url JDBC Url with authentication parameters
    */
  def this(url: String) {
    this(Map(JDBCOptions.JDBC_URL -> url));
  }
  
  // Check url validity, throws exception during instantiation if url is invalid
  try {
    if( url.isEmpty ) throw new Exception("JDBC Url is an empty string.")
    JdbcUtils.createConnectionFactory(new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> "placeholder"
    )))()
  } catch {
    case e: Exception => throw new Exception(
      "Problem connecting to the DB. Verify that the input JDBC Url is correct."
      + "\n"
      + e.toString
    )
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
    maker.createNew(
      if( url.contains("/") ) {  // url = jdbc:splice://localhost:1527/splicedb;user=userid;password=pwd
        val urlparts = url.split("/")
        urlparts(0) + urlparts(urlparts.length - 1)  // jdbc:splice:splicedb;user=userid;password=pwd
      } else
        url ,
      dbProperties
    )
  }

  /**
    * Get internal JDBC connection
    */
  def getConnection(): Connection = {
    internalConnection
  }

  @transient private[this]val internalConnection : Connection = {
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

  /**
    *
    * Generate the schema string for create table.
    *
    * @param schema
    * @param url
    * @return
    */
  def schemaString(schema: StructType, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    schema.fields foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ: String = getJdbcType(field.dataType, dialect).databaseTypeDefinition
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
    *
    * Retrieve the JDBC type based on the Spark DataType and JDBC Dialect.
    *
    * @param dt
    * @param dialect
    * @return
    */
  private[this]def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  /**
    * Retrieve standard jdbc types.
    *
    * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
    * @return The default JdbcType for this DataType
    */
  private[this] def getCommonJDBCType(dt: DataType) = {
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(
        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  /**
    *
    * Determine whether a table exists (uses JDBC).
    *
    * @param schemaTableName
    * @return true if the table exists, false otherwise
    */
  def tableExists(schemaTableName: String): Boolean =
    SpliceJDBCUtil.retrieveTableInfo(
      new JdbcOptionsInWrite( Map(
        JDBCOptions.JDBC_URL -> url,
        JDBCOptions.JDBC_TABLE_NAME -> schemaTableName
      ))
    ).nonEmpty

  /**
    * Determine whether a table exists given the schema name and table name.
    *
    * @param schemaName
    * @param tableName
    * @return true if the table exists, false otherwise
    */
  def tableExists(schemaName: String, tableName: String): Boolean = {
    tableExists(schemaName + "." + tableName)
  }

  /**
    *
    * Drop a table based on the schema name and table name.
    *
    * @param schemaName
    * @param tableName
    */
  def dropTable(schemaName: String, tableName: String): Unit = {
    dropTable(schemaName + "." + tableName)
  }

  /**
    *
    * Drop a table based on the schemaTableName (schema.table)
    *
    * @param schemaTableName
    */
  def dropTable(schemaTableName: String): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val JdbcOptionsInWrite = new JdbcOptionsInWrite(spliceOptions)
    val table = JdbcOptionsInWrite.table
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      JdbcUtils.dropTable(conn, table, jdbcOptions)
    } finally {
      conn.close()
    }
  }

  /**
    *
    * Create Table based on the table name, the schema, primary keys, and createTableOptions.
    *
    * @param schemaTableName tablename, or schema.tablename
    * @param structType Schema of the table
    * @param keys Names of columns to make up the primary key
    * @param createTableOptions Not yet implemented
    */
  def createTable(schemaTableName: String,
                  structType: StructType,
                  keys: Seq[String] = Seq(),
                  createTableOptions: String = ""): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    val jdbcOptions = new JdbcOptionsInWrite(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    val statement = conn.createStatement
    try {
      val actSchemaString = schemaString(structType, jdbcOptions.url)

      val primaryKeyString = if( keys.isEmpty ) {""}
      else {
        ", PRIMARY KEY(" + keys.map(dialect.quoteIdentifier(_)).mkString(", ") + ")"
      }

      val sql = s"CREATE TABLE $schemaTableName ($actSchemaString$primaryKeyString)"
      statement.executeUpdate(sql)
    } finally {
      statement.close()
      conn.close()
    }
  }

  /**
    *
    * Execute an update statement via JDBC against Splice Machine
    *
    * @param sql
    */
  def executeUpdate(sql: String): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> "dismiss")
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
      conn.close()
    }
  }

  /**
    *
    * Execute SQL against Splice Machine via JDBC.
    *
    * @param sql
    */
  def execute(sql: String): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> "dismiss"
      )
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    val statement = conn.createStatement
    try {
      statement.execute(sql)
    } finally {
      statement.close()
      conn.close()
    }
  }

  /**
    *
    * Truncate the table supplied.  The tableName should be in the form schema.table
    *
    * @param tableName
    */
  def truncateTable(tableName: String): Unit = {
    executeUpdate(s"TRUNCATE TABLE $tableName")
  }

  /**
    *
    * Analyze the scheme supplied via JDBC
    *
    * @param schemaName
    */
  def analyzeSchema(schemaName: String): Unit = {
    execute(s"ANALYZE SCHEMA $schemaName")
  }

  /**
    *
    * Analyze table provided.  Will use estimate statistics if provided.
    *
    * @param tableName
    * @param estimateStatistics
    * @param samplePercent
    */
  def analyzeTable(tableName: String, estimateStatistics: Boolean = false, samplePercent: Double = 10.0 ): Unit = {
    if (!estimateStatistics)
      execute(s"ANALYZE TABLE $tableName")
    else
      execute(s"ANALYZE TABLE $tableName ESTIMATE STATISTICS SAMPLE $samplePercent PERCENT")
  }

  lazy private[this]val tempDirectory = {
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

  private[this] def getRandomName(): String = {
    val name = new Array[Byte](32)
    new SecureRandom().nextBytes(name)
    Bytes.toHex(name)+"-"+System.nanoTime()
  }

  /**
    * SQL to Dataset translation.  (Lazy)
    *
    * @param sql SQL query
    * @return Dataset[Row] with the result of the query
    */
  def df(sql: String): Dataset[Row] = {
    val st = internalConnection.createStatement()
    try {
      SparkUtils.resultSetToDF(st.executeQuery(sql));
    } finally {
      st.close()
    }
  }

  /**
    * SQL to Dataset translation.  (Lazy)
    * Runs the query inside Splice Machine and sends the results to the Spark Adapter app
    *
    * @param sql SQL query
    * @return Dataset[Row] with the result of the query
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

      val st = connection.prepareStatement(s"EXPORT_BINARY('$tempDirectory/$id', false, 'parquet') " + sql)
      try {
        st.execute()
      } finally {
        st.close()
      }
      // spark-2.2.0: commons-lang3-3.3.2 does not support 'XXX' timezone, specify 'ZZ' instead
      var df = SpliceSpark.getSession.read.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").schema(schema).parquet(fs.getUri + s"$tempDirectory/$id")
      df
    } finally {
      connection.close()
    }
  }

  /**
    *
    * Table with projections in Splice mapped to an RDD.
    * Runs the query inside Splice Machine and sends the results to the Spark Adapter app
    *
    * @param schemaTableName Accessed table
    * @param columnProjection Selected columns
    * @return RDD[Row] with the result of the projection
    */
  def internalRdd(schemaTableName: String,
                  columnProjection: Seq[String] = Nil): RDD[Row] = {
    val columnList = SpliceJDBCUtil.listColumns(columnProjection.toArray)
    val sqlText = s"SELECT $columnList FROM ${schemaTableName}"
    internalDf(sqlText).rdd
  }

  /**
    *
    * Table with projections in Splice mapped to an RDD.
    *
    * @param schemaTableName Accessed table
    * @param columnProjection Selected columns
    * @return RDD[Row] with the result of the projection
    */
  def rdd(schemaTableName: String,
          columnProjection: Seq[String] = Nil): RDD[Row] = {
    val columnList = SpliceJDBCUtil.listColumns(columnProjection.toArray)
    val sqlText = s"SELECT $columnList FROM ${schemaTableName}"
    df(sqlText).rdd
  }
  
  private[this] def executeUpd(sql: String): Unit = {
    val st = internalConnection.createStatement()
    try {
      st.executeUpdate(sql)
    } finally {
      st.close()
    }
  }

  /**
    *
    * Insert a dataFrame into a table (schema.table).  This corresponds to an
    *
    * insert into from select statement
    *
    * @param dataFrame input data
    * @param schemaTableName output table
    */
  def insert(dataFrame: DataFrame, schemaTableName: String): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    executeUpd(sqlText)
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
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "insert into " + schemaTableName +
      " (" + columnList + ") --splice-properties useSpark=true, sampleFraction=" +
      sampleFraction + "\n select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    executeUpd(sqlText)
  }


  /**
    * Insert a RDD into a table (schema.table).  The schema is required since RDD's do not have schema.
    *
    * @param rdd input data
    * @param schema
    * @param schemaTableName
    */
  def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
    SpliceRDDVTI.datasetThreadLocal.set(rdd)
    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
      "as SpliceRDDVTI (" + schemaString + ")"
    executeUpd(sqlText)
  }


  /**
    *
    * Insert a dataFrame into a table (schema.table).  This corresponds to an
    *
    * insert into from select statement
    *
    * The status directory and number of badRecordsAllowed allows for duplicate primary keys to be
    * written to a bad records file.  If badRecordsAllowed is set to -1, all bad records will be written
    * to the status directory.
    *
    * @param dataFrame input data
    * @param schemaTableName
    * @param statusDirectory status directory where bad records file will be created
    * @param badRecordsAllowed how many bad records are allowed. -1 for unlimited
    */
  def insert(dataFrame: DataFrame, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ")" +
    " --splice-properties useSpark=true, insertMode=INSERT, statusDirectory=" + statusDirectory + ", badRecordsAllowed=" + badRecordsAllowed + "\n " +
    "select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    executeUpd(sqlText)
  }

  /**
    * Insert a RDD into a table (schema.table).  The schema is required since RDD's do not have schema.
    *
    * The status directory and number of badRecordsAllowed allows for duplicate primary keys to be
    * written to a bad records file.  If badRecordsAllowed is set to -1, all bad records will be written
    * to the status directory.
    *
    * @param rdd input data
    * @param schema
    * @param schemaTableName
    * @param statusDirectory status directory where bad records file will be created
    * @param badRecordsAllowed how many bad records are allowed. -1 for unlimited
    *
    */
  def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Unit = {
    SpliceRDDVTI.datasetThreadLocal.set(rdd)
    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ")" +
      " --splice-properties useSpark=true, insertMode=INSERT, statusDirectory=" + statusDirectory + ", badRecordsAllowed=" + badRecordsAllowed + "\n " +
    " select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
      "as SpliceRDDVTI (" + schemaString + ")"
    executeUpd(sqlText)
  }

  /**
    * Upsert data into the table (schema.table) from a DataFrame.  This will insert the data if the record is not found by primary key and if it is it will change
    * the columns that are different between the two records.
    *
    * @param dataFrame input data
    * @param schemaTableName output table
    */
  def upsert(dataFrame: DataFrame, schemaTableName: String): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") --splice-properties insertMode=UPSERT\n select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    executeUpd(sqlText)
  }

  /**
    * Upsert data into the table (schema.table) from an RDD.  This will insert the data if the record is not found by primary key and if it is it will change
    * the columns that are different between the two records.
    *
    * @param rdd input data
    * @param schema
    * @param schemaTableName
    */
  def upsert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
    SpliceRDDVTI.datasetThreadLocal.set(rdd)
    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") --splice-properties insertMode=UPSERT\n select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
      "as SpliceRDDVTI (" + schemaString + ")"
    executeUpd(sqlText)
  }

  /**
    * Delete records in a dataframe based on joining by primary keys from the data frame.  Be careful with column naming and case sensitivity.
    *
    * @param dataFrame rows to delete
    * @param schemaTableName table to delete from
    */
  def delete(dataFrame: DataFrame, schemaTableName: String): Unit = {
    val jdbcOptions = new JdbcOptionsInWrite(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
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
    executeUpd(combinedText)
  }

  /**
    * Delete records in a dataframe based on joining by primary keys from the data frame.  Be careful with column naming and case sensitivity.
    *
    * @param rdd rows to delete
    * @param schema
    * @param schemaTableName table to delete from
    */
  def delete(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
    val jdbcOptions = new JdbcOptionsInWrite(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceRDDVTI.datasetThreadLocal.set(rdd)
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
    executeUpd(combinedText)
  }

  /**
    * Update data from a dataframe for a specified schemaTableName (schema.table).  The keys are required for the update and any other
    * columns provided will be updated in the rows.
    *
    * @param dataFrame rows for update
    * @param schemaTableName table to update
    */
  def update(dataFrame: DataFrame, schemaTableName: String): Unit = {
    val jdbcOptions = new JdbcOptionsInWrite(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
    if (keys.length == 0)
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
    executeUpd(combinedText)
  }

  /**
    * Update data from a RDD for a specified schemaTableName (schema.table) and schema (StructType).  The keys are required for the update and any other
    * columns provided will be updated in the rows.
    *
    * @param rdd rows for update
    * @param schema
    * @param schemaTableName
    */
  def update(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
    val jdbcOptions = new JdbcOptionsInWrite(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceRDDVTI.datasetThreadLocal.set(rdd)
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
    executeUpd(combinedText)
  }

  /**
   * Bulk Import HFile from a dataframe into a schemaTableName(schema.table)
   *
   * @param dataFrame input data
   * @param schemaTableName
   * @param options options to be passed to --splice-properties; bulkImportDirectory is required
   */
  def bulkImportHFile(dataFrame: DataFrame, schemaTableName: String,
                      options: java.util.Map[String, String]): Unit =
    bulkImportHFile(dataFrame, schemaTableName, options.asScala)

  /**
    * Bulk Import HFile from a dataframe into a schemaTableName(schema.table)
    *
    * @param dataFrame input data
    * @param schemaTableName
    * @param options options to be passed to --splice-properties; bulkImportDirectory is required
    */
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
    executeUpd(sqlText)
  }

  /**
    * Bulk Import HFile from a RDD into a schemaTableName(schema.table)
    *
    * @param rdd input data
    * @param schemaTableName
    * @param options options to be passed to --splice-properties; bulkImportDirectory is required
    */
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
    executeUpd(sqlText)
  }

  /**
    * Return a table's schema via JDBC.
    *
    * @param schemaTableName table
    * @return table's schema
    */
  def getSchema(schemaTableName: String): StructType = {
    val newSpliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    JDBCRDD.resolveTable(new JDBCOptions(newSpliceOptions))
  }

  private[this]val dialect = new SplicemachineDialect
  private[this]val dialectNoTime = new SplicemachineDialectNoTime
  private[this]def resolveQuery(connection: Connection, sql: String, noTime: Boolean): StructType = {
    val st = connection.prepareStatement(s"select * from ($sql) a where 1=0 ")
    try {
      val rs = st.executeQuery()

      try {
        if (noTime)
          JdbcUtils.getSchema(rs, dialectNoTime)
        else
          JdbcUtils.getSchema(rs, dialect)
      } finally {
        rs.close()
      }
    } finally {
      st.close()
    }
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

  private[this] def executeSql(sql: String): Unit = {
    val st = internalConnection.createStatement()
    try {
      st.execute(sql)
    } finally {
      st.close()
    }
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
    executeSql(sqlText)
  }

  private[this] def quotedOrNull(value: String) = {
    if (value == null) "null" else s"'$value"
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
    executeSql(sqlText)
  }
}
