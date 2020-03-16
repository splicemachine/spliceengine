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
package com.splicemachine.spark2.splicemachine

import java.io.Externalizable
import java.security.{PrivilegedExceptionAction, SecureRandom}
import java.sql.Connection

import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import java.util.Properties

import com.splicemachine.db.impl.jdbc.EmbedConnection
import com.splicemachine.spark.splicemachine.ShuffleUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

//import com.splicemachine.derby.vti.SpliceDatasetVTI
//import com.splicemachine.derby.vti.SpliceRDDVTI

import com.splicemachine.primitives.Bytes

import java.security.{PrivilegedExceptionAction, SecureRandom}
import java.sql.Connection

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import java.util.Properties

import com.splicemachine.access.HConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.Token
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.IntegerSerializer
import com.splicemachine.derby.stream.spark.ExternalizableSerializer
import com.splicemachine.db.impl.sql.execute.ValueRow
import com.splicemachine.db.iapi.types.SQLInteger
import com.splicemachine.db.iapi.types.SQLLongint
import com.splicemachine.db.iapi.types.SQLDouble
import com.splicemachine.db.iapi.types.SQLReal
import com.splicemachine.db.iapi.types.SQLSmallint
import com.splicemachine.db.iapi.types.SQLTinyint
import com.splicemachine.db.iapi.types.SQLBoolean
import com.splicemachine.db.iapi.types.SQLClob
import com.splicemachine.db.iapi.types.SQLBlob
import com.splicemachine.db.iapi.types.SQLTimestamp
import com.splicemachine.db.iapi.types.SQLDate
import com.splicemachine.db.iapi.types.SQLDecimal

private object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

object KafkaOptions {
  val KAFKA_SERVERS = "KAFKA_SERVERS"
  val KAFKA_POLL_TIMEOUT = "KAFKA_POLL_TIMEOUT"
}

/**
  *
  * Context for Splice Machine.
  *
  * @param options Supported options are JDBCOptions.JDBC_URL (required), JDBCOptions.JDBC_INTERNAL_QUERIES,
  *                JDBCOptions.JDBC_TEMP_DIRECTORY, KafkaOptions.KAFKA_SERVERS, KafkaOptions.KAFKA_POLL_TIMEOUT
  */
class SplicemachineContext(options: Map[String, String]) extends Serializable {
  private[this] val url = options(JDBCOptions.JDBC_URL)

  private[this] val kafkaServers = options.getOrElse(KafkaOptions.KAFKA_SERVERS, "localhost:9092")
  println(s"Kafka: $kafkaServers")

  private[this] val kafkaPollTimeout = options.getOrElse(KafkaOptions.KAFKA_POLL_TIMEOUT, "20000").toLong

  /**
   *
   * Context for Splice Machine, specifying only the JDBC url.
   *
   * @param url JDBC Url with authentication parameters
   */
  def this(url: String) {
    this(Map(JDBCOptions.JDBC_URL -> url));
  }

  /**
   *
   * Context for Splice Machine.
   *
   * @param url JDBC Url with authentication parameters
   * @param kafkaServers Comma-separated list of Kafka broker addresses in the form host:port
   * @param kafkaPollTimeout Number of milliseconds to wait when polling Kafka
   */
  def this(url: String, kafkaServers: String = "localhost:9092", kafkaPollTimeout: Long = 20000) {
    this(Map(
      JDBCOptions.JDBC_URL -> url,
      KafkaOptions.KAFKA_SERVERS -> kafkaServers,
      KafkaOptions.KAFKA_POLL_TIMEOUT -> kafkaPollTimeout.toString
    ))
  }

  @transient var credentials = UserGroupInformation.getCurrentUser().getCredentials()
  JdbcDialects.registerDialect(new SplicemachineDialect2)
//
//  private[this] def initConnection() = {
//    Holder.log.info(f"Creating internal connection")
//
//    SpliceSpark.setupSpliceStaticComponents()
//    val engineDriver = EngineDriver.driver
//    assert(engineDriver != null, "Not booted yet!")
//    // Create a static statement context to enable nested connections
//    val maker = new EmbedConnectionMaker
//    val dbProperties = new Properties
//    dbProperties.put("useSpark", "true")
//    dbProperties.put(EmbedConnection.INTERNAL_CONNECTION, "true")
//    maker.createNew(dbProperties)
//  }

//  @transient private[this]val internalConnection : Connection = {
//    Holder.log.debug("Splice Client in SplicemachineContext "+SpliceClient.isClient())
//    SpliceClient.connectionString = url
//    SpliceClient.setClient(HConfiguration.getConfiguration.getAuthenticationTokenEnabled, SpliceClient.Mode.MASTER)
//
//    val principal = System.getProperty("spark.yarn.principal")
//    val keytab = System.getProperty("spark.yarn.keytab")
//
//    if (principal != null && keytab != null) {
//      Holder.log.info(f"Authenticating as ${principal} with keytab ${keytab}")
//
//      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
//      UserGroupInformation.setLoginUser(ugi)
//
//      ugi.doAs(new PrivilegedExceptionAction[Connection] {
//        override def run(): Connection = {
//
//          val connection = initConnection()
//          connection
//        }
//      })
//    } else {
//      Holder.log.info(f"Authentication disabled, principal=${principal}; keytab=${keytab}")
//      initConnection()
//    }
//  }

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
  def tableExists(schemaTableName: String): Boolean = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      JdbcUtils.tableExists(conn, jdbcOptions)
    } finally {
      conn.close()
    }
  }

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
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      JdbcUtils.dropTable(conn, jdbcOptions.table)
    } finally {
      conn.close()
    }
  }

  /**
    *
    * Create Table based on the table name, the struct (data types), key sequence and createTableOptions.
    *
    * We currently do not have any custom table options.  These could be added if needed.
    *
    * @param tableName
    * @param structType
    * @param keys
    * @param createTableOptions
    */
  def createTable(tableName: String,
                  structType: StructType,
                  keys: Seq[String],  // not being used
                  createTableOptions: String): Unit = {  // createTableOptions not being used
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> tableName)
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      val actSchemaString = schemaString(structType, jdbcOptions.url)
      val keyArray = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
      val primaryKeyString = new StringBuilder()
      val dialect = JdbcDialects.get(jdbcOptions.url)
      keyArray foreach { field =>
        val name = dialect.quoteIdentifier(field)
        primaryKeyString.append(s", $name")
      }
      val sql = s"CREATE TABLE $tableName ($actSchemaString) $primaryKeyString"
      val statement = conn.createStatement
      statement.executeUpdate(sql)
    } finally {
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
    try {
      val statement = conn.createStatement
      statement.executeUpdate(sql)
    } finally {
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
    try {
      val statement = conn.createStatement
      statement.execute(sql)
    } finally {
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
  def analyzeTable(tableName: String, estimateStatistics: Boolean = false, samplePercent: Double = 0.10 ): Unit = {
    if (!estimateStatistics)
      execute(s"ANALYZE TABLE $tableName")
    else
      execute(s"ANALYZE TABLE $tableName ESTIMATE STATISTICS SAMPLE $samplePercent PERCENT")
  }

  /**
    * SQL to Dataset translation.
    * Runs the query inside Splice Machine and sends the results to the Spark Adapter app through Kafka
    *
    * @param sql SQL query
    * @return Dataset[Row] with the result of the query
    */
  def df(sql: String): Dataset[Row] = {

//    val spliceOptions = Map(
//      JDBCOptions.JDBC_URL -> url,
//      JDBCOptions.JDBC_TABLE_NAME -> "placeholder"
//    )
//    val jdbcOptions = new JDBCOptions(spliceOptions)
//    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
//    val configuration = HConfiguration.getConfiguration
//
////    try {
//    val topicName = getRandomName() // Kafka topic
//
////    println( s"SMC.df topic $topicName" )
//
//    // hbase user has read/write permission on the topic
//
//    conn.prepareStatement(s"EXPORT_KAFKA('$topicName') " + sql + " --splice-properties useSpark=true").execute()

    val sqlMod = if( sql.trim.toUpperCase.startsWith("SELECT") ) {
      val spliceProps = " --splice-properties useSpark=true"
      if( sql.toUpperCase.contains("WHERE") ) {
        sql.toUpperCase.replace( "WHERE" , spliceProps+"\nWHERE")
      } else {
        sql + spliceProps
      }
    } else {
      sql
    }

    val kdf = new KafkaToDF(kafkaServers, kafkaPollTimeout)
    kdf.df( send(sqlMod) )
//    }
  }

  def internalDf(sql: String): Dataset[Row] = df(sql)

  private[this] def send(sql: String): String = {

    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> "placeholder"
    )
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    val configuration = HConfiguration.getConfiguration

    //    try {
    val topicName = getRandomName() // Kafka topic

    //    println( s"SMC.df topic $topicName" )

    // hbase user has read/write permission on the topic

//    println( s"SMC.send sql $sql" )
    conn.prepareStatement(s"EXPORT_KAFKA('$topicName') " + sql).execute()

    topicName
    //    }
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
  def rdd(schemaTableName: String,
                  columnProjection: Seq[String] = Nil): RDD[Row] = {
    val columnList = SpliceJDBCUtil.listColumns(columnProjection.toArray)
    val sqlText = s"SELECT $columnList FROM ${schemaTableName} --splice-properties useSpark=true"
    val kdf = new KafkaToDF(kafkaServers, kafkaPollTimeout)
    kdf.rdd( send(sqlText) )
  }

  def getRandomName(): String = {
    val name = new Array[Byte](32)
    new SecureRandom().nextBytes(name)
    Bytes.toHex(name)+"-"+System.nanoTime()
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
                  columnProjection: Seq[String] = Nil): RDD[Row] = rdd(schemaTableName, columnProjection)

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
  def insert(dataFrame: DataFrame, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Unit =
    insert(dataFrame.rdd, dataFrame.schema, schemaTableName, statusDirectory, badRecordsAllowed)

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
  def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Unit =
    insert(rdd, schema, schemaTableName, Map("insertMode"->"INSERT","statusDirectory"->statusDirectory,"badRecordsAllowed"->badRecordsAllowed.toString) )

  /**
    *
    * Insert a dataFrame into a table (schema.table).  This corresponds to an
    *
    * insert into from select statement
    *
    * @param dataFrame input data
    * @param schemaTableName output table
    */
  def insert(dataFrame: DataFrame, schemaTableName: String): Unit = insert(dataFrame.rdd, dataFrame.schema, schemaTableName)

  /**
   * Insert a RDD into a table (schema.table).  The schema is required since RDD's do not have schema.
   *
   * @param rdd input data
   * @param schema
   * @param schemaTableName
   */
  def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = insert(rdd, schema, schemaTableName, Map[String,String]())

  private[this] def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String, spliceProperties: scala.collection.immutable.Map[String,String]): Unit = {
    val topicName = getRandomName() //Kafka topic

    println( s"SMC.insert topic $topicName" )

    // hbase user has read/write permission on the topic

//    ShuffleUtils.shuffle(dataFrame).rdd.mapPartitionsWithIndex(
    rdd.rdd.mapPartitions(
//      new com.splicemachine.derby.stream.spark.KafkaStreamer[Row]("localhost", 9092, -1, topicName)
//      new KafkaStreamer[Row]("localhost", 9092, topicName)
//      (i,itrRow) => {
      itrRow => {
        println( s"SMC.insert mapPartitions" ) //WithIndex" )
        val props = new Properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-"+java.util.UUID.randomUUID() )
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ExternalizableSerializer].getName)

        val producer = new KafkaProducer[Integer, Externalizable](props)

        // java example
//        int count = 0 ;
//        while (locatedRowIterator.hasNext()) {
//          T lr = locatedRowIterator.next();
//
//          ProducerRecord<Integer, Externalizable> record = new ProducerRecord(topicName,
//            partition.intValue(), count++, lr);
//          producer.send(record);
//        }

        var count = 0
        itrRow.foreach( row => {
//          println( s"SMC.insert partition $i record $count" )
//          producer.send( new ProducerRecord(topicName, i, count, externalizable(row)) )
          println( s"SMC.insert record $count" )
          producer.send( new ProducerRecord(topicName, count, externalizable(row, schema)) )
          count += 1
        })

        producer.close

        java.util.Arrays.asList("OK").iterator().asScala
      }
    ).collect

    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url).replace("\"","")
    val sProps = spliceProperties.map({case (k,v) => k+"="+v}).fold("--splice-properties useSpark=true")(_+", "+_)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") "+sProps+"\nselect " + columnList + " from " +
      "new com.splicemachine.derby.vti.KafkaVTI('"+topicName+"') " +
      "as SpliceDatasetVTI (" + schemaString + ")"

    println( s"SMC.insert sql $sqlText" )
    executeUpdate(sqlText)
  }

  def externalizable(row: Row, schema: StructType): ValueRow = {
    val valRow = new ValueRow(row.length);
    for (i <- 1 to row.length) {
//      val fieldDef = row.schema(i-1)
      val fieldDef = schema(i-1)
      spliceType( fieldDef.dataType , row , i-1 ) match {
        case Some(splType) =>
          valRow.setColumn( i , splType )
        case None =>
          throw new IllegalArgumentException(s"Can't get Splice type for ${fieldDef.dataType.simpleString}")
      }
    }
    valRow
  }

  def spliceType(sparkType: DataType, row: Row, i: Int): Option[com.splicemachine.db.iapi.types.DataType] = {
    val isNull = row.isNullAt(i)
    sparkType match {  // the first several SQL types are set to null in the default constructor
      case IntegerType => Option( if (isNull) new SQLInteger else new SQLInteger(row.getInt(i)) )
      case LongType => Option( if (isNull) new SQLLongint else new SQLLongint(row.getLong(i)) )
      case DoubleType => Option( if (isNull) new SQLDouble else new SQLDouble(row.getDouble(i)) )
      case FloatType => Option( if (isNull) new SQLReal else new SQLReal(row.getFloat(i)) )
      case ShortType => Option( if (isNull) new SQLSmallint else new SQLSmallint(row.getShort(i)) )
      case ByteType => Option( if (isNull) new SQLTinyint else new SQLTinyint(row.getByte(i)) )
      case BooleanType => Option( if (isNull) new SQLBoolean else new SQLBoolean(row.getBoolean(i)) )
      case StringType => {
        val clob = new SQLClob
        clob setStreamHeaderFormat false
        if (isNull) clob.setToNull else clob.setValue( row.getString(i) )
        Option(clob)
      }
      case BinaryType => {
        val blob = new SQLBlob
        if (isNull) blob.setToNull else blob.setValue( row.getAs[Array[Byte]](i) )
        Option(blob)
      }
      case TimestampType => {
        val ts = new SQLTimestamp
        if (isNull) ts.setToNull else ts.setValue( row.getTimestamp(i) , null )
        Option(ts)
      }
      case DateType => {
        val dt = new SQLDate
        if (isNull) dt.setToNull else dt.setValue( row.getDate(i) , null )
        Option(dt)
      }
      case t: DecimalType => {
        val dc = new SQLDecimal
        if (isNull) dc.setToNull else dc.setValue( row.getDecimal(i) )
        Option(dc)
      }
      case _ => None
    }
  }

  /**
   * Bulk Import HFile from a dataframe into a schemaTableName(schema.table)
   *
   * @param dataFrame input data
   * @param schemaTableName
   * @param options options to be passed to --splice-properties; bulkImportDirectory is required
   */
  def bulkImportHFile(dataFrame: DataFrame, schemaTableName: String,
                      options: scala.collection.mutable.Map[String, String]): Unit =
    bulkImportHFile(dataFrame.rdd, dataFrame.schema, schemaTableName, options)

  /**
   * Bulk Import HFile from a RDD into a schemaTableName(schema.table)
   *
   * @param rdd input data
   * @param schemaTableName
   * @param options options to be passed to --splice-properties; bulkImportDirectory is required
   */
  def bulkImportHFile(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String,
                      options: scala.collection.mutable.Map[String, String]): Unit = {

    if( ! options.contains("bulkImportDirectory") ) {
      throw new IllegalArgumentException("bulkImportDirectory cannot be null")
    }

    insert(rdd, schema, schemaTableName, options.toMap)
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
  def splitAndInsert(dataFrame: DataFrame, schemaTableName: String, sampleFraction: Double): Unit =
    insert(dataFrame.rdd, dataFrame.schema, schemaTableName, Map("sampleFraction"->sampleFraction.toString))

  /**
   * Export a dataFrame to Kafka
   *
   * @param topic  - Kafka topic directory
   * @param compression - Whether to compress the output or not
   */
//  def exportKafka(dataFrame: DataFrame, topic: String): Unit = {
////    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
//    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
//    val sqlText = s"export_binary ( '$location', $compression, '$format') select " + columnList + " from " +
//      s"new com.splicemachine.derby.vti.SpliceDatasetVTI() as SpliceDatasetVTI ($schemaString)"
////    internalConnection.createStatement().execute(sqlText)
//  }

//  /**
//    *
//    * Sample the dataframe, split the table, and insert a dataFrame into a table (schema.table).  This corresponds to an
//    *
//    * insert into from select statement
//    *
//    * @param dataFrame
//    * @param schemaTableName
//    * @param sampleFraction
//    */
//  def splitAndInsert(dataFrame: DataFrame, schemaTableName: String, sampleFraction: Double): Unit = {
//    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
//    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
//    val sqlText = "insert into " + schemaTableName +
//      " (" + columnList + ") --splice-properties useSpark=true, sampleFraction=" +
//      sampleFraction + "\n select " + columnList + " from " +
//      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
//      "as SpliceDatasetVTI (" + schemaString + ")"
//    internalConnection.createStatement().executeUpdate(sqlText)
//  }
//
//
//  /**
//    *
//    * Insert a dataFrame into a table (schema.table).  This corresponds to an
//    *
//    * insert into from select statement
//    *
//    * The status directory and number of badRecordsAllowed allows for duplicate primary keys to be
//    * written to a bad records file.  If badRecordsAllowed is set to -1, all bad records will be written
//    * to the status directory.
//    *
//    * @param dataFrame input data
//    * @param schemaTableName
//    * @param statusDirectory status directory where bad records file will be created
//    * @param badRecordsAllowed how many bad records are allowed. -1 for unlimited
//    */
//  def insert(dataFrame: DataFrame, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Unit = {
//    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
//    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
//    val sqlText = "insert into " + schemaTableName + " (" + columnList + ")" +
//    " --splice-properties useSpark=true, insertMode=INSERT, statusDirectory=" + statusDirectory + ", badRecordsAllowed=" + badRecordsAllowed + "\n " +
//    "select " + columnList + " from " +
//      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
//      "as SpliceDatasetVTI (" + schemaString + ")"
//    internalConnection.createStatement().executeUpdate(sqlText)
//  }
//
//  /**
//    * Insert a RDD into a table (schema.table).  The schema is required since RDD's do not have schema.
//    *
//    * The status directory and number of badRecordsAllowed allows for duplicate primary keys to be
//    * written to a bad records file.  If badRecordsAllowed is set to -1, all bad records will be written
//    * to the status directory.
//    *
//    * @param rdd input data
//    * @param schema
//    * @param schemaTableName
//    * @param statusDirectory status directory where bad records file will be created
//    * @param badRecordsAllowed how many bad records are allowed. -1 for unlimited
//    *
//    */
//  def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Unit = {
//    SpliceRDDVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(rdd))
//    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
//    val sqlText = "insert into " + schemaTableName + " (" + columnList + ")" +
//      " --splice-properties useSpark=true, insertMode=INSERT, statusDirectory=" + statusDirectory + ", badRecordsAllowed=" + badRecordsAllowed + "\n " +
//    " select " + columnList + " from " +
//      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
//      "as SpliceRDDVTI (" + schemaString + ")"
//    internalConnection.createStatement().executeUpdate(sqlText)
//  }
//
//  /**
//    * Upsert data into the table (schema.table) from a DataFrame.  This will insert the data if the record is not found by primary key and if it is it will change
//    * the columns that are different between the two records.
//    *
//    * @param dataFrame input data
//    * @param schemaTableName output table
//    */
//  def upsert(dataFrame: DataFrame, schemaTableName: String): Unit = {
//    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
//    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
//    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") --splice-properties insertMode=UPSERT\n select " + columnList + " from " +
//      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
//      "as SpliceDatasetVTI (" + schemaString + ")"
//    internalConnection.createStatement().executeUpdate(sqlText)
//  }
//
//  /**
//    * Upsert data into the table (schema.table) from an RDD.  This will insert the data if the record is not found by primary key and if it is it will change
//    * the columns that are different between the two records.
//    *
//    * @param rdd input data
//    * @param schema
//    * @param schemaTableName
//    */
//  def upsert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
//    SpliceRDDVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(rdd))
//    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
//    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") --splice-properties insertMode=UPSERT\n select " + columnList + " from " +
//      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
//      "as SpliceRDDVTI (" + schemaString + ")"
//    internalConnection.createStatement().executeUpdate(sqlText)
//  }
//
//  /**
//    * Delete records in a dataframe based on joining by primary keys from the data frame.  Be careful with column naming and case sensitivity.
//    *
//    * @param dataFrame rows to delete
//    * @param schemaTableName table to delete from
//    */
//  def delete(dataFrame: DataFrame, schemaTableName: String): Unit = {
//    val jdbcOptions = new JDBCOptions(Map(
//      JDBCOptions.JDBC_URL -> url,
//      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
//    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
//    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
//    if (keys.length == 0)
//      throw new UnsupportedOperationException("Primary Key Required for the Table to Perform Deletes")
//    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
//    val sqlText = "delete from " + schemaTableName + " where exists (select 1 from " +
//      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
//      "as SDVTI (" + schemaString + ") where "
//    val dialect = JdbcDialects.get(url)
//    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) +
//      " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
//    val combinedText = sqlText + whereClause + ")"
//    internalConnection.createStatement().executeUpdate(combinedText)
//  }
//
//  /**
//    * Delete records in a dataframe based on joining by primary keys from the data frame.  Be careful with column naming and case sensitivity.
//    *
//    * @param rdd rows to delete
//    * @param schema
//    * @param schemaTableName table to delete from
//    */
//  def delete(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
//    val jdbcOptions = new JDBCOptions(Map(
//      JDBCOptions.JDBC_URL -> url,
//      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
//    SpliceRDDVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(rdd))
//    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
//    if (keys.length == 0)
//      throw new UnsupportedOperationException("Primary Key Required for the Table to Perform Deletes")
//    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
//    val sqlText = "delete from " + schemaTableName + " where exists (select 1 from " +
//      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
//      "as SDVTI (" + schemaString + ") where "
//    val dialect = JdbcDialects.get(url)
//    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) +
//      " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
//    val combinedText = sqlText + whereClause + ")"
//    internalConnection.createStatement().executeUpdate(combinedText)
//  }
//
//  /**
//    * Update data from a dataframe for a specified schemaTableName (schema.table).  The keys are required for the update and any other
//    * columns provided will be updated in the rows.
//    *
//    * @param dataFrame rows for update
//    * @param schemaTableName table to update
//    */
//  def update(dataFrame: DataFrame, schemaTableName: String): Unit = {
//    val jdbcOptions = new JDBCOptions(Map(
//      JDBCOptions.JDBC_URL -> url,
//      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
//    SpliceDatasetVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(dataFrame))
//    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
//    if (keys.length == 0)
//      throw new UnsupportedOperationException("Primary Key Required for the Table to Perform Updates")
//    val prunedFields = dataFrame.schema.fieldNames.filter((p: String) => keys.indexOf(p) == -1)
//    val columnList = SpliceJDBCUtil.listColumns(prunedFields)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
//    val sqlText = "update " + schemaTableName + " " +
//      "set (" + columnList + ") = (" +
//      "select " + columnList + " from " +
//      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
//      "as SDVTI (" + schemaString + ") where "
//    val dialect = JdbcDialects.get(url)
//    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) +
//      " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
//    val combinedText = sqlText + whereClause + ")"
//    internalConnection.createStatement().executeUpdate(combinedText)
//  }
//
//  /**
//    * Update data from a RDD for a specified schemaTableName (schema.table) and schema (StructType).  The keys are required for the update and any other
//    * columns provided will be updated in the rows.
//    *
//    * @param rdd rows for update
//    * @param schema
//    * @param schemaTableName
//    */
//  def update(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = {
//    val jdbcOptions = new JDBCOptions(Map(
//      JDBCOptions.JDBC_URL -> url,
//      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
//    SpliceRDDVTI.datasetThreadLocal.set(ShuffleUtils.shuffle(rdd))
//    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
//    if (keys.length == 0)
//      throw new UnsupportedOperationException("Primary Key Required for the Table to Perform Updates")
//    val prunedFields = schema.fieldNames.filter((p: String) => keys.indexOf(p) == -1)
//    val columnList = SpliceJDBCUtil.listColumns(prunedFields)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
//    val sqlText = "update " + schemaTableName + " " +
//      "set (" + columnList + ") = (" +
//      "select " + columnList + " from " +
//      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
//      "as SDVTI (" + schemaString + ") where "
//    val dialect = JdbcDialects.get(url)
//    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) +
//      " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
//    val combinedText = sqlText + whereClause + ")"
//    internalConnection.createStatement().executeUpdate(combinedText)
//  }
//
//  /**
//    * Bulk Import HFile from a dataframe into a schemaTableName(schema.table)
//    *
//    * @param dataFrame input data
//    * @param schemaTableName
//    * @param options options to be passed to --splice-properties; bulkImportDirectory is required
//    */
//  def bulkImportHFile(dataFrame: DataFrame, schemaTableName: String,
//                      options: scala.collection.mutable.Map[String, String]): Unit = {
//
//    val bulkImportDirectory = options.get("bulkImportDirectory")
//    if (bulkImportDirectory == null) {
//      throw new IllegalArgumentException("bulkImportDirectory cannot be null")
//    }
//    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
//    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
//    var properties = "--SPLICE-PROPERTIES "
//    options foreach (option => properties += option._1 + "=" + option._2 + ",")
//    properties = properties.substring(0, properties.length - 1)
//
//    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") " + properties + "\n" +
//      "select " + columnList + " from " +
//      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
//      "as SpliceDatasetVTI (" + schemaString + ")"
//    internalConnection.createStatement().executeUpdate(sqlText)
//  }
//
//  /**
//    * Bulk Import HFile from a RDD into a schemaTableName(schema.table)
//    *
//    * @param rdd input data
//    * @param schemaTableName
//    * @param options options to be passed to --splice-properties; bulkImportDirectory is required
//    */
//  def bulkImportHFile(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String,
//                      options: scala.collection.mutable.Map[String, String]): Unit = {
//
//    val bulkImportDirectory = options.get("bulkImportDirectory")
//    if (bulkImportDirectory == null) {
//      throw new IllegalArgumentException("bulkImportDirectory cannot be null")
//    }
//    SpliceRDDVTI.datasetThreadLocal.set(rdd)
//    val columnList = SpliceJDBCUtil.listColumns(schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(schema, url)
//    var properties = "--SPLICE-PROPERTIES "
//    options foreach (option => properties += option._1 + "=" + option._2 + ",")
//    properties = properties.substring(0, properties.length - 1)
//
//    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") " + properties + "\n" +
//      "select " + columnList + " from " +
//      "new com.splicemachine.derby.vti.SpliceRDDVTI() " +
//      "as SpliceRDDVTI (" + schemaString + ")"
//    internalConnection.createStatement().executeUpdate(sqlText)
//  }

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

  private[this]val dialect = new SplicemachineDialect2
  private[this]val dialectNoTime = new SplicemachineDialectNoTime2
//  private[this]def resolveQuery(connection: Connection, sql: String, noTime: Boolean): StructType = {
//    try {
//      val rs = connection.prepareStatement(s"select * from ($sql) a where 1=0 ").executeQuery()
//
//      try {
//        if (noTime)
//          JdbcUtils.getSchema(rs, dialectNoTime)
//        else
//          JdbcUtils.getSchema(rs, dialect)
//      } finally {
//        rs.close()
//      }
//    }
//  }
//
//  /**
//    * Prune all but the specified columns from the specified Catalyst schema.
//    *
//    * @param schema  - The Catalyst schema of the master table
//    * @param columns - The list of desired columns
//    * @return A Catalyst schema corresponding to columns in the given order.
//    */
//  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
//    val fieldMap = Map(schema.fields.map(x => x.metadata.getString("name") -> x): _*)
//    new StructType(columns.map(name => fieldMap(name)))
//  }
//
//  /**
//    * Export a dataFrame in CSV
//    *
//    * @param location  - Destination directory
//    * @param compression - Whether to compress the output or not
//    * @param replicationCount - Replication used for HDFS write
//    * @param fileEncoding - fileEncoding or null, defaults to UTF-8
//    * @param fieldSeparator - fieldSeparator or null, defaults to ','
//    * @param quoteCharacter - quoteCharacter or null, defaults to '"'
//    *
//    */
//  def export(dataFrame: DataFrame, location: String,
//                   compression: Boolean, replicationCount: Int,
//             fileEncoding: String,
//             fieldSeparator: String,
//             quoteCharacter: String): Unit = {
//    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
//    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
//    val encoding = quotedOrNull(fileEncoding)
//    val separator = quotedOrNull(fieldSeparator)
//    val quoteChar = quotedOrNull(quoteCharacter)
//    val sqlText = s"export ( '$location', $compression, $replicationCount, $encoding, $separator, $quoteChar) select " + columnList + " from " +
//      s"new com.splicemachine.derby.vti.SpliceDatasetVTI() as SpliceDatasetVTI ($schemaString)"
//    internalConnection.createStatement().execute(sqlText)
//  }
//
//  private[this] def quotedOrNull(value: String) = {
//    if (value == null) "null" else s"'$value"
//  }
//
//  /**
//    * Export a dataFrame in binary format
//    *
//    * @param location  - Destination directory
//    * @param compression - Whether to compress the output or not
//    * @param format - Binary format to be used, currently only 'parquet' is supported
//    */
//  def exportBinary(dataFrame: DataFrame, location: String,
//                   compression: Boolean, format: String): Unit = {
//    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
//    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
//    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
//    val sqlText = s"export_binary ( '$location', $compression, '$format') select " + columnList + " from " +
//      s"new com.splicemachine.derby.vti.SpliceDatasetVTI() as SpliceDatasetVTI ($schemaString)"
//    internalConnection.createStatement().execute(sqlText)
//  }
}
