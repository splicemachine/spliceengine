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
package com.splicemachine.spark2.splicemachine

import java.io.Externalizable
import java.security.SecureRandom
import java.sql.{Connection, ResultSetMetaData}
import java.util.Properties

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
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
import com.splicemachine.db.impl.sql.execute.ValueRow
import com.splicemachine.derby.stream.spark.ExternalizableSerializer
import com.splicemachine.nsds.kafka.KafkaTopics
import com.splicemachine.nsds.kafka.KafkaUtils
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

import scala.collection.JavaConverters._

@SerialVersionUID(20200517241L)
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
@SerialVersionUID(20200517242L)
@SuppressFBWarnings(value = Array("NP_ALWAYS_NULL"), justification = "These fields usually are not null")
@SuppressFBWarnings(value = Array("NP_LOAD_OF_KNOWN_NULL_VALUE"), justification = "These fields usually are not null")
@SuppressFBWarnings(value = Array("EI_EXPOSE_REP2"), justification = "The nonKeys value is needed and is used read-only")
@SuppressFBWarnings(value = Array("SE_BAD_FIELD"), justification = "The meta and itrRow objects are not used in serialization")
class SplicemachineContext(options: Map[String, String]) extends Serializable {
  private[this] val url = options(JDBCOptions.JDBC_URL) + ";useSpark=true"

  private[this] val kafkaServers = options.getOrElse(KafkaOptions.KAFKA_SERVERS, "localhost:9092")
  println(s"Splice Kafka: $kafkaServers")

  private[this] val kafkaPollTimeout = options.getOrElse(KafkaOptions.KAFKA_POLL_TIMEOUT, "20000").toLong
  
  private[this] val kafkaTopics = new KafkaTopics(kafkaServers)

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

  // Check url validity, throws exception during instantiation if url is invalid
  try {
    if( options(JDBCOptions.JDBC_URL).isEmpty ) throw new Exception("JDBC Url is an empty string.")
    getConnection()
  } catch {
    case e: Exception => throw new Exception(
      "Problem connecting to the DB. Verify that the input JDBC Url is correct."
        + "\n"
        + e.toString
    )
  }

  private[this]val dialect = new SplicemachineDialect2
  private[this]val dialectNoTime = new SplicemachineDialectNoTime2
  JdbcDialects.registerDialect(new SplicemachineDialect2)

  SparkSession.builder.getOrCreate.sparkContext.addSparkListener(new SparkListener {
    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      println(s"Cleaning up SplicemachineContext.")
      try {
        kafkaTopics.cleanup(60*1000)
      } catch {
        case e: Throwable => ;  // no-op, undeleted topics should be handled by separate cleanup process
      }
      println(s"Clean up of SplicemachineContext Completed.")
    }
  })

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
      getJdbcOptionsInWrite( schemaTableName )
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
    val (conn, jdbcOptionsInWrite) = getConnection(schemaTableName)
    try {
      JdbcUtils.dropTable(conn, jdbcOptionsInWrite.table, jdbcOptionsInWrite)
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
    val (conn, jdbcOptionsInWrite) = getConnection(schemaTableName)
    val statement = conn.createStatement
    try {
      val actSchemaString = schemaString(structType, jdbcOptionsInWrite.url)

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

  private[this] def getJdbcOptionsInWrite(schemaTableName: String): JdbcOptionsInWrite =
    new JdbcOptionsInWrite( Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName
    ))

  /**
   * Get JDBC connection
   */
  def getConnection(): Connection = getConnection("placeholder")._1

  /**
   * Get JDBC connection
   */
  private[this] def getConnection(schemaTableName: String): (Connection, JdbcOptionsInWrite) = {
    val jdbcOptionsInWrite = getJdbcOptionsInWrite( schemaTableName )
    val conn = JdbcUtils.createConnectionFactory( jdbcOptionsInWrite )()
    (conn, jdbcOptionsInWrite)
  }

  /**
    *
    * Execute an update statement via JDBC against Splice Machine
    *
    * @param sql
    */
  def executeUpdate(sql: String): Unit = {
    val conn = getConnection()
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
    val conn = getConnection()
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

  /**
    * SQL to Dataset translation.
    * Runs the query inside Splice Machine and sends the results to the Spark Adapter app through Kafka
    *
    * @param sql SQL query
    * @return Dataset[Row] with the result of the query
    */
  def df(sql: String): Dataset[Row] = {
    val topicName = kafkaTopics.create
    try {
      sendSql(sql, topicName)
      new KafkaToDF(kafkaServers, kafkaPollTimeout, getSchemaOfQuery(sql)).df(topicName)
    } finally {
      kafkaTopics.delete(topicName)
    }
  }

  def internalDf(sql: String): Dataset[Row] = df(sql)

  private[this] def sendSql(sql: String, topicName: String): Unit = {
    if( sql.toUpperCase.replace(" ","").contains("USESPARK=FALSE") ) {
      throw new IllegalArgumentException(s"Property useSpark=false is not supported by ${this.getClass.getName}")
    }

    // hbase user has read/write permission on the topic
    val conn = getConnection()
    val statement = conn.prepareStatement(s"EXPORT_KAFKA('$topicName') " + sql)
    try {
//      println( s"SMC.sendSql sql $sql" )
      statement.execute()
    } finally {
      statement.close()
      conn.close()
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
  def rdd(schemaTableName: String,
                  columnProjection: Seq[String] = Nil): RDD[Row] = {
    val columnList = SpliceJDBCUtil.listColumns(columnProjection.toArray)
    val sqlText = s"SELECT $columnList FROM ${schemaTableName}"
    val topicName = kafkaTopics.create
    try {
      sendSql(sqlText, topicName)
      new KafkaToDF(kafkaServers, kafkaPollTimeout, getSchemaOfQuery(sqlText)).rdd(topicName)
    } finally {
      kafkaTopics.delete(topicName)
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

  private[this] def columnList(schema: StructType): String = SpliceJDBCUtil.listColumns(schema.fieldNames)
  private[this] def schemaString(schema: StructType): String = SpliceJDBCUtil.schemaWithoutNullableString(schema, url).replace("\"","")

  private[this] def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String, spliceProperties: scala.collection.immutable.Map[String,String]): Unit = {
    val topicName = kafkaTopics.create
//    println( s"SMC.insert topic $topicName" )

    // hbase user has read/write permission on the topic
    try {
      sendData(topicName, rdd, schema)

      val colList = columnList(schema)
      val sProps = spliceProperties.map({ case (k, v) => k + "=" + v }).fold("--splice-properties useSpark=true")(_ + ", " + _)
      val sqlText = "insert into " + schemaTableName + " (" + colList + ") " + sProps + "\nselect " + colList + " from " +
        "new com.splicemachine.derby.vti.KafkaVTI('" + topicName + "') " +
        "as SpliceDatasetVTI (" + schemaString(schema) + ")"

      //println( s"SMC.insert sql $sqlText" )
      executeUpdate(sqlText)
    } finally {
      kafkaTopics.delete(topicName)
    }
  }

  private[this] def sendData(topicName: String, rdd: JavaRDD[Row], schema: StructType): Unit =
    rdd.rdd.mapPartitionsWithIndex(
      (partition, itrRow) => {
        val taskContext = TaskContext.get

        var msgIdx = 0
        if (taskContext != null && taskContext.attemptNumber > 0) {
          val entriesInKafka = KafkaUtils.messageCount(kafkaServers, topicName, partition)
          for(i <- (1: Long) to entriesInKafka) {
            itrRow.next
          }
          msgIdx = entriesInKafka.asInstanceOf[Int]
        }

        val props = new Properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-s2s-smc-"+java.util.UUID.randomUUID() )
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ExternalizableSerializer].getName)

        val producer = new KafkaProducer[Integer, Externalizable](props)

        while( itrRow.hasNext ) {
          producer.send( new ProducerRecord(topicName, msgIdx, externalizable(itrRow.next, schema)) )
          msgIdx += 1
        }

        producer.close

        java.util.Arrays.asList("OK").iterator().asScala
      }
    ).collect

  /** Convert org.apache.spark.sql.Row to Externalizable. */
  def externalizable(row: Row, schema: StructType): ValueRow = {
    val valRow = new ValueRow(row.length);
    for (i <- 1 to row.length) {  // convert each column of the row
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
   * Delete records in a dataframe based on joining by primary keys from the data frame.  Be careful with column naming and case sensitivity.
   *
   * @param dataFrame rows to delete
   * @param schemaTableName table to delete from
   */
  def delete(dataFrame: DataFrame, schemaTableName: String): Unit =
    delete(dataFrame.rdd, dataFrame.schema, schemaTableName)

  /**
   * Delete records in a dataframe based on joining by primary keys from the data frame.  Be careful with column naming and case sensitivity.
   *
   * @param rdd rows to delete
   * @param schema
   * @param schemaTableName table to delete from
   */
  def delete(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = if( !rdd.isEmpty ) {
    val keys = primaryKeys(schemaTableName)
    if (keys.length == 0)
      throw new UnsupportedOperationException(s"$schemaTableName has no Primary Key, Required for the Table to Perform Deletes")

    modifyOnKeys(rdd, schema, schemaTableName, keys,
      "delete from " + schemaTableName + " where exists (select 1 "
    )
  }

  def primaryKeys(schemaTableName: String): Array[String] =
    SpliceJDBCUtil.retrievePrimaryKeys(
      getJdbcOptionsInWrite( schemaTableName)
    )
  
  /** Schema string built from JDBC metadata. */
  def schemaString(schemaTableName: String, schema: StructType = new StructType()): String =
    SpliceJDBCUtil.retrieveColumnInfo(
      getJdbcOptionsInWrite( schemaTableName)
    ).filter(
      col => schema.isEmpty || schema.exists( field => field.name.toUpperCase.equals( col(0).toUpperCase ) )
    ).map(i => {
      val colName = i(0)
      val sqlType = i(1)
      val size = sqlType match {
        case "VARCHAR" => s"(${i(2)})"
        case "DECIMAL" | "NUMERIC" => s"(${i(2)},${i(3)})"
        case _ => ""
      }
      s"$colName $sqlType$size"
    }).mkString(", ")
  
  /**
   * Modify records identified by their primary keys.
   *
   * @param rdd rows to modify
   * @param schema
   * @param schemaTableName table to modify
   * @param keys the column names of the primary keys of schemaTableName
   * @param sqlStart beginning of the sql statement
   */
  private[this] def modifyOnKeys(
    rdd: JavaRDD[Row],
    schema: StructType,
    schemaTableName: String,
    keys: Array[String],
    sqlStart: String
  ): Unit = {
    val topicName = kafkaTopics.create
    //println( s"SMC.modifyOnKeys topic $topicName" )
    try {
      sendData(topicName, rdd, schema)

      val sqlText = sqlStart +
        " from new com.splicemachine.derby.vti.KafkaVTI('" + topicName + "') " +
        "as SDVTI (" + schemaString(schemaTableName, schema) + ") where "
      val dialect = JdbcDialects.get(url)
      val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) +
        " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
      val combinedText = sqlText + whereClause + ")"

      //println( s"SMC.modifyOnKeys sql $combinedText" )
      executeUpdate(combinedText)
    } finally {
      kafkaTopics.delete(topicName)
    }
  }

  /**
   * Update data from a dataframe for a specified schemaTableName (schema.table).  The keys are required for the update and any other
   * columns provided will be updated in the rows.
   *
   * @param dataFrame rows for update
   * @param schemaTableName table to update
   */
  def update(dataFrame: DataFrame, schemaTableName: String): Unit =
    update(dataFrame.rdd, dataFrame.schema, schemaTableName)

  /**
   * Update data from a RDD for a specified schemaTableName (schema.table) and schema (StructType).  The keys are required for the update and any other
   * columns provided will be updated in the rows.
   *
   * @param rdd rows for update
   * @param schema
   * @param schemaTableName
   */
  def update(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = if( !rdd.isEmpty ) {
    val keys = primaryKeys(schemaTableName)
    if (keys.length == 0)
      throw new UnsupportedOperationException(s"$schemaTableName has no Primary Key, Required for the Table to Perform Updates")

    val nonKeys = schema.fieldNames.filter((p: String) => keys.indexOf(p) == -1)
    val columnList = SpliceJDBCUtil.listColumns(nonKeys)

    modifyOnKeys(rdd, schema, schemaTableName, keys,
      "update " + schemaTableName +
        " set (" + columnList + ") = (" +
        "select " + columnList
    )
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
   * Upsert data into the table (schema.table) from a DataFrame.  This will insert the data if the record is not found by primary key and if it is it will change
   * the columns that are different between the two records.
   *
   * @param dataFrame input data
   * @param schemaTableName output table
   */
  def upsert(dataFrame: DataFrame, schemaTableName: String): Unit =
    upsert(dataFrame.rdd, dataFrame.schema, schemaTableName)

  /**
   * Upsert data into the table (schema.table) from an RDD.  This will insert the data if the record is not found by primary key and if it is it will change
   * the columns that are different between the two records.
   *
   * @param rdd input data
   * @param schema
   * @param schemaTableName
   */
  def upsert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit =
    insert(rdd, schema, schemaTableName, Map("insertMode"->"UPSERT") )

  /**
    * Return a table's schema via JDBC.
    *
    * @param schemaTableName table
    * @return table's schema
    */
  def getSchema(schemaTableName: String): StructType = {
    val options = new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName
    ))
    val schemaJdbcOpt = JdbcUtils.getSchemaOption(getConnection(), options)

    if( schemaJdbcOpt.isEmpty ) {
      if   ( tableExists(schemaTableName) ) { JDBCRDD.resolveTable( options ) }  // resolveTable should throw a descriptive exception
      else { throw new Exception( s"Table/View '$schemaTableName' does not exist." ) }
    }

    val schemaJdbc = schemaJdbcOpt.get

    // If schemaJdbc contains a ShortType field, it may have been incorrectly mapped by JDBC,
    //  so get a schema from df and take its type
    if( schemaJdbc.exists(_.dataType.isInstanceOf[ShortType]) ) {
      val schemaDf = df(s"select top 1 * from $schemaTableName").schema

      if (schemaJdbc.size == schemaDf.size) {
        var i = 0
        var schema = StructType(Nil)
        for (field <- schemaJdbc.iterator) {
          val dataType = if( field.dataType.typeName.equals("short") ) { schemaDf(i).dataType } else { field.dataType }
          schema = schema.add(field.name, dataType, field.nullable)
          i += 1
        }
        schema
      } else { schemaJdbc }
    } else { schemaJdbc }
  }
  
  private[this] def getSchemaOfQuery(query: String): StructType = {
    val stmt = getConnection.prepareStatement(query)
    try {
      val meta = stmt.getMetaData() // might be jdbc driver dependent
      val colNames = for (c <- 1 to meta.getColumnCount) yield meta.getColumnLabel(c)
      val hasDuplicateNames = colNames.size != colNames.distinct.size
      val fields = for (i <- 1 to meta.getColumnCount) yield {
        val name = if (hasDuplicateNames) {
          meta.getSchemaName(i) + "." + meta.getTableName(i) + "." + meta.getColumnLabel(i)
        } else meta.getColumnLabel(i)
        val dataType = meta.getColumnType(i)
        val typeName = meta.getColumnTypeName(i)
        val precision = meta.getPrecision(i)
        val scale = meta.getScale(i)
        val nullable = meta.isNullable(i) == ResultSetMetaData.columnNullable
        val columnType =
          dialect.getCatalystType(dataType, typeName, precision, null).getOrElse(
            SpliceJDBCUtil.getCatalystType(dataType, precision, scale, meta.isSigned(i)))
        StructField(name, columnType, nullable)
      }
      StructType(fields)
    } finally {
      stmt.close
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
    val str = (value: String) => { Option(value).map(v => if( v.equals("'") ) {s"'$v$v'"} else {s"'$v'"} ).getOrElse("null") }
    export(
      dataFrame,
      s"export ( '$location', $compression, $replicationCount, ${str(fileEncoding)}, ${str(fieldSeparator)}, ${str(quoteCharacter)})"
    )
  }

  /**
   * Export a dataFrame in binary format
   *
   * @param location  - Destination directory
   * @param compression - Whether to compress the output or not
   * @param format - Binary format to be used, currently only 'parquet' is supported
   */
  def exportBinary(dataFrame: DataFrame, location: String,
                   compression: Boolean, format: String): Unit =
    export(dataFrame, s"export_binary ( '$location', $compression, '$format')")


  private[this] def export(dataFrame: DataFrame, exportCmd: String): Unit = {
    if( dataFrame.isEmpty ) {
      throw new IllegalArgumentException( "Dataframe is empty." )
    }

    val topicName = kafkaTopics.create
    //println( s"SMC.export topic $topicName" )
    try {
      val schema = dataFrame.schema
      sendData(topicName, dataFrame.rdd, schema)

      val sqlText = exportCmd + s" select " + columnList(schema) + " from " +
        s"new com.splicemachine.derby.vti.KafkaVTI('" + topicName + s"') as SpliceDatasetVTI (${schemaString(schema)})"
      //println( s"SMC.export sql $sqlText" )
      execute(sqlText)
    } finally {
      kafkaTopics.delete(topicName)
    }
  }
}
