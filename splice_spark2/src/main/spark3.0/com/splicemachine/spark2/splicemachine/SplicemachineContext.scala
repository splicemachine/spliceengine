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

import java.sql.{Connection, ResultSetMetaData}
import java.util.Properties

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, IntegerSerializer}
import com.splicemachine.db.iapi.types.{SQLBlob, SQLBoolean, SQLClob, SQLDate, SQLDecimal, SQLDouble, SQLInteger, SQLLongint, SQLReal, SQLSmallint, SQLTime, SQLTimestamp, SQLTinyint, SQLVarchar}
import com.splicemachine.db.impl.sql.execute.ValueRow
import com.splicemachine.derby.impl.kryo.KryoSerialization
import com.splicemachine.derby.stream.spark.KafkaReadFunction
import com.splicemachine.nsds.kafka.{KafkaTopics, KafkaUtils}
import com.splicemachine.spark2.splicemachine
import com.splicemachine.spark2.splicemachine.SplicemachineContext.RowForKafka
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

import scala.collection.JavaConverters._

@SerialVersionUID(20200517301L)
private object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

object KafkaOptions {
  val KAFKA_SERVERS = "KAFKA_SERVERS"
  val KAFKA_POLL_TIMEOUT = "KAFKA_POLL_TIMEOUT"
  val KAFKA_TOPIC_PARTITIONS = "KAFKA_TOPIC_PARTITIONS"
}

object SplicemachineContext {
  @SerialVersionUID(20200922301L)
  class RowForKafka(
      topicName: String,
      partition: Int,
      schema: StructType
    ) extends Serializable
  {
    var sparkRow: Option[Row] = None
    var valueRow: Option[ValueRow] = None
    var msgCount: Int = -1
    
    def topicName(): String = topicName
    def partition(): Int = partition
    def schema(): StructType = schema
    
    override def toString(): String =
      if( valueRow.isDefined ) {
        valueRow.toString
      } else {
        "None"
      }

    def send(producer: KafkaProducer[Integer, Array[Byte]], kryo: KryoSerialization, last: Boolean = false): Unit =
      if( valueRow.isDefined ) {
        producer.send( new ProducerRecord(
          topicName,
          partition,
          partition,
          kryo.serialize(
            new KafkaReadFunction.Message(
              valueRow.get,
              msgCount,
              if(last) { msgCount } else -1
            )
          )
        ))
      }
  }
}

object Options {
  val USE_FLOW_MARKERS = "USE_FLOW_MARKERS"
}

/**
  *
  * Context for Splice Machine.
  *
  * @param options Supported options are JDBCOptions.JDBC_URL (required), JDBCOptions.JDBC_INTERNAL_QUERIES,
  *                JDBCOptions.JDBC_TEMP_DIRECTORY, KafkaOptions.KAFKA_SERVERS, KafkaOptions.KAFKA_POLL_TIMEOUT,
  *                KafkaOptions.KAFKA_TOPIC_PARTITIONS, Options.USE_FLOW_MARKERS
  */
@SerialVersionUID(20200517302L)
@SuppressFBWarnings(value = Array("NP_ALWAYS_NULL","NP_LOAD_OF_KNOWN_NULL_VALUE","EI_EXPOSE_REP2","SE_BAD_FIELD"), justification = "These fields usually are not null|These fields usually are not null|The nonKeys value is needed and is used read-only|The meta and itrRow objects are not used in serialization")
class SplicemachineContext(options: Map[String, String]) extends Serializable {
  private[this] val url = options(JDBCOptions.JDBC_URL) + ";useSpark=true"

  private[this] val kafkaServers = options.getOrElse(KafkaOptions.KAFKA_SERVERS, "localhost:9092")
  println(s"Splice Kafka: $kafkaServers")

  private[this] val kafkaPollTimeout = options.getOrElse(KafkaOptions.KAFKA_POLL_TIMEOUT, "20000").toLong

  private[this] val insertTopicPartitions = options.getOrElse(KafkaOptions.KAFKA_TOPIC_PARTITIONS, "1").toInt

  @transient lazy private[this] val kafkaTopics = new KafkaTopics(
    kafkaServers,
    insertTopicPartitions
  )

  private[this] val useFlowMarkers = options.getOrElse(Options.USE_FLOW_MARKERS, "false").toBoolean
  
  private[this] val (fmColList, fmSchemaStr, fmCount) = if( useFlowMarkers ) {
    (",PTN_NSDS,TM_NSDS", ", PTN_NSDS INTEGER, TM_NSDS BIGINT", 2)
  } else {
    ("", "", 0)
  }

  @transient lazy private[this] val log = Holder.log

  private[this] val insAccum = SparkSession.builder.getOrCreate.sparkContext.longAccumulator("NSDSv2_Ins")
  private[this] val lastRowsToSend = 
    SparkSession.builder.getOrCreate.sparkContext.collectionAccumulator[RowForKafka]("LastRowsToSend")
  
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

  private[this] def info(msg: String): Unit = {
    log.info(s"${java.time.Instant.now} $msg")
  }

  private[this] def debug(msg: String): Unit = {
    log.debug(s"${java.time.Instant.now} $msg")
  }

  private[this] def trace(msg: String): Unit = {
    log.trace(s"${java.time.Instant.now} $msg")
  }

  def columnNamesCaseSensitive(caseSensitive: Boolean): Unit =
    splicemachine.columnNamesCaseSensitive(caseSensitive)

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
        ", PRIMARY KEY(" + keys.map(quoteIdentifier(_)).mkString(", ") + ")"
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
    val topicName = kafkaTopics.create()
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
      trace( s"SMC.sendSql sql $sql" )
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
    val topicName = kafkaTopics.create()
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
  def insert(dataFrame: DataFrame, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Long =
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
  def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String, statusDirectory: String, badRecordsAllowed: Integer): Long =
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
  def insert(dataFrame: DataFrame, schemaTableName: String): Long = insert(dataFrame.rdd, dataFrame.schema, schemaTableName)

  /**
   * Insert a RDD into a table (schema.table).  The schema is required since RDD's do not have schema.
   *
   * @param rdd input data
   * @param schema
   * @param schemaTableName
   */
  def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Long = insert(rdd, schema, schemaTableName, Map[String,String]())

  private[this] def columnList(schema: StructType): String = SpliceJDBCUtil.listColumns(schema.fieldNames)

  private[this] def insert(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String, 
                           spliceProperties: scala.collection.immutable.Map[String,String]): Long = if( rdd.getNumPartitions > 0 ) {
    debug("SMC.ins get topic name")
    val topicName = if( rdd.getNumPartitions == insertTopicPartitions ) {
      kafkaTopics.create()
    } else {
      kafkaTopics.createTopic(rdd.getNumPartitions)
    }
    trace( "SMC.insert topic $topicName" )

    // hbase user has read/write permission on the topic
    try {
      insAccum.reset
      debug("SMC.ins sendData")
      val tableSchemaStr = schemaString( columnInfo(schemaTableName) , schema)
      val ptnInfo = sendData(topicName, rdd, modifySchema(schema, tableSchemaStr))

      if( ! insAccum.isZero ) {
        debug("SMC.ins prepare sql")
        val colList = columnList(schema) + fmColList
        val sProps = spliceProperties.map({ case (k, v) => k + "=" + v }).fold("--splice-properties useSpark=true")(_ + ", " + _)
        val sqlText = "insert into " + schemaTableName + " (" + colList + ") " + sProps + "\nselect " + colList + " from " +
          "new com.splicemachine.derby.vti.KafkaVTI('" + topicName + topicSuffix(ptnInfo, rdd.getNumPartitions) + "') " +
          "as SpliceDatasetVTI (" + tableSchemaStr + fmSchemaStr + ")"

        debug( s"SMC.insert sql $sqlText" )
        debug("SMC.ins executeUpdate")
        executeUpdate(sqlText)
        debug("SMC.ins done")
      }
      
      insAccum.sum
    } finally {
      kafkaTopics.delete(topicName)
    }
  } else { 0L }

  private[this] val activePartitionAcm =
    SparkSession.builder.getOrCreate.sparkContext.collectionAccumulator[String]("ActivePartitions")

  def activePartitions(df: DataFrame): Seq[Int] = {
    activePartitionAcm.reset
    df.rdd.mapPartitionsWithIndex((p, itr) => {
      activePartitionAcm.add( s"$p ${itr.nonEmpty}" )
      Iterator.apply("OK")
    }).collect
  
    activePartitionAcm.value.asScala.filter( _.endsWith("true") ).map( _.split(" ")(0).toInt )
  }

  var insertSql: String => String = _
  
  /* Sets up insertSql to be used by insert_streaming */
  def setTable(schemaTableName: String, schema: StructType): Unit = {
    val colList = columnList(schema) + fmColList
    val schStr = schemaStringWithoutNullable(schema, url)
    // Line break at the end of the first line and before select is required, other line breaks aren't required
    insertSql = (topicName: String) => s"""insert into $schemaTableName ($colList)
                                       select $colList from 
      new com.splicemachine.derby.vti.KafkaVTI('$topicName') 
      as SpliceDatasetVTI ($schStr$fmSchemaStr)"""
  }
  
  def insert_streaming(topicInfo: String, retries: Int = 0): Unit = {
    val topicName = if( topicInfo.contains("::") ) {
      topicInfo.split("::")(0)
    } else {
      topicInfo
    }
    try {
      debug("SMC.inss prepare sql")
      val sqlText = insertSql(topicInfo)
      debug(s"SMC.inss sql $sqlText")
      
      trace( s"SMC.inss topicCount preex ${KafkaUtils.messageCount(kafkaServers, topicName)}")

      debug("SMC.inss executeUpdate")
      executeUpdate(sqlText)
      debug("SMC.inss done")

      debug( s"SMC.inss topicCount postex ${KafkaUtils.messageCount(kafkaServers, topicName)}")
    } catch {
      case e: java.sql.SQLNonTransientConnectionException => 
        if( retries < 2 ) {
          insert_streaming(topicInfo, retries + 1)
        }
    } finally {
      kafkaTopics.delete(topicName)
    }
  }

  def newTopic_streaming(): String = {
    debug("SMC.nit get topic name")
    kafkaTopics.create()
  }
  
  def sendData_streaming(dataFrame: DataFrame, topicName: String): (Seq[RowForKafka], Long, Array[String]) = {
    insAccum.reset
    lastRowsToSend.reset
    debug("SMC.sds sendData")
    val ptnInfo = sendData(topicName, dataFrame.rdd, dataFrame.schema, true)

    val rows = lastRowsToSend.value.asScala
    trace(s"SMC.sds last rows ${rows.mkString("\n")}")

    (rows, insAccum.sum, ptnInfo)
  }
  
//  /** checkRecovery was written to help debug an issue and normally won't need to be called.
//   *  Keep it here for reference.
//   */
//  private[this] def checkRecovery(
//     id: String,
//     topicName: String, 
//     partition: Int, 
//     itr: Iterator[Row],
//     schema: StructType
//   ): Unit = {
//
//    val lastVR = KafkaUtils.lastMessageOf(kafkaServers, topicName, partition)
//      .asInstanceOf[KafkaReadFunction.Message].vr
//    val cols = if( lastVR.length > 0) { Range(0,lastVR.length-1).toArray } else { Array(0) }
//    val lastKHash = lastVR.hashCode(cols)
//
//    val khashcodes = KafkaUtils.messagesFrom(kafkaServers, topicName, partition)
//      .map( _.asInstanceOf[KafkaReadFunction.Message].vr.hashCode(cols) )
//
//    debug(s"$id SMC.checkRecovery 1st Kafka hashcode ${khashcodes.headOption.getOrElse(-1)}" )
//    
//    var i = 0
//    var res = Seq.empty[String]
//    while( itr.hasNext ) {
//      val hashcode = externalizable(itr.next, schema, partition).hashCode(cols)
//      res = res :+ s"$i,${khashcodes.indexOf(hashcode)}\t${hashcode==lastKHash}"
//      i += 1
//    }
//    
//    debug(s"$id SMC.checkRecovery res: Kafka count ${khashcodes.size}\n${res.mkString("\n")}")
//  }
  
  private[this] var sendDataTimestamp: Long = _
  
  private[this] def sendData(
    topicName: String, 
    rdd: JavaRDD[Row], 
    schema: StructType,
    accumulateLastRows: Boolean = false
  ): Array[String] = {
    sendDataTimestamp = System.currentTimeMillis
    rdd.rdd.mapPartitionsWithIndex(
      (partition, itrRow) => {
        val id = topicName.substring(0,5)+":"+partition.toString
        trace(s"$id SMC.sendData p== $partition ${itrRow.nonEmpty}")

        var msgCount = 0
        if( itrRow.nonEmpty ) {
          val taskContext = TaskContext.get
          val itr = if (taskContext != null && taskContext.attemptNumber > 0) {
            // Recover from previous task failure
            // Be sure the iterator is advanced past the items previously published to Kafka

            info(s"$id SMC.sendData Retry $partition ${taskContext.attemptNumber} ${insAccum.sum}")

            //val itr12 = itrRow //.duplicate
            //checkRecovery(id, topicName, partition, itr12._1, schema)

            val lastMsg = KafkaUtils.lastMessageOf(kafkaServers, topicName, partition)
            if (lastMsg.isEmpty) {
              itrRow
            } else {
              // Convert last message in Kafka to a ValueRow (lastVR)
              val lastVR = lastMsg.get.asInstanceOf[KafkaReadFunction.Message].vr
              // Get the hash code (lastKHash) of lastVR based on the columns that are in lastVR (hashCols)
              val hashCols = if( lastVR.length > 0) { Range(0,lastVR.length-1).toArray } else { Array(0) }
              val lastKHash = lastVR.hashCode(hashCols)
              // Define function (hash) for converting a spark row to a ValueRow and getting its hash code
              def hash: Row => Int = row => externalizable(row, schema, partition).hashCode(hashCols)

              // Get a pair of iterators (itr34) of rdd data to use for different purposes
              //val itr34 = itr12._2.duplicate
              val itr34 = itrRow.duplicate //itr12.duplicate

              // Use span to split itr34._1 into a pair of iterators (inKafka_NotInKafka).
              // inKafka_NotInKafka._1 will contain all of the rows from the beginning of itrRow whose hash != lastKHash.
              // inKafka_NotInKafka._2 will contain all of the rows from the one whose hash == lastKHash to the end of itrRow.
              // So inKafka_NotInKafka._1 will contain rows already in Kafka, and inKafka_NotInKafka._2 will contain the 
              //  last row in Kafka followed by rows that are not in Kafka.
              val inKafka_NotInKafka = itr34._1.span(hash(_) != lastKHash)
              if (inKafka_NotInKafka._2.hasNext) {
                inKafka_NotInKafka._2.next // matches the last item in Kafka, get past it
                msgCount = inKafka_NotInKafka._1.size + 1 // this message count was lost during previous task failure
                inKafka_NotInKafka._2
              } else {
                // In this case, itrRow didn't contain the last row of Kafka, so inKafka_NotInKafka._2 is empty.
                // This happens when itrRow starts after the last item added to Kafka.
                itr34._2
              }
            }
          } else {
            itrRow
          }

          val props = new Properties
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
          props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-s2s-smc-" + java.util.UUID.randomUUID())
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
          //        // Throughput performance?
          //        trace(s"SMC.sendData batch 1MB linger 750ms")
          //        props.put(ProducerConfig.BATCH_SIZE_CONFIG, (1000*1000).toString )
          //        props.put(ProducerConfig.LINGER_MS_CONFIG, "500")

          val producer = new KafkaProducer[Integer, Array[Byte]](props)

          val rowK = new RowForKafka(topicName, partition, schema)
          rowK.sparkRow = if (itr.hasNext) {
            msgCount += 1
            Some(itr.next)
          } else None
          rowK.msgCount = msgCount

          val kryo = new KryoSerialization()
          kryo.init

          while (itr.hasNext) {
            rowK.valueRow = Some(externalizable(rowK.sparkRow.get, schema, partition))
            rowK.send(producer, kryo)
            msgCount += 1
            rowK.sparkRow = Some(itr.next)
            rowK.msgCount = msgCount
          }

          if (accumulateLastRows) {
            lastRowsToSend.add(rowK)
          } else {
            rowK.valueRow = Some(externalizable(rowK.sparkRow.get, schema, partition))
            rowK.send(producer, kryo, true)
          }

          kryo.close

          insAccum.add(msgCount)

          debug(s"$id SMC.sendData t $topicName p $partition records $msgCount")

          producer.flush
          producer.close
        }
        java.util.Arrays.asList(s"$partition $msgCount").iterator().asScala
      }
    ).collect
  }
  
  def activePartitions(ptnInfo: Array[String]): Seq[Int] =
    ptnInfo.filter( _.split(" ")(1).toInt > 0 ).map( _.split(" ")(0).toInt )

  def topicSuffix(ptnInfo: Array[String], numPartitionsSpark: Int): String = {
    val activePtn = activePartitions(ptnInfo)
    if (activePtn.size == numPartitionsSpark) { "" }
    else { s"::${activePtn.mkString(",")}" }
  }

  def sendData(rows: Seq[RowForKafka], last: Boolean): Unit = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-s2s-smcrfk-"+java.util.UUID.randomUUID() )
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    val producer = new KafkaProducer[Integer, Array[Byte]](props)

    val kryo = new KryoSerialization()
    kryo.init
    
    rows.foreach( r => {
      if( r.sparkRow.isDefined ) {
        r.valueRow = Some(externalizable(r.sparkRow.get, r.schema, r.partition))
        r.send(producer, kryo, last)
      }
    })
    
    kryo.close
    
    producer.flush
    producer.close
  }

  /** Convert org.apache.spark.sql.Row to Externalizable. */
  def externalizable(row: Row, schema: StructType, partition: Int): ValueRow = {
    val valRow = new ValueRow(row.length + fmCount)
    for (i <- 1 to row.length) {  // convert each column of the row
      val fieldDef = schema(i-1)
      spliceType( fieldDef.dataType , row , i-1 ) match {
        case Some(splType) =>
          valRow.setColumn( i , splType )
        case None =>
          throw new IllegalArgumentException(s"Can't get Splice type for ${fieldDef.dataType.simpleString}")
      }
    }
    if( useFlowMarkers ) {
      valRow.setColumn(row.length + 1, new SQLInteger(partition))
      valRow.setColumn(row.length + 2, new SQLLongint(sendDataTimestamp))
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
        val varchar = new SQLVarchar
        if (isNull) varchar.setToNull else varchar.setValue( row.getString(i) )
        Option(varchar)
        // Keep CLOB code for reference in case it's needed in the future
        // val clob = new SQLClob
        // clob setStreamHeaderFormat false
        // if (isNull) clob.setToNull else clob.setValue( row.getString(i) )
        // Option(clob)
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
      case TimeType => {
        val tm = new SQLTime
        if (isNull) tm.setToNull else tm.setValue( java.sql.Time.valueOf( row.getTimestamp(i).toLocalDateTime.toLocalTime ) , null )
        Option(tm)
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
  def delete(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = /* if( !rdd.isEmpty ) */ {
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
  
  def columnInfo(schemaTableName: String): Array[Seq[String]] = {
    val info = SpliceJDBCUtil.retrieveColumnInfo(
      getJdbcOptionsInWrite( schemaTableName )
    )
    if( info.nonEmpty ) { info }
    else{ throw new Exception(s"No column metadata found for $schemaTableName") }
  }

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
  ): Unit = if( rdd.getNumPartitions > 0 ) {
    val topicName = if( rdd.getNumPartitions == insertTopicPartitions ) {
      kafkaTopics.create()
    } else {
      kafkaTopics.createTopic(rdd.getNumPartitions)
    }
    trace( s"SMC.modifyOnKeys topic $topicName" )
    try {
      insAccum.reset
      val tableSchemaStr = schemaString( columnInfo(schemaTableName) , schema)
      val ptnInfo = sendData(topicName, rdd, modifySchema(schema, tableSchemaStr))

      if( ! insAccum.isZero ) {
        val sqlText = sqlStart +
          " from new com.splicemachine.derby.vti.KafkaVTI('" + topicName + topicSuffix(ptnInfo, rdd.getNumPartitions) + "') " +
          "as SDVTI (" + tableSchemaStr + ") where "
        val whereClause = keys.map(x => schemaTableName + "." + quoteIdentifier(x) +
          " = SDVTI." ++ quoteIdentifier(x)).mkString(" AND ")
        val combinedText = sqlText + whereClause + ")"

        trace(s"SMC.modifyOnKeys sql $combinedText")
        executeUpdate(combinedText)
      }
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
  def update(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = /* if( !rdd.isEmpty ) */ {
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
   * Rows in dataFrame whose primary key is not in schemaTableName will be inserted into the table;
   *  rows in dataFrame whose primary key is in schemaTableName will be used to update the table.
   *
   * This implementation differs from upsert in a way that allows triggers to work.
   *
   * @param dataFrame
   * @param schemaTableName
   */
  def mergeInto(dataFrame: DataFrame, schemaTableName: String): Unit =
    mergeInto(dataFrame.rdd, dataFrame.schema, schemaTableName)

  /**
   * Rows in rdd whose primary key is not in schemaTableName will be inserted into the table;
   *  rows in rdd whose primary key is in schemaTableName will be used to update the table.
   *
   * This implementation differs from upsert in a way that allows triggers to work.
   *
   * @param rdd
   * @param schema
   * @param schemaTableName
   */
  def mergeInto(rdd: JavaRDD[Row], schema: StructType, schemaTableName: String): Unit = if( rdd.getNumPartitions > 0 ) {
    val keys = primaryKeys(schemaTableName)
    if (keys.length == 0)
      throw new UnsupportedOperationException(s"$schemaTableName has no Primary Key, Required for the Table to Perform MergeInto")

    val topicName = if( rdd.getNumPartitions == insertTopicPartitions ) {
      kafkaTopics.create()
    } else {
      kafkaTopics.createTopic(rdd.getNumPartitions)
    }

    try {
      insAccum.reset
      val tableSchemaStr = schemaString( columnInfo(schemaTableName), schema )
      val ptnInfo = sendData(topicName, rdd, modifySchema(schema, tableSchemaStr))

      if( ! insAccum.isZero ) {
        val insColumnList = columnList(schema)
        val selColumnList = s"SpliceDatasetVTI.${insColumnList.replaceAll(",",",SpliceDatasetVTI.")}"
        val joinClause = keys.map( k => s"target.$k = SpliceDatasetVTI.$k" ).mkString(" and ")  // " a.pk0 = b.pk0 and a.pk1 = b.pk1 and ..."
        val insertText = "insert into " + schemaTableName + " (" + insColumnList + ") select " + selColumnList + " from " +
          "new com.splicemachine.derby.vti.KafkaVTI('" + topicName + topicSuffix(ptnInfo, rdd.getNumPartitions) + "') " +
          "as SpliceDatasetVTI (" + tableSchemaStr + ") left join " + schemaTableName + " as target on " +
          joinClause + " where target."+ keys(0) + " is null"

        val nonKeys = schema.fieldNames.filter((p: String) => keys.indexOf(p) == -1)
        val updColumnList = SpliceJDBCUtil.listColumns(nonKeys)

        val updateText = "update " + schemaTableName +
          " set (" + updColumnList + ") = (" +
          "select " + updColumnList +
          " from new com.splicemachine.derby.vti.KafkaVTI('" + topicName + topicSuffix(ptnInfo, rdd.getNumPartitions) + "') " +
          "as SDVTI (" + tableSchemaStr + ") where "
        val whereClause = keys.map(x => schemaTableName + "." + quoteIdentifier(x) +
          " = SDVTI." ++ quoteIdentifier(x)).mkString(" AND ")
        val combinedUpdText = updateText + whereClause + ")"
        
        executeUpdate(combinedUpdText)  // update
        executeUpdate(insertText)       // insert
      }
    } finally {
      kafkaTopics.delete(topicName)
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
   * If triggers fail when calling upsert, use the mergeInto function instead of upsert.
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
   * If triggers fail when calling upsert, use the mergeInto function instead of upsert.
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


  private[this] def export(dataFrame: DataFrame, exportCmd: String): Unit = if( dataFrame.rdd.getNumPartitions > 0 ) {
    if( dataFrame.isEmpty ) {
      throw new IllegalArgumentException( "Dataframe is empty." )
    }

    val topicName = if( dataFrame.rdd.getNumPartitions == insertTopicPartitions ) {
      kafkaTopics.create()
    } else {
      kafkaTopics.createTopic(dataFrame.rdd.getNumPartitions)
    }
    trace( s"SMC.export topic $topicName" )
    try {
      val schema = dataFrame.schema
      val ptnInfo = sendData(topicName, dataFrame.rdd, schema)

      val sqlText = exportCmd + s" select " + columnList(schema) + " from " +
        s"new com.splicemachine.derby.vti.KafkaVTI('"+topicName+topicSuffix(ptnInfo, dataFrame.rdd.getNumPartitions)+s"') as SpliceDatasetVTI (${schemaStringWithoutNullable(schema, url)})"
      trace( s"SMC.export sql $sqlText" )
      execute(sqlText)
    } finally {
      kafkaTopics.delete(topicName)
    }
  }
}
