package org.apache.spark.sql.execution.datasources.jdbc

import com.splicemachine.spark2.splicemachine.{SpliceJDBCOptions, SpliceJDBCUtil, SplicemachineContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

/**
  * Created by jleach on 4/7/17.
  */

@SuppressFBWarnings(value = Array("SE_NO_SERIALVERSIONID","SE_TRANSIENT_FIELD_NOT_RESTORED", "NP_ALWAYS_NULL"), justification = "DB-9846")
case class SpliceRelation2(jdbcOptions: JdbcOptionsInWrite)(@transient val sqlContext: SQLContext, @transient var userSchema: Option[StructType]) extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  import scala.collection.JavaConverters._
  private val context: SplicemachineContext = new SplicemachineContext(Map() ++ jdbcOptions.asProperties.asScala)

  override val needConversion: Boolean = true

  // Check if JDBCRDD.compileFilter can accept input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(JDBCRDD.compileFilter(_, JdbcDialects.get(jdbcOptions.url)).isEmpty)
  }



  /**
    * Generates a SparkSQL schema object so SparkSQL knows what is being
    * provided by this BaseRelation.
    *
    * @return schema generated from the Splice Machine table's schema
    */
  override def schema: StructType = {
    userSchema match {
      case Some(x) =>
        userSchema.get
      case None =>
        context.getSchema(jdbcOptions.table)
    }
  }


  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    context.insert(data,jdbcOptions.table)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    val columnList = SpliceJDBCUtil.listColumns(requiredColumns)
    val myWhereClause = SpliceJDBCUtil.filterWhereClause(jdbcOptions.url,filters)
    val sqlText =
      if (myWhereClause.length > 0)
        s"SELECT $columnList FROM ${jdbcOptions.table} WHERE $myWhereClause"
      else
        s"SELECT $columnList FROM ${jdbcOptions.table}"

    val internal = jdbcOptions.asProperties.getProperty(SpliceJDBCOptions.JDBC_INTERNAL_QUERIES, "false").toBoolean

    val dataFrame = if (internal) context.internalDf(sqlText) else context.df(sqlText)

    userSchema = Option.apply(dataFrame.schema)
    dataFrame.rdd
  }

}
