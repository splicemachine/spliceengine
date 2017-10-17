package com.splicemachine.spark.splicemachine
import java.sql._

import org.apache.spark.sql.{DataFrame, ForeachWriter}

/**
  * Created by Monte Zweben on 10/16/17.
  * TBD: Catch exceptions and rollback
  */

class  SpliceSink(url:String, user:String, pwd:String) extends ForeachWriter[DataFrame] {
  var context: SplicemachineContext = _
  var connection:Connection = _
  var schemaTableName: String = _

  def open(partitionId: Long, version: Long): Boolean = {
    context = new SplicemachineContext(s"""$url;user=$user;password=$pwd""")
    connection = context.getConnection()
    connection.setAutoCommit(false)
    true
  }

  def process(value: DataFrame): Unit = {
      context.insert(value,getSchemaTableName())
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.commit()
    connection.close
  }

  def setSchemaTableName(name: String): Unit = {
    schemaTableName = name
  }

  def getSchemaTableName():String = {
    schemaTableName
  }
}

