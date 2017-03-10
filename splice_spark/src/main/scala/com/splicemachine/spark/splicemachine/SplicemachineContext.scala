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

import java.sql.ResultSet
import java.util.Properties

import com.splicemachine.EngineDriver
import com.splicemachine.db.catalog.types.RoutineAliasInfo
import com.splicemachine.db.iapi.sql.conn.StatementContext
import com.splicemachine.db.impl.jdbc.{EmbedResultSet40, EmbedConnection}
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation
import com.splicemachine.derby.impl.SpliceSpark
import com.splicemachine.derby.impl.sql.execute.operations.{LocatedRow, SpliceBaseOperation}
import com.splicemachine.derby.stream.iapi.{OperationContext, DataSetProcessor}
import com.splicemachine.derby.stream.spark.{SparkDataSet, SparkUtils}
import com.splicemachine.tools.EmbedConnectionMaker
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import java.util.Properties

class SplicemachineContext() extends Serializable {

  @transient lazy val internalConnection = {
    SpliceSpark.setupSpliceStaticComponents();
    val engineDriver: EngineDriver = EngineDriver.driver
    assert(engineDriver != null, "Not booted yet!")
    // Create a static statement context to enable nested connections
    val maker: EmbedConnectionMaker = new EmbedConnectionMaker
    val dbProperties: Properties = new Properties;
    maker.createNew(dbProperties);
    dbProperties.put(EmbedConnection.INTERNAL_CONNECTION, "true")
    maker.createNew(dbProperties)
  }


  def tableExists(schemaName: String, tableName: String):
  Boolean = internalConnection.getMetaData.getTables(null,schemaName,tableName,null).next()

  def deleteTable(schemaName: String, tableName: String):
  Integer = internalConnection.prepareStatement(String.format("drop tabble %s.%s",schemaName,tableName)).executeUpdate();

  def splicemachineDataFrame(sql: String): Dataset[Row] = {
    SparkUtils.resultSetToDF(internalConnection.createStatement().executeQuery(sql));
  }
}