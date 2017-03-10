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

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

class DefaultSource extends RelationProvider with CreatableRelationProvider
  with SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]):
  BaseRelation = { null }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame):
  BaseRelation = { null }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType):
  BaseRelation = { new SpliceRelation(sqlContext,parameters,schema)  }

  class SpliceRelation(override val sqlContext: SQLContext, parameters : Map[String, String], userSchema : StructType )
    extends BaseRelation with InsertableRelation with Serializable {

    override def schema: StructType = {
      if (userSchema != null) {
        userSchema
      } else {
        StructType(
          StructField("id",IntegerType,false) ::Nil
        )
      }
    }

    override def insert(data: _root_.org.apache.spark.sql.DataFrame, overwrite: Boolean): Unit = ???
  }

}
