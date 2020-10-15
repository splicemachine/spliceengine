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

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils, SpliceRelation2, JDBCOptions, JdbcOptionsInWrite, JDBCRDD}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider with CreatableRelationProvider
  with SchemaRelationProvider {


  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]):
  BaseRelation = {
    new SpliceRelation2(new JdbcOptionsInWrite(parameters))(sqlContext,None)
  }

  /**
    *
    * Creates a relation and inserts data to specified table.
    *
    * @param sqlContext
    * @param mode
    * @param parameters
    * @param df
    * @return
    */

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], df: DataFrame):
  BaseRelation = {
    val jdbcOptions = new JdbcOptionsInWrite(parameters)
    val url = jdbcOptions.url
    val table = jdbcOptions.table
    val createTableOptions = jdbcOptions.createTableOptions
    val isTruncate = jdbcOptions.isTruncate
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      val tableExists = JdbcUtils.tableExists(conn, jdbcOptions)
      if (tableExists) {
        val actualRelation = new SpliceRelation2(new JdbcOptionsInWrite(parameters))(sqlContext,Option.apply(df.schema))

        mode match {
          case SaveMode.Overwrite =>
            if (isTruncate && isCascadingTruncateTable(url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, jdbcOptions)
              actualRelation.insert(df,true)
              actualRelation
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, table, jdbcOptions)
              createTable(conn, df, jdbcOptions)
              actualRelation.insert(df,false)
              actualRelation
            }
          case SaveMode.Append =>
            actualRelation.insert(df,false)
            actualRelation

          case SaveMode.ErrorIfExists =>
            throw new Exception(
              s"Table or view '$table' already exists. SaveMode: ErrorIfExists.")

          case SaveMode.Ignore =>
            actualRelation
          // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
          // to not save the contents of the DataFrame and to not change the existing data.
          // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(conn, df, jdbcOptions)
        val actualRelation = new SpliceRelation2(new JdbcOptionsInWrite(parameters))(sqlContext,Option.apply(df.schema))
        actualRelation.insert(df,false)
        actualRelation
      }
    } finally {
      conn.close()
    }
  }

  /**
    *
    * Creates a relation based on the schema
    *
    * @param sqlContext
    * @param parameters
    * @param schema
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType):
  BaseRelation = {
    new SpliceRelation2(new JdbcOptionsInWrite(parameters))(sqlContext,Option.apply(schema))
  }

}
