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
 *
 */

package org.apache.spark.sql

/**
 * This is copied from org.apache.spark.sql.Dataset.withColumns() which was introduced in Spark 2.3 and is not available
 * in Spark 2.2
 */
object WithColumns {

  def withColumns(colNames: Seq[String], cols: Seq[Column], ds: DataFrame): DataFrame = {
    val resolver = ds.sparkSession.sessionState.analyzer.resolver
    val output = ds.queryExecution.analyzed.output

    val columnMap = colNames.zip(cols).toMap

    val replacedAndExistingColumns = output.map { field =>
      columnMap.find { case (colName, _) =>
        resolver(field.name, colName)
      } match {
        case Some((colName: String, col: Column)) => col.as(colName)
        case _ => Column(field)
      }
    }

    val newColumns = columnMap.filter { case (colName, col) =>
      !output.exists(f => resolver(f.name, colName))
    }.map { case (colName, col) => col.as(colName) }

    ds.select(replacedAndExistingColumns ++ newColumns : _*)
  }
}
