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
package com.splicemachine.spark2

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object splicemachine {

  /**
    * Adds a method, `splicemachine`, to DataFrameReader that allows you to read SpliceMachine tables using
    * the DataFrameReader.
    */
  implicit class SplicemachineDataFrameReader(reader: DataFrameReader) {
    def splicemachine: DataFrame = reader.format("com.splicemachine.spark2.splicemachine").load
  }

  /**
    * Adds a method, `splicemachine`, to DataFrameWriter that allows writes to Splicemachine using
    * the DataFileWriter
    */
  implicit class SplicemachineDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def splicemachine = writer.format("com.splicemachine.spark2.splicemachine").save
  }
}