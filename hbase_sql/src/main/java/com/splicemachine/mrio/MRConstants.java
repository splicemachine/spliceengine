/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.mrio;

public class MRConstants {
	public final static String SPLICE_TRANSACTION_ID = "transaction.id";
	final public static String SPLICE_TABLE_NAME = "splice.tableName";
    final public static String SPLICE_INPUT_TABLE_NAME="splice.input.tableName";
    final public static String SPLICE_OUTPUT_TABLE_NAME="splice.output.tableName";
    final public static String SPLICE_CONGLOMERATE = "splice.conglomerate";
    final public static String SPLICE_INPUT_CONGLOMERATE = "splice.input.conglomerate";
    final public static String SPLICE_OUTPUT_CONGLOMERATE = "splice.output.conglomerate";
	final public static String SPLICE_WRITE_BUFFER_SIZE = "splice.write.buffer.size";
	final public static String SPLICE_JDBC_STR = "splice.jdbc";
	final public static String ONE_SPLIT_PER_REGION = "one.split.per.region";
    final public static String SPLICE_SCAN_INFO = "splice.scan.info";
    final public static String SPLICE_OPERATION_CONTEXT = "splice.operation.context";
    final public static String SPLICE_TXN_MIN_TIMESTAMP = "splice.txn.timestamp.min";
    final public static String SPLICE_TXN_MAX_TIMESTAMP = "splice.txn.timestamp.max";
    final public static String SPLICE_TXN_DEST_TABLE = "splice.txn.destination.table";
    final public static String HBASE_OUTPUT_TABLE_NAME = "hbase_output_tableName";
	final public static String SPLICE_SCAN_MEMSTORE_ONLY="MR";
    // final public static String SPLICE_TBLE_CONTEXT="splice.tableContext";
    final public static String TABLE_WRITER = "table.writer";
    final public static String TABLE_WRITER_TYPE = "table.writer.type";
    final public static String REGION_LOCATION = "region.location";
    final public static String COMPACTION_FILES = "compaction.files";
}