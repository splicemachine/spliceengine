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
    final public static String SPLICE_SAMPLING = "splice.sampling";
    final public static String SPLICE_SPLITS_PER_TABLE = "splice.splits.per.table";
    final public static String SPLICE_SCAN_INFO = "splice.scan.info";
    final public static String SPLICE_CONNECTION_STRING = "splice.connection.string";
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
    final public static String SPLICE_SCAN_INPUT_SPLITS_ID = "splice.scan.input.splits.id";
}
