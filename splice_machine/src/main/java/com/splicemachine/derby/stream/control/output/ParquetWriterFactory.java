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

package com.splicemachine.derby.stream.control.output;

import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public interface ParquetWriterFactory {
    <K, V> RecordWriter<K, V> getParquetRecordWriter(String location, String compression, StructType tableSchema) throws IOException, InterruptedException;

    InternalRow encodeToRow(StructType tableSchema, ValueRow valueRow, ExpressionEncoder<Row> encoder);
}
