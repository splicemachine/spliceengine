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

package com.splicemachine.stream.output;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.stream.control.output.ParquetWriterFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public class ParquetWriterFactoryImpl implements ParquetWriterFactory{
    
    @Override
    public RecordWriter<Void, Object> getParquetRecordWriter(String location, String compression, StructType tableSchema) throws IOException, InterruptedException {
        ParquetWriteSupport pws = new ParquetWriteSupport();
        final Configuration conf = new Configuration((Configuration) EngineDriver.driver().getConfiguration().getConfigSource().unwrapDelegate());
        conf.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT().key(), "false");
        conf.set("spark.sql.parquet.int64AsTimestampMillis", "false");
        conf.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), "true");
        conf.set(SQLConf.PARQUET_BINARY_AS_STRING().key(), "false");

        pws.setSchema(tableSchema, conf);
        return new ParquetOutputFormat(new ParquetWriteSupport()) {
            @Override
            public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
                return new Path(location+"/part-r-00000"+extension);
            }

            @Override
            public RecordWriter<Void, Object> getRecordWriter(TaskAttemptContext taskAttemptContext)
                    throws IOException, InterruptedException {

                CompressionCodecName codec;
                switch (compression) {
                    case "none":
                        codec = CompressionCodecName.UNCOMPRESSED;
                        break;
                    case "snappy":
                        codec = CompressionCodecName.SNAPPY;
                        break;
                    case "lzo":
                        codec = CompressionCodecName.LZO;
                        break;
                    case "gzip":
                    case "zip":
                        codec = CompressionCodecName.GZIP;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown compression: " + compression);
                }
                String extension = codec.getExtension() + ".parquet";
                Path file = getDefaultWorkFile(taskAttemptContext, extension);
                return getRecordWriter(conf, file, codec);
            }
        }.getRecordWriter(null);
    }

    @Override
    public InternalRow encodeToRow(StructType tableSchema, ValueRow valueRow, ExpressionEncoder<Row> encoder) {
        return encoder.toRow(valueRow);
    }
}
