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
package com.splicemachine.orc.input;

import com.splicemachine.orc.HdfsOrcDataSource;
import com.splicemachine.orc.OrcDataSource;
import com.splicemachine.orc.OrcReader;
import com.splicemachine.orc.OrcRecordReader;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.OrcMetadataReader;
import com.splicemachine.orc.predicate.SpliceORCPredicate;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.io.orc.OrcNewSplit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.splicemachine.orc.input.SpliceOrcNewInputFormat.*;

/**
 *
 *
 */
public class OrcMapreduceRecordReader extends RecordReader<NullWritable,Row> {
    OrcRecordReader orcRecordReader;
    private ColumnarBatch columnarBatch;
    private Iterator<InternalRow> currentIterator;
    private StructType rowStruct;
    private SpliceORCPredicate predicate;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        OrcNewSplit orcNewSplit = (OrcNewSplit) inputSplit;
        Configuration configuration = taskAttemptContext.getConfiguration();
        double maxMergeDistance = configuration.getDouble(MAX_MERGE_DISTANCE,MAX_MERGE_DISTANCE_DEFAULT);
        double maxReadSize = configuration.getDouble(MAX_READ_SIZE,MAX_READ_SIZE_DEFAULT);
        double streamBufferSize = configuration.getDouble(STREAM_BUFFER_SIZE,STREAM_BUFFER_SIZE_DEFAULT);
        Path path = orcNewSplit.getPath();
        FileSystem fileSystem = FileSystem.get(path.toUri(),configuration);
        long size = fileSystem.getFileStatus(path).getLen();
        FSDataInputStream inputStream = fileSystem.open(path);
        rowStruct = getRowStruct(configuration);
        predicate = getSplicePredicate(configuration);
        List<Integer> partitions = getPartitionIds(configuration);
        List<Integer> columnIds = getColumnIds(configuration);



        List<String> values = null;
        try {
            values = Warehouse.getPartValuesFromPartName(((OrcNewSplit) inputSplit).getPath().toString());
        } catch (MetaException me) {
            throw new IOException(me);
        }
        OrcDataSource orcDataSource = new HdfsOrcDataSource(path.toString(), size, new DataSize(maxMergeDistance, DataSize.Unit.MEGABYTE),
                new DataSize(maxReadSize, DataSize.Unit.MEGABYTE),
                new DataSize(streamBufferSize, DataSize.Unit.MEGABYTE), inputStream);
        OrcReader orcReader = new OrcReader(orcDataSource, new OrcMetadataReader(), new DataSize(maxMergeDistance, DataSize.Unit.MEGABYTE),
                new DataSize(maxReadSize, DataSize.Unit.MEGABYTE));
        orcRecordReader =
            orcReader.createRecordReader(getColumnsAndTypes(columnIds, rowStruct),
                                         predicate, orcNewSplit.getStart(), orcNewSplit.getLength(),
                                         HIVE_STORAGE_TIME_ZONE, new AggregatedMemoryContext(),
                                         partitions, values);
    
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while(currentIterator == null || !currentIterator.hasNext()) {
            if (orcRecordReader.nextBatch() == -1)
                return false;
            columnarBatch = orcRecordReader.getColumnarBatch(rowStruct);
            final Iterator<? extends InternalRow> iter = columnarBatch.rowIterator();
            currentIterator = new Iterator<InternalRow>() {
                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public InternalRow next() {
                    return iter.next();
                }
            };
        }
        return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Row getCurrentValue() throws IOException, InterruptedException {
        return new ColumnarBatchRow(currentIterator.next(),rowStruct);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return orcRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        orcRecordReader.close();
    }

}
