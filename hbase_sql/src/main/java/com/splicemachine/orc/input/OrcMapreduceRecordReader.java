package com.splicemachine.orc.input;

import com.splicemachine.orc.*;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.OrcMetadataReader;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewSplit;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTimeZone;
import scala.Function1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by jleach on 3/21/17.
 */
public class OrcMapreduceRecordReader extends RecordReader<NullWritable,Row> {
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.UTC;
    public static final String SPARK_STRUCT ="com.splicemachine.spark.struct";
    OrcRecordReader orcRecordReader;
    private StructType structType;
    private ColumnarBatch columnarBatch;
    private Iterator<ColumnarBatch.Row> currentIterator;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        OrcNewSplit orcNewSplit = (OrcNewSplit) inputSplit;
        List<Integer> columnIds = ColumnProjectionUtils.getReadColumnIDs(taskAttemptContext.getConfiguration());
        Path path = orcNewSplit.getPath();
        FileSystem fileSystem = FileSystem.get(path.toUri(),taskAttemptContext.getConfiguration());
        long size = fileSystem.getFileStatus(path).getLen();
        FSDataInputStream inputStream = fileSystem.open(path);
        OrcDataSource orcDataSource = new HdfsOrcDataSource(path.toString(), size, new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE), inputStream);
        OrcReader orcReader = new OrcReader(orcDataSource, new OrcMetadataReader(), new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE));
        orcRecordReader = orcReader.createRecordReader(getColumnsAndTypes(taskAttemptContext.getConfiguration()),
                OrcPredicate.TRUE, HIVE_STORAGE_TIME_ZONE, new AggregatedMemoryContext());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while(currentIterator == null || !currentIterator.hasNext()) {
            if (orcRecordReader.nextBatch() == -1)
                return false;
            columnarBatch = orcRecordReader.getColumnarBatch(structType);
            currentIterator = columnarBatch.rowIterator();
        }
        return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Row getCurrentValue() throws IOException, InterruptedException {
        return new ColumnarBatchRow(currentIterator.next(),structType);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return orcRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        orcRecordReader.close();
    }

    public Map getColumnsAndTypes(Configuration configuration) throws IOException {
        List<Integer> columnIds = ColumnProjectionUtils.getReadColumnIDs(configuration);
        String sparkStruct = configuration.get(SPARK_STRUCT);
        if (sparkStruct == null)
            throw new IOException("Spark struct not passed in configuration, please set "+ SPARK_STRUCT);
        structType = (StructType) StructType.fromJson(sparkStruct);
        int size = structType.size();
        if (size != columnIds.size())
            throw new IOException(String.format("Column IDS do not match the underlying struct columnIds(%s), struct(%s)",columnIds,structType.json()));
        Map columnsAndTypes = new HashMap<>();
        for (int i = 0; i < size; i++) {
            columnsAndTypes.put(columnIds.get(i),structType.fields()[i]);
        }
        return columnsAndTypes;
    }

}
