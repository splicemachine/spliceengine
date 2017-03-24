package com.splicemachine.orc.input;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.splicemachine.orc.*;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.OrcMetadataReader;
import com.splicemachine.orc.predicate.SpliceORCPredicate;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewSplit;
import org.apache.hadoop.hive.ql.io.orc.SpliceOrcUtils;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Created by jleach on 3/21/17.
 */
public class OrcMapreduceRecordReader extends RecordReader<NullWritable,Row> {
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.UTC;
    public static final String SPARK_STRUCT ="com.splicemachine.spark.struct";
    public static final String SPLICE_PREDICATE ="com.splicemachine.predicate";

    public static final String MAX_MERGE_DISTANCE ="com.splicemachine.orc.maxMergeDistance";
    public static final String MAX_READ_SIZE ="com.splicemachine.orc.maxReadSize";
    public static final String STREAM_BUFFER_SIZE ="com.splicemachine.orc.streamBufferSize";
    public static final double MAX_MERGE_DISTANCE_DEFAULT = 64;
    public static final double MAX_READ_SIZE_DEFAULT = 64;
    public static final double STREAM_BUFFER_SIZE_DEFAULT = 64;


    OrcRecordReader orcRecordReader;
    private StructType structType;
    private ColumnarBatch columnarBatch;
    private Iterator<ColumnarBatch.Row> currentIterator;
    private SpliceORCPredicate spliceORCPredicate;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        OrcNewSplit orcNewSplit = (OrcNewSplit) inputSplit;
        Configuration configuration = taskAttemptContext.getConfiguration();
        double maxMergeDistance = configuration.getDouble(MAX_MERGE_DISTANCE,MAX_MERGE_DISTANCE_DEFAULT);
        double maxReadSize = configuration.getDouble(MAX_READ_SIZE,MAX_READ_SIZE_DEFAULT);
        double streamBufferSize = configuration.getDouble(STREAM_BUFFER_SIZE,STREAM_BUFFER_SIZE_DEFAULT);

        List<Integer> columnIds = ColumnProjectionUtils.getReadColumnIDs(configuration);
        Path path = orcNewSplit.getPath();
        FileSystem fileSystem = FileSystem.get(path.toUri(),configuration);
        long size = fileSystem.getFileStatus(path).getLen();
        FSDataInputStream inputStream = fileSystem.open(path);
        OrcDataSource orcDataSource = new HdfsOrcDataSource(path.toString(), size, new DataSize(maxMergeDistance, DataSize.Unit.MEGABYTE),
                new DataSize(maxReadSize, DataSize.Unit.MEGABYTE),
                new DataSize(streamBufferSize, DataSize.Unit.MEGABYTE), inputStream);
        OrcReader orcReader = new OrcReader(orcDataSource, new OrcMetadataReader(), new DataSize(maxMergeDistance, DataSize.Unit.MEGABYTE),
                new DataSize(maxReadSize, DataSize.Unit.MEGABYTE));
        orcRecordReader = orcReader.createRecordReader(getColumnsAndTypes(configuration),
                spliceORCPredicate, HIVE_STORAGE_TIME_ZONE, new AggregatedMemoryContext());
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
        List<Integer> columnIds = getReadColumnIDs(configuration);
        String sparkStruct = configuration.get(SPARK_STRUCT);
        String base64Pred = configuration.get(SPLICE_PREDICATE);


        if (sparkStruct == null)
            throw new IOException("Spark struct not passed in configuration, please set "+ SPARK_STRUCT);
        if (base64Pred == null)
            throw new IOException("Splice predicates not passed in configuration, please set "+ SPLICE_PREDICATE);
        spliceORCPredicate = SpliceORCPredicate.deserialize(base64Pred);
        structType = (StructType) StructType.fromJson(sparkStruct);
        int structTypeSize = structType.size();
        int columnIdsSize = columnIds.size();
        Map columnsAndTypes = new HashMap<>();
        for (int i = 0,j = 0; i < columnIdsSize; i++) {
            if (columnIds.get(i) == -1)
                continue;
            columnsAndTypes.put(i,structType.fields()[j]);
            j++;
        }
        if (columnsAndTypes.size() != structTypeSize)
            throw new IOException(String.format("Column IDS do not match the underlying struct columnIds(%s), struct(%s)",columnIds,structType.json()));
        return columnsAndTypes;
    }

    public static List<Integer> getReadColumnIDs(Configuration conf) {
        String skips = conf.get("hive.io.file.readcolumn.ids", "");
        String[] list = StringUtils.split(skips);
        ArrayList result = new ArrayList(list.length);
        String[] arr$ = list;
        int len$ = list.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            String element = arr$[i$];
            Integer toAdd = Integer.valueOf(Integer.parseInt(element));
            result.add(toAdd);
        }

        return result;
    }


}
