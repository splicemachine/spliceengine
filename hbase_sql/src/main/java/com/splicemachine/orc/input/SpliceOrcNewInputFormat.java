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

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.splicemachine.orc.predicate.SpliceORCPredicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.Warehouse;
import com.splicemachine.orc.metadata.ColumnStatistics;
import org.apache.hadoop.hive.ql.io.orc.OrcNewSplit;
import org.apache.hadoop.hive.ql.io.orc.SpliceOrcUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTimeZone;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 *
 *
 */
public class SpliceOrcNewInputFormat extends InputFormat<NullWritable,Row>
        implements DataSourceRegister {
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.getDefault();
    public static final String SPARK_STRUCT ="com.splicemachine.spark.struct";
    public static final String SPLICE_PREDICATE ="com.splicemachine.predicate";
    public static final String SPLICE_PARTITIONS ="com.splicemachine.partitions";
    public static final String SPLICE_COLUMNS ="com.splicemachine.columns";
    public static final String MAX_MERGE_DISTANCE ="com.splicemachine.orc.maxMergeDistance";
    public static final String MAX_READ_SIZE ="com.splicemachine.orc.maxReadSize";
    public static final String STREAM_BUFFER_SIZE ="com.splicemachine.orc.streamBufferSize";
    public static final double MAX_MERGE_DISTANCE_DEFAULT = 1;
    public static final double MAX_READ_SIZE_DEFAULT = 8;
    public static final double STREAM_BUFFER_SIZE_DEFAULT = 8;
    public static final long DEFAULT_PARTITION_SIZE = 10000;
    public static final String SPLICE_COLLECTSTATS ="com.splicemachine.collectstats";


    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        Configuration configuration = jobContext.getConfiguration();

        // Filter Based On Partition Logic
        List<InputSplit> inputSplits = SpliceOrcUtils.getSplits(jobContext);
        final List<Integer> partitions = getReadColumnIDs(SPLICE_PARTITIONS,jobContext.getConfiguration());
        final List<Integer> columns = getReadColumnIDs(SPLICE_COLUMNS,jobContext.getConfiguration());
        final StructType structType = getRowStruct(configuration);
        final SpliceORCPredicate orcPredicate = getSplicePredicate(configuration);
        boolean isCollectStats = !(configuration.get(SPLICE_COLLECTSTATS, "").isEmpty());

        try {
            // Predicate Pruning...
            return Lists.newArrayList(Iterables.filter(inputSplits,
                    new Predicate<InputSplit>() {
                        @Override
                        public boolean apply(@Nullable InputSplit s) {
                            try {
                                List<String> values = Warehouse.getPartValuesFromPartName(((OrcNewSplit) s).getPath().toString());
                                Map<Integer,ColumnStatistics> columnStatisticsMap = SpliceORCPredicate.partitionStatsEval(columns,structType,partitions,values.toArray(new String[values.size()]), isCollectStats);
                                return orcPredicate.matches(10000,columnStatisticsMap);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }));
        } catch (Exception e) {
            throw new IOException(e);
        }


    }

    @Override
    public RecordReader<NullWritable, Row> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        OrcMapreduceRecordReader reader = new OrcMapreduceRecordReader();
        reader.initialize(inputSplit,taskAttemptContext);
        return reader;
    }

    @Override
    public String shortName() {
        return "sorc";
    }

    public static StructType getRowStruct(Configuration configuration) throws IOException {
        String sparkStruct = configuration.get(SPARK_STRUCT);
        if (sparkStruct == null)
            throw new IOException("Spark struct not passed in configuration, please set "+ SPARK_STRUCT);
        return (StructType) StructType.fromJson(sparkStruct);
    }

    public static SpliceORCPredicate getSplicePredicate(Configuration configuration) throws IOException {
        String base64Pred = configuration.get(SPLICE_PREDICATE);
        if (base64Pred == null)
            throw new IOException("Splice predicates not passed in configuration, please set "+ SPLICE_PREDICATE);
        return SpliceORCPredicate.deserialize(base64Pred);
    }

    public static List<Integer> getColumnIds(Configuration configuration) throws IOException {
        return getReadColumnIDs(SPLICE_COLUMNS,configuration);
    }

    public static List<Integer> getPartitionIds(Configuration configuration) throws IOException {
        return getReadColumnIDs(SPLICE_PARTITIONS,configuration);
    }


    public static Map<Integer,DataType> getColumnsAndTypes(List<Integer> columnIds, StructType rowStruct) throws IOException {
        int structTypeSize = rowStruct.size();
        int columnIdsSize = columnIds.size();
        Map columnsAndTypes = new HashMap<>();
        for (int i = 0,j = 0; i < columnIdsSize; i++) {
            if (columnIds.get(i) == -1)
                continue;
            columnsAndTypes.put(i,rowStruct.fields()[j]);
            j++;
        }
        if (columnsAndTypes.size() != structTypeSize)
            throw new IOException(String.format("Column IDS do not match the underlying struct columnIds(%s), struct(%s)",columnIds,rowStruct.json()));
        return columnsAndTypes;
    }

    public static List<Integer> getReadColumnIDs(String confString, Configuration conf) {
        String skips = conf.get(confString, "");
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
