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

package com.splicemachine.compactions;

import com.google.common.collect.Lists;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.AbstractSMInputFormat;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jyuan on 3/24/16.
 */
public class CompactionInputFormat extends AbstractSMInputFormat<Integer, Iterator> {

    protected CompactionRecordReader recordReader;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        String regionLocation = conf.get(MRConstants.REGION_LOCATION);
        CompactionSplit split = new CompactionSplit(regionLocation);
        List<InputSplit> splits = Lists.newArrayList();
        splits.add(split);

        return splits;
    }

    @Override
    public RecordReader<Integer, Iterator> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {

        SpliceLogUtils.info(LOG, "createRecordReader for split=%s, context %s", split, context);
        if (recordReader != null)
            return recordReader;
        else {
            recordReader = new CompactionRecordReader(conf);
            return recordReader;
        }
    }
}
