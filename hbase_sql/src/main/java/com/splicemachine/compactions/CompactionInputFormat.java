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
