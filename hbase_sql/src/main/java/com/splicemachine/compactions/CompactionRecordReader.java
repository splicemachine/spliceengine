/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.mrio.MRConstants;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jyuan on 3/24/16.
 */
public class CompactionRecordReader extends RecordReader<Integer, Iterator> {

    private Integer currentKey;
    private Iterator<String> currentValue;
    private Configuration conf;
    private List<String> files;
    private boolean consumed;
    public CompactionRecordReader(Configuration conf) {
        this.conf = conf;
        this.consumed = false;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        String fileString = conf.get(MRConstants.COMPACTION_FILES);
        files = SerializationUtils.deserialize(Base64.decodeBase64(fileString));
        currentKey = new Integer(0);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!consumed) {
            currentKey = new Integer(1);
            currentValue = files.iterator();
            consumed = true;

            return true;
        }
        else {
            currentKey = null;
            currentValue = null;

            return false;
        }
    }

    @Override
    public Integer getCurrentKey() throws IOException, InterruptedException {

        return currentKey;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public Iterator<String> getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

}
