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
