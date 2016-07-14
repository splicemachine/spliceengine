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

package com.splicemachine.mrio.api.core;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 * This class exists to simply shuffle the splits
 *
 * Created by jleach on 3/17/16.
 */
public class SMTextInputFormat extends TextInputFormat {

    /*
    * Protects Splice Machine in the case of loading ordered data
    *
    * Shuffle is performed in linear time (i.e. slows as number of splits increases)
    *
    */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> inputSplits = super.getSplits(job);
        Collections.shuffle(inputSplits);
        return inputSplits;
    }
}
