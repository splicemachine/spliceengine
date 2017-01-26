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
