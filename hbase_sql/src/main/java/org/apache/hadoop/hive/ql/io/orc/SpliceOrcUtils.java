/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jleach on 3/21/17.
 */
public class SpliceOrcUtils {

    public static List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {
        List<OrcSplit> splits =
                OrcInputFormat.generateSplitsInfo(ShimLoader.getHadoopShims()
                        .getConfiguration(jobContext));
        List<InputSplit> result = new ArrayList<InputSplit>(splits.size());
        // Filter Out Splits based on paths...
        for(OrcSplit split: splits) {

            result.add(new OrcNewSplit(split));
        }



        return result;
    }
}