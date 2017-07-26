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