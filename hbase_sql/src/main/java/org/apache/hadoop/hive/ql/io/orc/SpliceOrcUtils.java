package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.Context;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jleach on 3/21/17.
 */
public class SpliceOrcUtils {

    public static List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {
        Configuration conf = ShimLoader.getHadoopShims().getConfiguration(jobContext);
        List<OrcSplit> splits = OrcInputFormat.generateSplitsInfo(conf, createContext(conf, -1));
        List<InputSplit> result = new ArrayList<InputSplit>(splits.size());
        for(OrcSplit split: splits) {
            result.add(new OrcNewSplit(split));
        }
        return result;
    }

    // Nearly C/P from OrcInputFormat; there are too many statics everywhere to sort this out.
    private static Context createContext(Configuration conf, int numSplits) throws IOException {
        // Use threads to resolve directories into splits.
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ORC_MS_FOOTER_CACHE_ENABLED)) {
            // Create HiveConf once, since this is expensive.
            conf = new HiveConf(conf, OrcInputFormat.class);
        }
        return new Context(conf, numSplits, null);
    }
}
