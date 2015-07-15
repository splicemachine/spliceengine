package com.splicemachine.derby.impl.job.scheduler;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.mapreduce.InputSplit;
import java.util.List;

/**
 * Created by dgomezferro on 3/13/15.
 *
 * Used to compute a list of splits for a given table smaller than regions
 */
public interface SubregionSplitter {
    List<InputSplit> getSubSplits(HTableInterface table, List<InputSplit> splits);
}
