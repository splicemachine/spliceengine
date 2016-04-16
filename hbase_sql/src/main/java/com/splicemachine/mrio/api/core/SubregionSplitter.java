package com.splicemachine.mrio.api.core;

import com.splicemachine.si.impl.HMissedSplit;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.InputSplit;
import java.util.List;

/**
 * Created by dgomezferro on 3/13/15.
 *
 * Used to compute a list of splits for a given table smaller than regions
 */
public interface SubregionSplitter {
    List<InputSplit> getSubSplits(Table table, List<Partition> splits) throws HMissedSplit;
}
