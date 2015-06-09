package com.splicemachine.derby.stream.utils;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.stream.partitioner.MergePartitioner;
import com.splicemachine.mrio.api.core.SMSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.rdd.NewHadoopPartition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jleach on 6/9/15.
 */
public class StreamPartitionUtils {

    public Partitioner getPartitioner(int[] formatIds, Partition[] partitions) {
        List<byte[]> splits = new ArrayList<>();
        for (Partition p : partitions) {
            assert p instanceof NewHadoopPartition;
            NewHadoopPartition nhp = (NewHadoopPartition) p;
            InputSplit is = nhp.serializableHadoopSplit().value();
            assert is instanceof SMSplit;
            SMSplit ss = (SMSplit) is;
            splits.add(ss.getSplit().getEndRow());
        }
        Collections.sort(splits, BytesUtil.endComparator);

        return new MergePartitioner(splits, formatIds);
    }
}
