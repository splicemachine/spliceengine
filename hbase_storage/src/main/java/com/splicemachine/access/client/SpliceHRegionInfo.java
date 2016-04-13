package com.splicemachine.access.client;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;

/**
 * Created by jleach on 4/12/16.
 */
public class SpliceHRegionInfo extends HRegionInfo {

    public SpliceHRegionInfo(HRegionInfo info) {
        super(info);
    }

    @Override
    /**
     * @return Comparator to use comparing {@link org.apache.hadoop.hbase.KeyValue}s.
     */
    public KeyValue.KVComparator getComparator() {
        return SpliceKVComparator.INSTANCE;
    }

}