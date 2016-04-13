package com.splicemachine.access.client;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;

/**
 *
 * Splice Machine Custom RegionInfo that allows us to supply a comparator for the client side
 * merge of the StoreFileScanners and MemstoreKeyValueScanner.  This needs to be custom to address
 * the issues with holding the memory scanner open.
 *
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