package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/14/14
 */
public class FilteredRowKeyDistributor extends RowKeyDistributor {
    private final RowKeyDistributor delegate;
    private final boolean[] filterMask;

    public FilteredRowKeyDistributor(RowKeyDistributor delegate, boolean[] filterMask) {
        this.delegate = delegate;
        this.filterMask = filterMask;
    }

    @Override public byte[] getDistributedKey(byte[] originalKey) { return delegate.getDistributedKey(originalKey); }
    @Override public byte[] getOriginalKey(byte[] adjustedKey) { return delegate.getOriginalKey(adjustedKey); }
    @Override public byte[][] getAllDistributedKeys(byte[] originalKey) { return delegate.getAllDistributedKeys(originalKey); }

    private static final Comparator<Pair<byte[], byte[]>> pairComparator = new Comparator<Pair<byte[], byte[]>>() {
        @Override
        public int compare(Pair<byte[], byte[]> o1, Pair<byte[], byte[]> o2) {
            return Bytes.BYTES_COMPARATOR.compare(o1.getFirst(),o2.getFirst());
        }
    };
    @Override
    @SuppressWarnings("unchecked")
    public Pair<byte[], byte[]>[] getDistributedIntervals(byte[] originalStartKey, byte[] originalStopKey) {
        Pair<byte[],byte[]>[] originalIntervals = delegate.getDistributedIntervals(originalStartKey,originalStopKey);
        Arrays.sort(originalIntervals, pairComparator);
        List<Pair<byte[],byte[]>> filteredIntervals = Lists.newArrayListWithCapacity(originalIntervals.length);
        int filteredSize =0;
        for(int i=0;i<originalIntervals.length;i++){
            if(filterMask[i]){
                filteredIntervals.add(originalIntervals[i]);
                filteredSize++;
            }
        }
        return filteredIntervals.toArray(new Pair[filteredSize]);
    }
}
