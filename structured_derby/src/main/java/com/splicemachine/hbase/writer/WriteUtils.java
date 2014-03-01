package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.table.SpliceHTableUtil;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.*;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class WriteUtils {


    private static Comparator<BulkWrite> writeComparator = new Comparator<BulkWrite>() {
        @Override
        public int compare(BulkWrite o1, BulkWrite o2) {
            if(o1==null) {
                if(o2==null) return 1;
                else return -1;
            }else if(o2==null)
                return 1;

            else return Bytes.compareTo(o1.getRegionKey(),o2.getRegionKey());
        }
    };

    public static boolean bucketWrites(ObjectArrayList<KVPair> buffer,List<BulkWrite> buckets) throws Exception{
        //make sure regions are in sorted order
        Collections.sort(buckets, writeComparator);

        Object[] buffers = buffer.buffer;
        int iBuffer = buffer.size();
        for (int i = 0; i<iBuffer;i++) {
        	KVPair kv = (KVPair) buffers[i];
            byte[] row = kv.getRow();
            boolean less;
            Iterator<BulkWrite> bucketList = buckets.listIterator();
            BulkWrite bucket = null;
            //we know this iterator has at least one region, otherwise we would have exploded
            do{
                BulkWrite next = bucketList.next();
                int compare = BytesUtil.startComparator.compare(next.getRegionKey(), row);
                less = compare<0;
                if(compare==0||less){
                    bucket = next;
                }
            }while(bucketList.hasNext() && less);

            if(bucket!=null)
                bucket.addWrite(kv);
            else
                return false;
        }

        return true;
    }

    public static long getWaitTime(int tryNum,long pause) {
        //refactored to make use of this method elsewhere as well.
        return SpliceHTableUtil.getWaitTime(tryNum,pause);
    }
}
