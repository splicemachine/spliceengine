package com.splicemachine.si.impl.region;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.Source;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class HBaseTxnFinder implements TxnFinder {

    private HRegion region;

    public HBaseTxnFinder(HRegion region) {
        this.region = region;
    }

    private class ScanTimestampIterator implements Source<Pair<Long, Long>> {
        private final RegionScanner regionScanner;
        protected Pair<Long, Long> next;
        private List<Cell> currentResults;

        protected Pair<Long, Long> decode(List<Cell> keyValues) throws IOException{
            if(keyValues.size()<=0) return null;
            Cell dataKv=null;
            long ts = 0;

            for(Cell kv : keyValues){
                if(CellUtils.singleMatchingColumn(kv,V2TxnDecoder.FAMILY,V2TxnDecoder.DATA_QUALIFIER_BYTES)) {
                    dataKv=kv;
                }
                if(ts <  kv.getTimestamp()) {
                    ts = kv.getTimestamp(); // get last update
                }
            }
            if(dataKv==null) return null;
            long txnId= TxnUtils.txnIdFromRowKey(dataKv.getRowArray(),dataKv.getRowOffset(),dataKv.getRowLength());
            return new Pair<>(txnId, ts);
        }


        public ScanTimestampIterator(RegionScanner scanner){
            this.regionScanner=scanner;
        }

        @Override
        public boolean hasNext() throws IOException{
            if(next!=null) return true;
            if(currentResults==null)
                currentResults=new ArrayList<>(10);
            boolean shouldContinue;
            do{
                shouldContinue=regionScanner.next(currentResults);
                if(currentResults.size()<=0) return false;

                this.next = decode(currentResults);
                currentResults.clear();
            }while(next==null && shouldContinue);

            return next!=null;
        }

        @Override
        public Pair<Long, Long> next() throws IOException{
            if(!hasNext()) throw new NoSuchElementException();
            Pair<Long, Long> n=next;
            next=null;
            return n;
        }

        @Override
        public void close() throws IOException{
            regionScanner.close();
        }
    }

    @Override
    public Pair<Long, Long> find(byte bucket, byte[] begin, boolean reverse) throws IOException {
        Scan s = new Scan();
        byte[] arr = {bucket};
        s.setReversed(reverse).setFilter(new PrefixFilter(arr)); // todo get one row only

        if(begin != null) {
            s.withStartRow(begin);
        }
        HBaseTxnFinder.ScanTimestampIterator si = new HBaseTxnFinder.ScanTimestampIterator(region.getScanner(s));
        if(si.hasNext()) {
            return si.next;
        } else {
            return null;
        }
    }
}
