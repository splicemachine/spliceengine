package com.splicemachine.derby.stream.temporary.update;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

/**
 *
 *
 *
 *
 */
public class ResultSupplier{
    private KeyValue result;
    private byte[] location;
    private byte[] filterBytes;
    private HTableInterface htable;
    private TxnView txnView;
    private long heapConglom;

    public ResultSupplier(BitSet interestedFields,TxnView txnView, long heapConglom) {
        //we need the index so that we can transform data without the information necessary to decode it
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(interestedFields,new ObjectArrayList<Predicate>(),true);
        this.filterBytes = predicateFilter.toBytes();
        this.txnView = txnView;
        this.heapConglom = heapConglom;
    }

    public void setLocation(byte[] location){
        this.location = location;
        this.result = null;
    }

    public void setResult(EntryDecoder decoder) throws IOException {
        if(result==null) {
            //need to fetch the latest results
            if(htable==null){
                htable = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(Long.toString(heapConglom)));
            }
            Get remoteGet = SpliceUtils.createGet(txnView, location);
            remoteGet.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
            remoteGet.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,filterBytes);

            Result r = htable.get(remoteGet);
            //we assume that r !=null, because otherwise, what are we updating?
            KeyValue[] rawKvs = r.raw();
            for(KeyValue kv:rawKvs){
                if(kv.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES)){
                    result = kv;
                    break;
                }
            }
            //we also assume that PACKED_COLUMN_KEY is properly set by the time we get here
//								getTimer.tick(1);
        }
        decoder.set(result.getBuffer(),result.getValueOffset(),result.getValueLength());
    }

    public void close() throws IOException {
        if(htable!=null)
            htable.close();
    }
}

