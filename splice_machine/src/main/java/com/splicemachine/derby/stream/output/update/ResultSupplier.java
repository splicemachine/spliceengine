package com.splicemachine.derby.stream.output.update;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import java.io.IOException;
import com.carrotsearch.hppc.BitSet;

/**
 *
 *
 *
 *
 */
public class ResultSupplier{
    private DataCell result;
    private byte[] location;
    private byte[] filterBytes;
    private Partition htable;
    private TxnView txnView;
    private long heapConglom;
    private transient DataGet remoteGet;
    private transient DataResult dataResult;

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
                htable =SIDriver.driver().getTableFactory().getTable(Long.toString(heapConglom));
            }
            remoteGet = SIDriver.driver().getOperationFactory().newDataGet(txnView,location,remoteGet);

            remoteGet.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES);
            remoteGet.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL,filterBytes);

            dataResult = htable.get(remoteGet,dataResult);
            result = dataResult.userData();
            //we also assume that PACKED_COLUMN_KEY is properly set by the time we get here
//								getTimer.tick(1);
        }
        decoder.set(result.valueArray(),result.valueOffset(),result.valueLength());
    }

    public void close() throws IOException {
        if(htable!=null)
            htable.close();
    }
}

