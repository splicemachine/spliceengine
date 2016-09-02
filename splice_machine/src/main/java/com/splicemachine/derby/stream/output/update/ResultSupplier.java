/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.output.update;

import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import java.io.IOException;
import com.carrotsearch.hppc.BitSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(interestedFields,true);
        this.filterBytes = predicateFilter.toBytes();
        this.txnView = txnView;
        this.heapConglom = heapConglom;
    }

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
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

