/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.output.update;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.SpliceQuery;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;

/**
 *
 *
 *
 *
 */
public class RedoResultSupplier {
    private DataCell result;
    private byte[] location;
    private byte[] filterBytes;
    private Partition htable;
    private TxnView txnView;
    private long heapConglom;
    private ExecRow execRow;

    public RedoResultSupplier(BitSet interestedFields, TxnView txnView, long heapConglom, ExecRow execRow) {
        //we need the index so that we can transform data without the information necessary to decode it
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(interestedFields,true);
        this.filterBytes = predicateFilter.toBytes();
        this.txnView = txnView;
        this.heapConglom = heapConglom;
        this.execRow = execRow;
    }

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public void setLocation(byte[] location){
        this.location = location;
        this.result = null;
    }

    public DataCell getResult() throws IOException {
            //need to fetch the latest results
            if(htable==null){
                htable =SIDriver.driver().getTableFactory().getTable(Long.toString(heapConglom));
            }
            DataGet remoteGet = SIDriver.driver().getOperationFactory().newDataGet(txnView,location,null);
            remoteGet.addColumn(SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES,SIConstants.PACKED_COLUMN_BYTES);
            SpliceQuery spliceQuery = new SpliceQuery(execRow);
            SIDriver.driver().baseOperationFactory().setQuery(remoteGet,spliceQuery);
            remoteGet.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL,filterBytes);
            DataResult dataResult = htable.get(remoteGet,null);
            return dataResult.activeData();
            //we also assume that PACKED_COLUMN_KEY is properly set by the time we get here
//								getTimer.tick(1);
    }

    public void close() throws IOException {
        if(htable!=null)
            htable.close();
    }
}

