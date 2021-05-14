/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.si.impl;

import com.splicemachine.access.api.Durability;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.List;

public class RollbackTxnFilter extends FilterBase {

    private static final Logger LOG=Logger.getLogger(RollbackTxnFilter.class);


    private List<DataMutation> mutations;
    private OperationFactory opFactory;
    private TxnSupplier txnSupplier;

    public RollbackTxnFilter(TxnSupplier txnSupplier, List<DataMutation> mutations) {
        this.mutations = mutations;
        opFactory = SIDriver.driver().baseOperationFactory();
        this.txnSupplier = txnSupplier;
    }

    @Override
    public Filter.ReturnCode filterCell(Cell keyValue){
        DataCell data = new HCell(keyValue);
        long txnId = data.version();
        try {

            if (txnSupplier != null) {
                TxnView txn = txnSupplier.getTransaction(txnId);
                if (txn.getEffectiveState() == Txn.State.ROLLEDBACK) {
                    if (LOG.isDebugEnabled()) {
                        CellType type = data.dataType();
                        byte[] k = data.key();
                        byte[] v = data.value();
                        SpliceLogUtils.debug(LOG, "Filter out %s, k=%s, v=%s", type.toString(),
                                Bytes.toStringBinary(k), Bytes.toStringBinary(v));

                    }
                    ByteSlice bs = new ByteSlice();
                    bs.set(keyValue.getRowArray(), keyValue.getRowOffset(), keyValue.getRowLength());
                    DataDelete delete = opFactory.newDelete(bs);
                    delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.FK_COUNTER_COLUMN_BYTES, keyValue.getTimestamp());
                    delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.FIRST_OCCURRENCE_TOKEN_COLUMN_BYTES, keyValue.getTimestamp());
                    delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES, keyValue.getTimestamp());
                    delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.TOMBSTONE_COLUMN_BYTES, keyValue.getTimestamp());
                    delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES, keyValue.getTimestamp());
                    delete.addAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
                    delete.setDurability(Durability.NONE);
                    mutations.add(delete);
                    return ReturnCode.SKIP;
                }
                else {
                    if (LOG.isDebugEnabled()) {
                        CellType type = data.dataType();
                        byte[] k = data.key();
                        byte[] v = data.value();
                        SpliceLogUtils.debug(LOG, "Include %s, k=%s, v=%s", type.toString(),
                                Bytes.toStringBinary(k), Bytes.toStringBinary(v));

                    }
                    return ReturnCode.INCLUDE;
                }
            }
            return ReturnCode.INCLUDE;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<DataMutation> getMutations() {
        return mutations;
    }
}
