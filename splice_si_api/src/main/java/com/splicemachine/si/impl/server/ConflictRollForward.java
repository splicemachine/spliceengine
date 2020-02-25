/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.si.impl.server;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataDelete;
import com.splicemachine.storage.DataMutation;
import com.splicemachine.storage.DataPut;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConflictRollForward {
    private long commitTs;
    private long txnId;
    private final List<DataMutation> mutations;
    private final OperationFactory opFactory;
    private final TxnSupplier txnSupplier;

    public ConflictRollForward(OperationFactory opFactory, TxnSupplier txnSupplier) {
        this.mutations = new ArrayList<>();
        this.opFactory = opFactory;
        this.txnSupplier = txnSupplier;
    }

    public void reset(long txnId, long commitTs) {
        this.txnId = txnId;
        this.commitTs = commitTs;
    }

    public void handle(DataCell cell) throws IOException {
        if (cell == null) {
            return;
        }

        // We have a commit KV but couldn't decode the commit timestamp, ignore all Cells
        if (txnId > 0 && commitTs == 0)
            return;

        if (cell.version() <= txnId) {
            // This datacell is either covered by the commit timestamp or it could be handled by an older one, so we
            // ignore it
            return;
        }

        TxnView txn = txnSupplier.getTransaction(cell.version());

        if (!txn.getEffectiveState().isFinal())
            return;

        ByteSlice bs = new ByteSlice();
        bs.set(cell.keyArray(), cell.keyOffset(), cell.keyLength());
        if (txn.getEffectiveState().equals(Txn.State.COMMITTED)) {
            DataPut put = opFactory.newPut(bs);
            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES, cell.version(), Bytes.toBytes(txn.getEffectiveCommitTimestamp()));
            put.addAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
            put.skipWAL();
            mutations.add(put);
        } else {
            DataDelete delete = opFactory.newDelete(bs);
            delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.FK_COUNTER_COLUMN_BYTES, cell.version());
            delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.FIRST_OCCURRENCE_TOKEN_COLUMN_BYTES, cell.version());
            delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES, cell.version());
            delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.TOMBSTONE_COLUMN_BYTES, cell.version());
            delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES, cell.version());
            delete.addAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
            delete.skipWAL();
            mutations.add(delete);
        }
    }

    public List<DataMutation> getMutations() {
        return mutations;
    }
}
