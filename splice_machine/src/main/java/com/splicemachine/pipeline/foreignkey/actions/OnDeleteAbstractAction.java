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
 */

package com.splicemachine.pipeline.foreignkey.actions;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.foreignkey.ForeignKeyViolationProcessor;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.txn.ActiveWriteTxn;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.storage.*;
import com.splicemachine.utils.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class OnDeleteAbstractAction extends Action {

    private static final int MAX_BUFFER_SIZE = 1000;

    protected final DDLMessage.FKConstraintInfo constraintInfo;
    protected final ObjectObjectHashMap<KVPair, KVPair> mutationBuffer;
    protected final CallBuffer<KVPair> pipelineBuffer;
    protected final String parentTableName;
    Partition indexTable;
    private final TxnOperationFactory txnOperationFactory;
    protected final Map<String, KVPair> originators; // reverse lookup from child -> parent rows for propagating failures.

    private final ForeignKeyViolationProcessor violationProcessor;

    public OnDeleteAbstractAction(Long backingIndexConglomId,
                                  DDLMessage.FKConstraintInfo constraintInfo,
                                  WriteContext writeContext,
                                  TxnOperationFactory txnOperationFactory,
                                  ForeignKeyViolationProcessor violationProcessor,
                                  String parentTableName ) throws Exception {
        super(constraintInfo.getChildTable().getConglomerate(), backingIndexConglomId);
        this.txnOperationFactory = txnOperationFactory;
        assert childBaseTableConglomId != null;
        assert backingIndexConglomId != null;
        assert violationProcessor != null;
        this.constraintInfo = constraintInfo;
        this.mutationBuffer = new ObjectObjectHashMap<>();
        this.pipelineBuffer = writeContext.getSharedWriteBuffer(
                DDLUtils.getIndexConglomBytes(childBaseTableConglomId),
                this.mutationBuffer,
                MAX_BUFFER_SIZE * 2 + 10,
                true,
                writeContext.getTxn(),
                writeContext.getToken());
        this.indexTable = null;
        this.violationProcessor = violationProcessor;
        this.originators = new HashMap<>();
        this.parentTableName = parentTableName;
    }

    /*
     * The way prefix keys work is that longer keys sort after shorter keys. We
     * are already starting exactly where we want to be, and we want to end as soon
     * as we hit a record which is not this key.
     *
     * Historically, we did this by using an HBase PrefixFilter. We can do that again,
     * but it's a bit of a pain to make that work in an architecture-independent
     * way (we would need to implement a version of that for other architectures,
     * for example. It's much easier for us to just make use of row key sorting
     * to do the job for us.
     *
     * We start where we want, and we need to end as soon as we run off that. The
     * first key which is higher than the start key is the start key as a prefix followed
     * by 0x00 (in unsigned sort order). Therefore, we make the end key
     * [startKey | 0x00].
     */
    private static DataScan prepareScan(TxnOperationFactory factory, KVPair needle) {
        byte[] startKey = needle.getRowKey();
        byte[] stopKey = Bytes.unsignedCopyAndIncrement(startKey); // +1 from startKey.
        DataScan scan = factory.newDataScan(null); // Non-Transactional, will resolve on this side
        return scan.startKey(startKey).stopKey(stopKey);
    }

    private static Pair<SimpleTxnFilter, SimpleTxnFilter> prepareScanFilters(TxnView txnView, long indexConglomerateId) throws IOException {
        SimpleTxnFilter readUncommittedFilter, readCommittedFilter;
        if (txnView instanceof ActiveWriteTxn) {
            readCommittedFilter = new SimpleTxnFilter(Long.toString(indexConglomerateId), ((ActiveWriteTxn) txnView).getReadCommittedActiveTxn(), NoOpReadResolver.INSTANCE, SIDriver.driver().getTxnStore());
            readUncommittedFilter = new SimpleTxnFilter(Long.toString(indexConglomerateId), ((ActiveWriteTxn) txnView).getReadUncommittedActiveTxn(), NoOpReadResolver.INSTANCE, SIDriver.driver().getTxnStore());

        } else if (txnView instanceof WritableTxn) {
            readCommittedFilter = new SimpleTxnFilter(Long.toString(indexConglomerateId), ((WritableTxn) txnView).getReadCommittedActiveTxn(), NoOpReadResolver.INSTANCE, SIDriver.driver().getTxnStore());
            readUncommittedFilter = new SimpleTxnFilter(Long.toString(indexConglomerateId), ((WritableTxn) txnView).getReadUncommittedActiveTxn(), NoOpReadResolver.INSTANCE, SIDriver.driver().getTxnStore());
        } else {
            throw new IOException("invalidTxn,");
        }
        return Pair.newPair(readCommittedFilter, readUncommittedFilter);
    }

    private byte[] isVisible(List<DataCell> next, SimpleTxnFilter txnFilter) throws IOException {
        int cellCount = next.size();
        for(DataCell dc:next){
            DataFilter.ReturnCode rC = txnFilter.filterCell(dc);
            switch(rC){
                case NEXT_ROW:
                    return null; //the entire row is filtered
                case SKIP:
                case NEXT_COL:
                case SEEK:
                    cellCount--; //the cell is filtered
                    break;
                case INCLUDE:
                case INCLUDE_AND_NEXT_COL: //the cell is included
                default:
                    break;
            }
        }
        if(cellCount > 0) {
            return next.get(0).key();
        }
        return null;
    }

    private Partition getTable() throws IOException {
        if(indexTable == null) {
            indexTable = SIDriver.driver().getTableFactory().getTable(Long.toString((backingIndexConglomId)));
        }
        return indexTable;
    }

    protected abstract WriteResult handleExistingRow(byte[] indexRow, KVPair sourceMutation) throws Exception;

    protected static byte[] toChildBaseRowId(byte[] indexRowId, DDLMessage.FKConstraintInfo fkConstraintInfo) throws StandardException {
        MultiFieldDecoder multiFieldDecoder = MultiFieldDecoder.create();
        TypeProvider typeProvider = VersionedSerializers.typesForVersion(fkConstraintInfo.getParentTableVersion());
        int position = 0;
        multiFieldDecoder.set(indexRowId);
        for (int i = 0; i < fkConstraintInfo.getFormatIdsCount(); i++) {
            if (multiFieldDecoder.nextIsNull()) {
                throw StandardException.newException(String.format("unexpected index rowid format %s", Bytes.toHex(indexRowId)));
            }
            if (fkConstraintInfo.getFormatIds(i) == StoredFormatIds.SQL_DOUBLE_ID) {
                position += multiFieldDecoder.skipDouble();
            } else if (fkConstraintInfo.getFormatIds(i) == StoredFormatIds.SQL_REAL_ID) {
                position += multiFieldDecoder.skipFloat();
            } else if (typeProvider.isScalar(fkConstraintInfo.getFormatIds(i))) {
                position += multiFieldDecoder.skipLong();
            } else {
                position += multiFieldDecoder.skip();
            }
        }
        // position ends up 1 byte beyond the key prefix, last index is placed on the end on the prefix
        // and 1 byte before the row key.
        int lastKeyIndex = position - 1;

        if (lastKeyIndex == indexRowId.length) {
            throw StandardException.newException(String.format("unexpected index rowid format %s", Bytes.toHex(indexRowId)));
        } else {
            byte[] result = new byte[indexRowId.length - lastKeyIndex - 1];
            System.arraycopy(indexRowId, lastKeyIndex + 1, result, 0, result.length);
            return Encoding.decodeBytesUnsorted(result, 0, result.length);
        }
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        assert !failed;
        DataScan scan = prepareScan(txnOperationFactory, mutation);
        Pair<SimpleTxnFilter, SimpleTxnFilter> filters;
        Partition indexTable;
        try {
            filters = prepareScanFilters(ctx.getTxn(), backingIndexConglomId);
            indexTable = getTable();
        } catch (IOException e) {
            failed = true;
            writeResult = WriteResult.failed(e.getMessage());
            return;
        }
        assert indexTable != null;
        try (DataScanner scanner = indexTable.openScanner(scan)) {
            List<DataCell> next;
            while ((next = scanner.next(-1)) != null && !next.isEmpty()) {
                filters.getFirst().reset();
                filters.getSecond().reset();
                byte[] indexRow = isVisible(next, filters.getFirst());
                if (indexRow == null) {
                    indexRow = isVisible(next, filters.getSecond());
                }
                if (indexRow != null) {
                    writeResult = handleExistingRow(indexRow, mutation);
                }
            }
        } catch (Exception e) {
            failed = true;
            writeResult = WriteResult.failed(e.getMessage());
        }
    }

    @Override
    public void flush(WriteContext ctx) {
        try {
            pipelineBuffer.flushBufferAndWait();
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx, originators);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        if(indexTable != null) {
            indexTable.close();
        }
    }
}
