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

package com.splicemachine.pipeline.foreignkey;

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.stream.output.update.NonPkRowHash;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.WriteHandler;
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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Intercepts deletes from a parent table (primary key or unique index) and sends the rowKey over to the referencing
 * indexes to check for its existence.
 */
@NotThreadSafe
public class ForeignKeyParentInterceptWriteHandler implements WriteHandler{

    private boolean failed;

    static class ChildBaseTableContext {
        public ChildBaseTableContext(Long conglomerateId,
                                     Long fkIndexConglomerateId,
                                     DDLMessage.FKConstraintInfo constraintInfo,
                                     WriteContext context) throws Exception {
            this.conglomerateId = conglomerateId;
            this.fkIndexConglomerateId = fkIndexConglomerateId;
            this.mutationBuffer = new ObjectObjectHashMap<>();
            this.pipelineBuffer = context.getSharedWriteBuffer(
                    DDLUtils.getIndexConglomBytes(conglomerateId),
                    this.mutationBuffer,
                    1000 * 2 + 10,
                    true,
                    context.getTxn(),
                    context.getToken());
            this.constraintInfo = constraintInfo;
        }
        public final Long conglomerateId;
        public final Long fkIndexConglomerateId;
        public final ObjectObjectHashMap<KVPair, KVPair> mutationBuffer;
        public final CallBuffer<KVPair> pipelineBuffer;
        public final DDLMessage.FKConstraintInfo constraintInfo;
    }

    private final List<Long> referencingIndexConglomerateIds;
    private List<Long> childBaseTableConglomerateIds;
    private boolean childContextsCreated = false;
    List<ChildBaseTableContext> childBaseTableContexts;
    private final List<DDLMessage.FKConstraintInfo> constraintInfos;
    private final ForeignKeyViolationProcessor violationProcessor;
    private TxnOperationFactory txnOperationFactory;
    private HashMap<Long,Partition> childPartitions = new HashMap<>();
    private String parentTableName;
    private transient DataGet baseGet = null;
    private ObjectArrayList<KVPair> mutations = new ObjectArrayList<>();


    public ForeignKeyParentInterceptWriteHandler(String parentTableName,
                                                 List<Long> referencingIndexConglomerateIds,
                                                 List<Long> childBaseTableConglomerateIds,
                                                 List<DDLMessage.FKConstraintInfo> constraintInfos,
                                                 PipelineExceptionFactory exceptionFactory
                                                 ) {
        this.referencingIndexConglomerateIds = referencingIndexConglomerateIds;
        this.childBaseTableConglomerateIds = childBaseTableConglomerateIds;
        this.childBaseTableContexts = new ArrayList<>();
        this.violationProcessor = new ForeignKeyViolationProcessor(
                new ForeignKeyViolationProcessor.ParentFkConstraintContextProvider(parentTableName),exceptionFactory);
        this.constraintInfos = constraintInfos;
        this.txnOperationFactory = SIDriver.driver().getOperationFactory();
        this.parentTableName = parentTableName;
    }

    private boolean ensureBuffers(WriteContext context) {
        if (!childContextsCreated) {
            assert childBaseTableConglomerateIds.size() == constraintInfos.size();
            for (int i = 0; i < childBaseTableConglomerateIds.size(); i++) {
               if(constraintInfos.get(i).getDeleteRule() == StatementType.RA_SETNULL) {
                   try {
                       childBaseTableContexts.add(new ChildBaseTableContext(childBaseTableConglomerateIds.get(i),
                               referencingIndexConglomerateIds.get(i), constraintInfos.get(i), context));
                   } catch (Exception e) {
                       // todo improve this
                       return false;
                   }
               }
            }
            childContextsCreated = true;
        }
        return true;
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if(failed) {
            ctx.notRun(mutation);
            return;
        }
        if (isForeignKeyInterceptNecessary(mutation.getType())) {
            ensureBuffers(ctx); // todo handle failures properly
            mutations.add(mutation);
            for(ChildBaseTableContext childBaseTableContext : childBaseTableContexts) {
                if(!checkReferencesAndUpdateChildBuffers(mutation, ctx, childBaseTableContext)) {
                    failed = true;
                    failRow(mutation, ctx, childBaseTableContext.constraintInfo);
                    break;
                }
            }
        }
        if(!failed) {
            ctx.success(mutation);
            ctx.sendUpstream(mutation);
        }
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
    private static DataScan prepareScan(TxnOperationFactory factory, TxnView txnView, KVPair needle) {
        byte[] startKey = needle.getRowKey();
        byte[] stopKey = Bytes.unsignedCopyAndIncrement(startKey); // +1 from startKey.
        DataScan scan = factory.newDataScan(txnView);
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

    private static byte[] toChildBaseRowId(byte[] indexRowId, DDLMessage.FKConstraintInfo fkConstraintInfo) {
        // 1. determine the position
        MultiFieldDecoder multiFieldDecoder = MultiFieldDecoder.create();
        TypeProvider typeProvider = VersionedSerializers.typesForVersion(fkConstraintInfo.getParentTableVersion());
        int position = 0;
        multiFieldDecoder.set(indexRowId);
        for (int i = 0; i < fkConstraintInfo.getFormatIdsCount(); i++) {
            if (multiFieldDecoder.nextIsNull()) {
                return null;
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
        int lastKeyIndex = position - 2;

        if (lastKeyIndex == indexRowId.length - 1) {
            return null; // special case.
        } else {
            byte[] result = new byte[indexRowId.length - lastKeyIndex - 2];
            System.arraycopy(indexRowId, lastKeyIndex + 2, result, 0, result.length);
            return Encoding.decodeBytesUnsortd(result, 0, result.length);
        }
    }

    KVPair constructUpdateToNull(byte[] rowId, ChildBaseTableContext childBaseTableContext) throws StandardException, IOException {
        int colCount = childBaseTableContext.constraintInfo.getTable().getFormatIdsCount();
        int[] keyColumns = childBaseTableContext.constraintInfo.getColumnIndicesList().stream().mapToInt(i -> i).toArray();
        int[] oneBased = new int[colCount + 1];
        for(int i = 0; i < colCount; ++i) {
            oneBased[i+1] = i;
        }
        FormatableBitSet heapSet = new FormatableBitSet(oneBased.length);
        ExecRow execRow = WriteReadUtils.getExecRowFromTypeFormatIds(childBaseTableContext.constraintInfo.getTable().getFormatIdsList().stream().mapToInt(i -> i).toArray());
        for (int keyColumn : keyColumns) {
            execRow.setColumn(keyColumn, execRow.getColumn(keyColumn).getNewNull());
            heapSet.set(keyColumn);
        }
        DescriptorSerializer[] serializers = VersionedSerializers.forVersion(childBaseTableContext.constraintInfo.getTable().getTableVersion(), true).getSerializers(execRow);
        EntryDataHash entryEncoder = new NonPkRowHash(oneBased, null, serializers, heapSet);
        ValueRow rowToEncode = new ValueRow(execRow.getRowArray().length);
        rowToEncode.setRowArray(execRow.getRowArray());
        entryEncoder.setRow(rowToEncode);
        byte[] value = entryEncoder.encode();
        return new KVPair(rowId, value, KVPair.Type.UPDATE);
    }

    private boolean checkReferencesAndUpdateChildBuffers(KVPair mutation,
                                                         WriteContext ctx,
                                                         ChildBaseTableContext childBaseTableContext) {
        assert isForeignKeyInterceptNecessary(mutation.getType());

        DataScan scan = prepareScan(txnOperationFactory, ctx.getTxn(), mutation);
        Pair<SimpleTxnFilter, SimpleTxnFilter> filters = null;
        try {
            filters = prepareScanFilters(ctx.getTxn(), childBaseTableContext.fkIndexConglomerateId);
            Partition indexTable = getTable(childBaseTableContext.fkIndexConglomerateId);
            try (DataScanner scanner = indexTable.openScanner(scan)) {
                List<DataCell> next;
                while ((next = scanner.next(-1)) != null && !next.isEmpty()) {
                    filters.getFirst().reset();
                    filters.getSecond().reset();
                    byte[] indexRow = isVisible(next, filters.getFirst());
                    byte[] baseTableRowId = null;
                    if(indexRow == null) {
                        indexRow = isVisible(next, filters.getSecond());
                    }
                    if(indexRow != null) {
                        if (childBaseTableContext.constraintInfo.getDeleteRule() == StatementType.RA_SETNULL) {
                            baseTableRowId = toChildBaseRowId(indexRow, childBaseTableContext.constraintInfo);
                        } else {
                            return false;
                        }
                    }
                    if(baseTableRowId != null) {
                        KVPair pair = constructUpdateToNull(baseTableRowId, childBaseTableContext);
                        childBaseTableContext.pipelineBuffer.add(pair);
                        childBaseTableContext.mutationBuffer.putIfAbsent(pair, pair);
                    }
                }
            } catch (Exception e) {
                return false;
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    /** We exist to prevent updates/deletes of rows from the parent table which are referenced by a child.
     * Since we are a WriteHandler on a primary-key or unique-index we can just handle deletes.
     */
    private boolean isForeignKeyInterceptNecessary(KVPair.Type type) {
        /* We exist to prevent updates/deletes of rows from the parent table which are referenced by a child.
         * Since we are a WriteHandler on a primary-key or unique-index we can just handle deletes. */
        return type == KVPair.Type.DELETE;
    }


    @Override
    public void flush(WriteContext ctx) throws IOException {
        try {
            for(ChildBaseTableContext childBaseTableContext : childBaseTableContexts) {
                childBaseTableContext.pipelineBuffer.flushBufferAndWait();
            }
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx);
        }

    }

    private Partition getTable(long indexConglomerateId) throws IOException {
        Partition table;
        if (childPartitions.containsKey(indexConglomerateId))
            table = childPartitions.get(indexConglomerateId);
        else {
            table = SIDriver.driver().getTableFactory().getTable(Long.toString((indexConglomerateId)));
            childPartitions.put(indexConglomerateId, table);
        }
        return table;
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        mutations.clear();
        for (Partition table:childPartitions.values()) {
            if (table != null)
                table.close();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     *
     * TODO JL
     * Try to understand why we have replacement algorithms for simple messages.
     *
     * @param mutation
     * @param ctx
     * @param fkConstraintInfo
     */
    private void failRow(KVPair mutation, WriteContext ctx, DDLMessage.FKConstraintInfo fkConstraintInfo ) {
        String failedKvAsHex = Bytes.toHex(mutation.getRowKey());
        ConstraintContext context = ConstraintContext.foreignKey(fkConstraintInfo);
        WriteResult foreignKeyConstraint = new WriteResult(Code.FOREIGN_KEY_VIOLATION, context.withMessage(1, parentTableName));
        ctx.failed(mutation, foreignKeyConstraint);
    }
}
