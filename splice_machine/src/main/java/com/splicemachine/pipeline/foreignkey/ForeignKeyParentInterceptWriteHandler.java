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

package com.splicemachine.pipeline.foreignkey;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.txn.ActiveWriteTxn;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.storage.*;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.si.impl.driver.SIDriver;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Intercepts deletes from a parent table (primary key or unique index) and sends the rowKey over to the referencing
 * indexes to check for its existence.
 */
@NotThreadSafe
public class ForeignKeyParentInterceptWriteHandler implements WriteHandler{
    private final List<Long> referencingIndexConglomerateIds;
    private final List<DDLMessage.FKConstraintInfo> constraintInfos;
    private final ForeignKeyViolationProcessor violationProcessor;
    private TxnOperationFactory txnOperationFactory;
    private HashMap<Long,Partition> childPartitions = new HashMap<>();
    private String parentTableName;
    private ObjectArrayList<KVPair> mutations = new ObjectArrayList<>();


    public ForeignKeyParentInterceptWriteHandler(String parentTableName,
                                                 List<Long> referencingIndexConglomerateIds,
                                                 PipelineExceptionFactory exceptionFactory,
                                                 List<DDLMessage.FKConstraintInfo> constraintInfos
                                                 ) {
        this.referencingIndexConglomerateIds = referencingIndexConglomerateIds;
        this.violationProcessor = new ForeignKeyViolationProcessor(
                new ForeignKeyViolationProcessor.ParentFkConstraintContextProvider(parentTableName),exceptionFactory);
        this.constraintInfos = constraintInfos;
        this.txnOperationFactory = SIDriver.driver().getOperationFactory();
        this.parentTableName = parentTableName;
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (isForeignKeyInterceptNecessary(mutation.getType())) {
            mutations.add(mutation);
        }
        ctx.sendUpstream(mutation);
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
            // TODO Buffer with skip scan
            for (int k = 0; k<mutations.size();k++) {
                KVPair mutation = mutations.get(k);
                for (int i = 0; i < referencingIndexConglomerateIds.size(); i++) {
                    long indexConglomerateId = referencingIndexConglomerateIds.get(i);
                    Partition table = null;
                    if (childPartitions.containsKey(indexConglomerateId))
                        table = childPartitions.get(indexConglomerateId);
                    else {
                        table = SIDriver.driver().getTableFactory().getTable(Long.toString((indexConglomerateId)));
                        childPartitions.put(indexConglomerateId, table);
                    }
                    if (hasReferences(indexConglomerateId, table, mutation, ctx))
                        failRow(mutation, ctx, constraintInfos.get(i));
                    else
                        ctx.success(mutation);
                }
            }
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx);
        }

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


        private boolean hasReferences(Long indexConglomerateId, Partition table, KVPair kvPair, WriteContext ctx) throws IOException {
        byte[] startKey = kvPair.getRowKey();
        //make sure this is a transactional scan
        DataScan scan = txnOperationFactory.newDataScan(null); // Non-Transactional, will resolve on this side
        scan =scan.startKey(startKey);
        byte[] endKey = Bytes.unsignedCopyAndIncrement(startKey);//new byte[startKey.length+1];
        scan = scan.stopKey(endKey);

            SimpleTxnFilter readUncommittedFilter;
            SimpleTxnFilter readCommittedFilter;
            if (ctx.getTxn() instanceof ActiveWriteTxn) {
                readCommittedFilter = new SimpleTxnFilter(Long.toString(indexConglomerateId), ((ActiveWriteTxn) ctx.getTxn()).getReadCommittedActiveTxn(), NoOpReadResolver.INSTANCE, SIDriver.driver().getTxnStore());
                readUncommittedFilter = new SimpleTxnFilter(Long.toString(indexConglomerateId), ((ActiveWriteTxn) ctx.getTxn()).getReadUncommittedActiveTxn(), NoOpReadResolver.INSTANCE, SIDriver.driver().getTxnStore());

            }
            else if (ctx.getTxn() instanceof WritableTxn) {
                readCommittedFilter = new SimpleTxnFilter(Long.toString(indexConglomerateId), ((WritableTxn) ctx.getTxn()).getReadCommittedActiveTxn(), NoOpReadResolver.INSTANCE, SIDriver.driver().getTxnStore());
                readUncommittedFilter = new SimpleTxnFilter(Long.toString(indexConglomerateId), ((WritableTxn) ctx.getTxn()).getReadUncommittedActiveTxn(), NoOpReadResolver.INSTANCE, SIDriver.driver().getTxnStore());
            }
            else
                throw new IOException("invalidTxn");
        try(DataScanner scanner = table.openScanner(scan)) {
            List<DataCell> next;
            while ((next = scanner.next(-1)) != null && !next.isEmpty()) {
                readCommittedFilter.reset();
                readUncommittedFilter.reset();
                if (hasData(next, readCommittedFilter) || hasData(next, readUncommittedFilter))
                    return true;
            }
            return false;
        }catch (Exception e) {
            throw new IOException(e);
        }
    }

    private boolean hasData(List<DataCell> next, SimpleTxnFilter txnFilter) throws IOException {
        int cellCount = next.size();
        for(DataCell dc:next){
            DataFilter.ReturnCode rC = txnFilter.filterCell(dc);
            switch(rC){
                case NEXT_ROW:
                    return false; //the entire row is filtered
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
        return cellCount>0;
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
