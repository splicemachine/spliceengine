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

package com.splicemachine.pipeline;

import java.io.IOException;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.driver.SIDriver;

/**
 * Creates WriteHandlers that intercept writes to base tables and send transformed writes to corresponding index tables.
 */
class IndexFactory implements LocalWriteFactory{
    private DDLMessage.TentativeIndex tentativeIndex;
    private TxnView txn; // Null in the case of startup, populated in the case of DDL Change
    private long indexConglomerateId;

    IndexFactory(DDLMessage.TentativeIndex tentativeIndex, TxnView txn) {
        this.tentativeIndex = tentativeIndex;
        this.txn = txn;
        indexConglomerateId = tentativeIndex.getIndex().getConglomerate();
    }

    public static IndexFactory create(DDLMessage.TentativeIndex tentativeIndex) {
        return new IndexFactory(tentativeIndex,null);
    }
    public static IndexFactory create(DDLMessage.DDLChange ddlChange) {
        return new IndexFactory(ddlChange.getTentativeIndex(), DDLUtils.getLazyTransaction(ddlChange.getTxnId()));
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        IndexTransformer transformer = new IndexTransformer(tentativeIndex);
        IndexWriteHandler writeHandler = new IndexWriteHandler(keepState, expectedWrites, transformer);
        if (txn == null) {
            ctx.addLast(writeHandler);
        } else {
            DDLFilter ddlFilter = SIDriver.driver().readController().newDDLFilter(txn);
            ctx.addLast(new SnapshotIsolatedWriteHandler(writeHandler, ddlFilter));
        }
    }

    @Override
    public long getConglomerateId() {
        return indexConglomerateId;
    }

    @Override
    public boolean canReplace(LocalWriteFactory newContext){
        return newContext instanceof IndexFactory;
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        synchronized(this){
            IndexFactory other=(IndexFactory)newFactory;
            this.indexConglomerateId=other.indexConglomerateId;
            this.tentativeIndex=other.tentativeIndex;
            this.txn=other.txn;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof IndexFactory) {
            IndexFactory that = (IndexFactory) o;

            return indexConglomerateId == that.indexConglomerateId;
        } else if (o instanceof DropIndexFactory) {
            DropIndexFactory that = (DropIndexFactory) o;
            return indexConglomerateId == that.getDelegate().getConglomerateId();
        } else return false;
    }

    @Override
    public int hashCode() {
        return (int) (tentativeIndex.getIndex().getConglomerate() ^ (tentativeIndex.getIndex().getConglomerate() >>> 32));
    }

    @Override
    public String toString() {
        return "indexConglomId=" + indexConglomerateId + " isUnique=" + tentativeIndex.getIndex().getUnique() +
                " isUniqueWithDuplicateNulls=" + tentativeIndex.getIndex().getUniqueWithDuplicateNulls();
    }
}
