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

package com.splicemachine.pipeline;

import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import com.splicemachine.si.api.txn.Txn;
import java.io.IOException;

class DropIndexFactory implements LocalWriteFactory{
    private Txn dropTxn;
    private volatile LocalWriteFactory delegate;
    private long indexConglomId;

    DropIndexFactory(Txn dropTxn, LocalWriteFactory delegate, long indexConglomId) {
        this.dropTxn=dropTxn;
        this.delegate = delegate;
        this.indexConglomId = indexConglomId;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        if (delegate == null) return; //no delegate, so nothing to do just yet
        /*
         * We need to maintain the index just in case the dropping transaction is rolled back, BUT
         * simultaneously, if we perform future writes within the same transaction as the drop index,
         * we can NOT update the index itself. This boils into two categories:
         *
         * 1. T1 drops index, then performs subsequent write
         * 2. T1 drop index, then T2 performs subsequent write
         *
         * Category 1:
         *
         * There are two main possible timelines:
         *
         * T1.begin -> T1.dropIndex->T1.write->T1.rollback
         * T1.begin -> T1.dropIndex ->T1.write->T1.commit
         *
         * In the first case, when we rollback, we rollback BOTH the write AND the dropIndex, so it doesn't matter
         * if we updated the index or not.
         *
         * In the second case, the drop index is successfully applied, and therefore it is irrelevant whether we
         * updated the index or not.
         *
         * However, if we update the index, we run into a situation where we might cause a UniqueIndex violation.
         * Then we would have to do some kind of way of supressing the violation (since it's not a unique violation
         * in our transaction). In that situation, we can lead ourselves to overwriting other data, resulting in
         * a corrupted index. Therefore, we cannot write data to the index itself.
         *
         * Category 2:
         * In this scenario, any write which occurs AFTER the drop is committed does not need to write data
         * to the index, but any write which occurs BEFORE the drop is committed will have to, in case the drop
         * has been rolled back.
         *
         * Thus, the logic is something as follows:
         *
         * 1. Determine if DropTxn has been committed. If so, then discard any write at all
         * 2. If dropTxn has been rolled back, then proceed with the write
         * 3. If dropTxn is still active, find the youngest common ancestor(YCA) of dropTxn and ctx.getTxn().
         * 4. If YCA !=ROOT, then do not perform the write, otherwise, perform the write
         */

        if (!ctx.getTxn().canSee(dropTxn)) delegate.addTo(ctx, keepState, expectedWrites);
    }

    public void setDelegate(LocalWriteFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public long getConglomerateId() {
        return indexConglomId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof DropIndexFactory)
            return ((DropIndexFactory) o).indexConglomId == indexConglomId;
        else return o instanceof IndexFactory && ((IndexFactory) o).getConglomerateId() == indexConglomId;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public boolean canReplace(LocalWriteFactory newContext){
        return equals(newContext);
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        this.delegate = newFactory;
    }

    public LocalWriteFactory getDelegate() {
        return delegate;
    }
}
