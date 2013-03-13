package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.tools.ResourcePool;
import com.splicemachine.tools.ThreadSafeResourcePool;

/**
 * Pool implementation for IndexSets.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class IndexSetPool {

    /*
     * Key implementation for obtaining a shared IndexSet.
     */
    private static class IndexKey implements ResourcePool.Key{
        private final long mainTableConglomId;

        private IndexKey(long mainTableConglomId) {
            this.mainTableConglomId = mainTableConglomId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof IndexKey)) return false;

            IndexKey indexKey = (IndexKey) o;

            return mainTableConglomId == indexKey.mainTableConglomId;
        }

        @Override
        public int hashCode() {
            return (int) (mainTableConglomId ^ (mainTableConglomId >>> 32));
        }

        private static ResourcePool.Key create(long mainTableConglomId){
            return new IndexKey(mainTableConglomId);
        }
    }


    private static ResourcePool.Generator<IndexSet> indexSetGenerator =
            new ResourcePool.Generator<IndexSet>() {
        @Override
        public IndexSet makeNew(ResourcePool.Key refKey) {
            if(!(refKey instanceof IndexKey)){
                return IndexSet.noIndex();
            }
            return IndexSet.create(((IndexKey)refKey).mainTableConglomId);
        }

        @Override
        public void close(IndexSet entity) {
            //no-op
        }
    };

    private static ResourcePool<IndexSet> indexPool = new ThreadSafeResourcePool<IndexSet>(indexSetGenerator);

    public static IndexSet getIndex(long mainTableConglomId){
        return indexPool.get(IndexKey.create(mainTableConglomId));
    }

    public static void releaseIndex(IndexSet indexSet) {
        indexPool.release(IndexKey.create(indexSet.getMainTableConglomerateId()));
    }

}
