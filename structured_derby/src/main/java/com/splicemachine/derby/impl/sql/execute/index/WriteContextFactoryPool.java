package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.hbase.batch.WriteContextFactory;
import com.splicemachine.tools.ResourcePool;
import com.splicemachine.tools.ThreadSafeResourcePool;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class WriteContextFactoryPool {
    private static final LocalWriteContextFactory defaultFactory = LocalWriteContextFactory.unmanagedContextFactory();
    public static WriteContextFactory<RegionCoprocessorEnvironment> getDefaultFactory() {
        return defaultFactory;
    }

    private static class ContextKey implements ResourcePool.Key{
        private final long mainTableConglomId;

        private ContextKey(long mainTableConglomId){
            this.mainTableConglomId = mainTableConglomId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ContextKey)) return false;

            ContextKey that = (ContextKey) o;

            return mainTableConglomId == that.mainTableConglomId;

        }

        @Override
        public int hashCode() {
            return (int) (mainTableConglomId ^ (mainTableConglomId >>> 32));
        }

        public static ContextKey create(long mainTableConglomId) {
            return new ContextKey(mainTableConglomId);
        }
    }

    private static ResourcePool.Generator<LocalWriteContextFactory,ContextKey> generator = new ResourcePool.Generator<LocalWriteContextFactory,ContextKey>() {
        @Override
        public LocalWriteContextFactory makeNew(ContextKey refKey) {
            return new LocalWriteContextFactory(refKey.mainTableConglomId);
        }

        @Override
        public void close(LocalWriteContextFactory entity) {
            //no-op
        }
    };

    private static ResourcePool<LocalWriteContextFactory,ContextKey> pool = new ThreadSafeResourcePool<LocalWriteContextFactory,ContextKey>(generator);

    public static LocalWriteContextFactory getContextFactory(long mainTableConglomId) throws Exception {
        return pool.get(ContextKey.create(mainTableConglomId));
    }

    public static void releaseContextFactory(LocalWriteContextFactory writeContextFactory) throws Exception {
        pool.release(ContextKey.create(writeContextFactory.getMainTableConglomerateId()));
    }
}
