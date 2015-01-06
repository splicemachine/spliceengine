package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.si.api.TransactionalRegion;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Creates and caches a WriteContextFactory for each conglomerateId.
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class WriteContextFactoryManager {

    protected static final ConcurrentMap<Long, DiscardingWriteContextFactory<TransactionalRegion>> ctxMap = new ConcurrentHashMap<>();

    static {
        DiscardingWriteContextFactory<TransactionalRegion> discardingWriteContextFactory
                = new DiscardingWriteContextFactory<>(-1L, LocalWriteContextFactory.unmanagedContextFactory());

        // always hold on to the unmanaged factory so that it never closes
        discardingWriteContextFactory.incrementRefCount();

        ctxMap.put(-1L, discardingWriteContextFactory);
    }

    public static WriteContextFactory<TransactionalRegion> getWriteContext(long conglomerateId) {
        DiscardingWriteContextFactory<TransactionalRegion> ctxFactory = ctxMap.get(conglomerateId);

        if (ctxFactory == null) {
            LocalWriteContextFactory localWriteContextFactory = new LocalWriteContextFactory(conglomerateId);
            DiscardingWriteContextFactory<TransactionalRegion> newFactory = new DiscardingWriteContextFactory<>(conglomerateId, localWriteContextFactory);

            DiscardingWriteContextFactory<TransactionalRegion> oldFactory = ctxMap.putIfAbsent(conglomerateId, newFactory);
            ctxFactory = (oldFactory != null) ? oldFactory : newFactory;
        }

        ctxFactory.incrementRefCount();
        return ctxFactory;
    }

    protected static void remove(long conglomerateId, DiscardingWriteContextFactory value) {
        ctxMap.remove(conglomerateId, value);
    }

}
