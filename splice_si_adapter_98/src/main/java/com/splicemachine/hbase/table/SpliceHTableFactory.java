package com.splicemachine.hbase.table;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.hbase.table.SpliceConnectionPool;

public class SpliceHTableFactory implements HTableInterfaceFactory {
    private static Logger LOG = Logger.getLogger(SpliceHTableFactory.class);

    private boolean autoFlush = true;
    private ExecutorService tablePool;

    private final SpliceConnectionPool connectionPool;

    public SpliceHTableFactory(){
       this(true);
    }

    public SpliceHTableFactory(boolean autoFlush){
        this(autoFlush,SpliceConnectionPool.INSTANCE);
    }

    public SpliceHTableFactory(boolean autoFlush,SpliceConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        SpliceLogUtils.trace(LOG, "instantiated with autoFlush set to %s", autoFlush);
        this.autoFlush = autoFlush;
        tablePool = createHTablePool(SpliceConstants.config);
    }

    @Override
    public HTableInterface createHTableInterface(Configuration config, final byte[] tableName) {
        SpliceLogUtils.trace(LOG, "createHTableInterface for %s", Bytes.toString(tableName));
        try {
            SpliceLogUtils.trace(LOG, "creating actual HTable after connection created");
            final HTableInterface htable = new SpliceHTable(tableName, connectionPool.getConnectionDirect(),
                    tablePool, HBaseRegionCache.getInstance());
            htable.setAutoFlushTo(autoFlush);
            SpliceLogUtils.trace(LOG, "Returning created table");
            return htable;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void releaseHTableInterface(HTableInterface table) throws IOException {
        SpliceLogUtils.trace(LOG, "releaseHTableInterface for %s", Bytes.toString(table.getTableName()));
        table.close();
    }

    private static ExecutorService createHTablePool(Configuration config) {
        int maxThreads = config.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
        if (maxThreads == 0)
            maxThreads = 1;

        long keepAliveTime = config.getLong("hbase.htable.threads.keepalivetime", 60);

        return new ThreadPoolExecutor(1, maxThreads, keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new NamedThreadFactory("htable-pool-"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private ThreadGroup group;
        private String namePrefix;
        private AtomicInteger threadNumber = new AtomicInteger(1);

        private NamedThreadFactory(String namePrefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = namePrefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (!t.isDaemon())
                t.setDaemon(true);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);

            return t;
        }
    }
}