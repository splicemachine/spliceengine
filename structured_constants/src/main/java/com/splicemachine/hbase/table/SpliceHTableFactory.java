package com.splicemachine.hbase.table;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceHTableFactory implements HTableInterfaceFactory {
	private static Logger LOG = Logger.getLogger(SpliceHTableFactory.class);
	private boolean autoFlush = true;
    private ThreadPoolExecutor tableExecutor;
    protected IntObjectOpenHashMap<HConnection> connections = new IntObjectOpenHashMap<HConnection>();
    protected static AtomicInteger increment = new AtomicInteger();

		private final int numConnections;

	public SpliceHTableFactory() {
        this(true);
    }

		public SpliceHTableFactory(boolean autoFlush) {
			this(autoFlush,10);
		}

		public SpliceHTableFactory(boolean autoFlush,int numConnections) {
				SpliceLogUtils.trace(LOG, "instantiated with autoFlush set to %s",autoFlush);
				this.autoFlush = autoFlush;
				tableExecutor = getExecutor(SpliceConstants.config);
				this.numConnections = numConnections;

				/*
				 * This was put in place for TPCC behavior, but in 0.96, for some (currently
				 * unknown reason) this causes hangs when you try to use these connections
				 * instead of those pooled in HConnectionManager. At some point we'll need
				 * to try it again, but for now, it's just ripped out.
				 */
//				for (int i = 0; i< numConnections; i++) {
//						Configuration configuration = new Configuration(SpliceConstants.config);
//						configuration.setInt(HConstants.HBASE_CLIENT_INSTANCE_ID,i);
//						try {
//								connections.put(i, SpliceHConnection.createConnection(configuration));
//						} catch (ZooKeeperConnectionException e) {
//								throw new RuntimeException(e);
//						} catch (IOException e) {
//								throw new RuntimeException(e);
//						}
//				}
		}

		private ThreadPoolExecutor getExecutor(Configuration config) {
				int maxThreads = config.getInt("hbase.htable.threads.max",Integer.MAX_VALUE);
				if(maxThreads==0)
						maxThreads = 1;

        long keepAliveTime = config.getLong("hbase.htable.threads.keepalivetime",60);

        return new ThreadPoolExecutor(1,maxThreads,keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new NamedThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
		public HTableInterface createHTableInterface(Configuration config, final byte[] tableName) {
				SpliceLogUtils.trace(LOG, "createHTableInterface for %s",Bytes.toString(tableName));
				try {
						HConnection connection = HConnectionManager.getConnection(SpliceConstants.config);
						RegionCache instance = HBaseRegionCache.getInstance();
						SpliceLogUtils.trace(LOG,"creating actual HTable after connection created");
						final HTable htable =new SpliceHTable(tableName, connection,tableExecutor, instance);
						htable.setAutoFlushTo(autoFlush);
						SpliceLogUtils.trace(LOG,"Returning created table");
						return htable;
				} catch (IOException ioe) {
						ioe.printStackTrace();
						throw new RuntimeException(ioe);
				}
		}

	  @Override
	  public void releaseHTableInterface(HTableInterface table) throws IOException {
		SpliceLogUtils.trace(LOG, "releaseHTableInterface for %s",Bytes.toString(table.getTableName()));
	    table.close();
	  }

    private class NamedThreadFactory implements ThreadFactory {
        private ThreadGroup group;
        private String namePrefix;
        private AtomicInteger threadNumber = new AtomicInteger(1);

        private NamedThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s!=null)? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "htable-pool-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group,r,namePrefix+threadNumber.getAndIncrement(),0);
            if(!t.isDaemon())
                t.setDaemon(true);
            if(t.getPriority()!=Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);

            return t;
        }
    }
}