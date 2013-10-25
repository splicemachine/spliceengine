package com.splicemachine.hbase.table;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SpliceHTableFactory implements HTableInterfaceFactory {
	private static Logger LOG = Logger.getLogger(SpliceHTableFactory.class);
	private boolean autoFlush = true;
    private ThreadPoolExecutor tableExecutor;
    private HConnection connection;

	public SpliceHTableFactory() {
        this(true);
    }
	
	public SpliceHTableFactory(boolean autoFlush) {
		SpliceLogUtils.trace(LOG, "instantiated with autoFlush set to %s",autoFlush);
		this.autoFlush = autoFlush;

        tableExecutor = getExecutor(SpliceConstants.config);
        try {
            connection = HConnectionManager.getConnection(SpliceConstants.config);
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        }
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
	    	final HTable htable =new SpliceHTable(tableName,connection,tableExecutor, HBaseRegionCache.getInstance());
	    	htable.setAutoFlush(autoFlush);
	    	return htable;
	    } catch (IOException ioe) {
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