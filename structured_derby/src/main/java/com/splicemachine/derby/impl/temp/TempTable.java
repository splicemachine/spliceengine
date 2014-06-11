package com.splicemachine.derby.impl.temp;

import com.google.common.primitives.Longs;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.utils.Snowflake;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Representation of the current state of a TempTable.
 *
 * Implementation note(-sf-): This was deliberately constructed
 * to NOT be a singleton, in case we ever decided to go with
 * multiple TempTables.
 *
 * @author Scott Fines
 * Date: 11/18/13
 */
public class TempTable {
		private static final Logger LOG = Logger.getLogger(TempTable.class);
		private final byte[] tempTableName;
		private AtomicReference<SpreadBucket> spread;

		public TempTable(byte[] tempTableName) {
				this.tempTableName = tempTableName;
				this.spread = new AtomicReference<SpreadBucket>(SpreadBucket.SIXTEEN);
		}

		public SpreadBucket getCurrentSpread() {
				return spread.get();
		}

		public InternalScanner getTempCompactionScanner(){
			return new NoOpInternalScanner();
		}

		/**
		 * Filters out StoreFiles from TEMP which contain data that *must* be kept (they are data
		 * for an ongoing operation).
		 *
		 * @param config the configuration to use
		 * @param storeFiles the store files to use
		 * @throws ExecutionException
		 */
		public void filterCompactionFiles(Configuration config,List<StoreFile> storeFiles) throws ExecutionException {
				long deadDataThreshold = getTempCompactionThreshold(config);

				Iterator<StoreFile> storeFileIterator = storeFiles.iterator();
				while (storeFileIterator.hasNext()) {
						StoreFile storeFile = storeFileIterator.next();
						StoreFile.Reader reader = storeFile.getReader();
						long maxStoreTs = reader.getMaxTimestamp();
						if (maxStoreTs >= deadDataThreshold) {
								if(LOG.isTraceEnabled())
										LOG.trace("Keeping file with max timestamp "+maxStoreTs+", since maxTimestamp >= "+deadDataThreshold);
								//keep this store file around, it has data that's still interesting to us
								storeFileIterator.remove();
						}else if(LOG.isTraceEnabled()){
								LOG.trace("Removing file with timestamp "+maxStoreTs+" and "+reader.getEntries()+" rows");
						}
				}
		}

		private long getTempCompactionThreshold(Configuration c) throws ExecutionException {
				long[] activeOperations = SpliceDriver.driver().getJobScheduler().getActiveOperations();
				if(LOG.isDebugEnabled()){
						LOG.debug("Detected "+ activeOperations.length+" active operations");
				}
				if(LOG.isTraceEnabled())
						LOG.trace("Active Operations: "+ Arrays.toString(activeOperations));
				if(activeOperations.length==0){
						//we can remove everything!
						return System.currentTimeMillis();
				}
				//transform the operation ids into timestamps
				long[] activeTimestamps = new long[activeOperations.length];
				for(int i=0;i<activeOperations.length;i++){
						if(activeOperations[i]!=-1)
								activeTimestamps[i] = Snowflake.timestampFromUUID(activeOperations[i]);
				}

						/*
						 * HBase has a configurable "max clock skew" setting, which forces the RegionServer to have a System
						 * clock within <maxClockSkew> milliseconds of the Master. As a consequence of that, all RegionServers
						 * should have pretty close to the same time (within some multiple of clockSkew). We opt conservatively
						 * here and assume that we can have a system clock difference between two regionservers of 2*clockSkew.
						 *
						 * In practice, we want this clock Skew to be very small anyway, because we could run the risk of
						 * duplicate UUIDs if the system clock gets reset (e.g. it's best to run ntp or some other system
						 * to maintain consistent system clocks).
						 *
						 */
				long maxClockSkew = c.getLong("hbase.master.maxclockskew", 30000);
				maxClockSkew*=2; //unfortunate fudge factor to deal with the reality of different system clocks

				long l = Longs.min(activeTimestamps) - maxClockSkew;
				if(LOG.isTraceEnabled())
						LOG.trace("Removing data which occurs before timestamp "+ l);
				return l;
		}

		public byte[] getTempTableName() {
				return tempTableName;
		}

		private static class NoOpInternalScanner implements InternalScanner{

				@Override public boolean next(List<KeyValue> results) throws IOException { return false;   }

				@Override public boolean next(List<KeyValue> results, String metric) throws IOException { return false;}

				@Override public boolean next(List<KeyValue> result, int limit) throws IOException { return false;}

				@Override public boolean next(List<KeyValue> result, int limit, String metric) throws IOException { return false; }

				@Override public void close() throws IOException { }
		}
}
