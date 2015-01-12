package com.splicemachine.derby.impl.temp;

import com.google.common.primitives.Longs;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.Snowflake;

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
		protected static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
        private static final Logger LOG = Logger.getLogger(TempTable.class);

		private final byte[] tempTableName;
		private AtomicReference<SpreadBucket> spread;

    /**
     * Prefix that should be used for log messages related to splice specific
     * compaction logic, in particular for the temp table. Use this prefix here
     * and in {@link com.splicemachine.derby.impl.temp.TempTable}, so that
     * one grep command can find these messages regardless of originating class.
     */
    public static final String LOG_COMPACT_PRE = "(splicecompact)";


		public TempTable(byte[] tempTableName) {
				this.tempTableName = tempTableName;
				
				SpreadBucket spreadBucket = SpreadBucket.getValue(SpliceConstants.tempTableBucketCount);
				if (spreadBucket == null) {
					throw new RuntimeException("Temp table spread bucket was NULL. Unable to initialize.");
				}
				SpliceLogUtils.info(LOG, "Temp Table initial bucket count: %d", spreadBucket.getNumBuckets());
				this.spread = new AtomicReference<SpreadBucket>(spreadBucket);
		}

		public SpreadBucket getCurrentSpread() {
				return spread.get();
		}

		public InternalScanner getTempCompactionScanner(){
			return derbyFactory.noOpInternalScanner();
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
                                    LOG.trace(String.format("%s Not removing file with max timestamp %d (>= %d threshold) ", LOG_COMPACT_PRE, maxStoreTs, deadDataThreshold));
                            // Remove it from this candidate list, which means we keep the store file around,
                            // because it has data that's still interesting to us.
								storeFileIterator.remove();
						}else if(LOG.isTraceEnabled()){
                            LOG.trace(String.format("%s Removing file with max timestamp %d (< %d threshold) and %d rows",
                                    LOG_COMPACT_PRE, maxStoreTs, deadDataThreshold, reader.getEntries()));
						}
				}
		}

		private long getTempCompactionThreshold(Configuration c) throws ExecutionException {
				long[] activeOperations = SpliceDriver.driver().getJobScheduler().getActiveOperations();

                if(LOG.isDebugEnabled()){
						LOG.debug("Detected "+ activeOperations.length+" active operations");
				}

                // Comment this out for now. This can be 10k, 100k, huge even for trace level.
                // if (LOG.isTraceEnabled())
                //		LOG.trace("Active Operations: "+ Arrays.toString(activeOperations));

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

				// As extra safeguard against incorrectly allowing splice_temp files to be deleted
				// while pending operations still need them, allow optional extra safeguard to protect
				// files until they are some duration in the past. This guards against issue in the logic
				// that handles closing of multiple jobs within a statement. As one example,
				// there were issues with select count(*) over multiple merge joins, resulting
				// in incorrect result. This might be removed, or configured to a very low number,
				// post 1.0.0.
				long safeguard = c.getLong("splice.compact.temp.safeguard", 6 * 60 * 60 * 1000); // 6 hours;

				long maxToUse = Longs.min(activeTimestamps) - maxClockSkew - safeguard;
				if(LOG.isTraceEnabled()) {
                    LOG.trace(String.format("%s Extra safeguard duration is %d millis (%d mins)", LOG_COMPACT_PRE, safeguard, safeguard / 60000));
                    LOG.trace(String.format("%s Looking to remove files with timestamp before: %d", LOG_COMPACT_PRE, maxToUse));
				}
				
				return maxToUse;
		}

		public byte[] getTempTableName() {
				return tempTableName;
		}

}
