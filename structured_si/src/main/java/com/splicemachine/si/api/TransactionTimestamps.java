package com.splicemachine.si.api;

import com.splicemachine.si.txn.SpliceTimestampSource;
import com.splicemachine.utils.ThreadSafe;
import com.splicemachine.utils.ZkUtils;

/**
 * Factory class for getting direct access to a Transactional Timestamp source.
 *
 * @author Scott Fines
 * Date: 7/3/14
 */
public class TransactionTimestamps {
		private static volatile @ThreadSafe TimestampSource timestampSource;

		public static TimestampSource getTimestampSource(){
				TimestampSource ts = timestampSource; //do just a single volatile read, unless initializing
				if(ts==null)
						ts =initialize();
				return ts;
		}

		public static void setTimestampSource(@ThreadSafe TimestampSource ts){
				timestampSource = ts;
		}

		private static synchronized TimestampSource initialize() {
				//only do 1 volatile read and 1 volatile write
				TimestampSource ts = timestampSource;
				if(ts!=null) return ts;

				ts = new SpliceTimestampSource(ZkUtils.getRecoverableZooKeeper());
				//set the source for future callers
				timestampSource = ts; //volatile write
				return ts;
		}
}
