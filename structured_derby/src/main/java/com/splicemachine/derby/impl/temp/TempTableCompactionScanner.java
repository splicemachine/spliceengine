package com.splicemachine.derby.impl.temp;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.List;

/**
 * Scanner for use with the TEMP table compaction.
 *
 * TEMP table data is inherently temporary--once a job has completed,
 * the data that it has stored in TEMP is no longer necessary and can be
 * deleted.
 *
 * Now, we could delete those rows directly using HBase's interface, but that has
 * a number of negative consequences:
 *
 * 1. It's slow: We have to visit every record we want to delete
 * 2. It's expensive: each delete is an individual WAL edit, which is an individual
 * IO operation
 * 3. It causes compaction churn: Because each delete is a WAL edit, we end up with
 * a LOT of compactions, which hurt our operation performance.
 *
 * Point 3 is especially important--every time TEMP compacts using the standard
 * HBase compaction process, it causes write performance to that region to fall
 * precipitously.
 *
 * So we really want to delete old data in a way that doesn't cause very expensive
 * compactions. While we're at it, we also want TEMP compactions to be much less
 * expensive than they are by default.
 *
 * Thus, we take advantage of the encoded format for TEMP rows to create a
 * more efficient compaction scanner that removes old data without requiring
 * excessive IO.
 *
 * <b>Algorithm</b>
 *
 * Rows in the TEMP table always have the following row key format:
 *
 * [1-byte hash bucket] [Snowflake-generated job id] [grouping hash] [postfix bytes]
 *
 * The job id is especially useful, because Snowflake uses Timestamps to ensure uniqueness.
 * Timestamps are handy because we can get access to the Maximum timestamp for an entire store file,
 * which means that if we see a StoreFile who's maximum timestamp is below the timestamp for the
 * oldest currently active job, then we know it consists entirely of data which can be discarded,
 * and we can eliminate the file entirely without ever reading it.
 *
 * Since all operations should eventually end, this means that we will eventually remove all data
 * from TEMP without ever actually reading any of the data. This will clean out old data AND provide
 * a lightweight compaction mechanism.
 *
 * @author Scott Fines
 * Date: 11/19/13
 */
public class TempTableCompactionScanner implements InternalScanner {
		@Override
		public boolean next(List<KeyValue> results) throws IOException {
				return false;  //To change body of implemented methods use File | Settings | File Templates.
		}

		@Override
		public boolean next(List<KeyValue> results, String metric) throws IOException {
				return false;  //To change body of implemented methods use File | Settings | File Templates.
		}

		@Override
		public boolean next(List<KeyValue> result, int limit) throws IOException {
				return false;  //To change body of implemented methods use File | Settings | File Templates.
		}

		@Override
		public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
				return false;  //To change body of implemented methods use File | Settings | File Templates.
		}

		@Override
		public void close() throws IOException {
				//To change body of implemented methods use File | Settings | File Templates.
		}
}
