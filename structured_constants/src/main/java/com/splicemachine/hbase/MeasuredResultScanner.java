package com.splicemachine.hbase;

import com.splicemachine.metrics.TimeView;
import org.apache.hadoop.hbase.client.ResultScanner;


/**
 * @author Scott Fines
 *         Date: 7/29/14
 */
public interface MeasuredResultScanner extends ResultScanner {

    TimeView getRemoteReadTime();
		long getRemoteBytesRead();
		long getRemoteRowsRead();

		TimeView getLocalReadTime();
		long getLocalBytesRead();
		long getLocalRowsRead();
}
