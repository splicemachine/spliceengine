package com.splicemachine.hbase;

import com.splicemachine.stats.TimeView;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

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
