package com.splicemachine.derby.impl.storage;

import com.splicemachine.stats.Counter;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 10/29/13
 */
public interface SpliceResultScanner extends ResultScanner {

    void open() throws IOException, StandardException;

		TimeView getRemoteReadTime();
		long getRemoteBytesRead();
		long getRemoteRowsRead();

		TimeView getLocalReadTime();
		long getLocalBytesRead();
		long getLocalRowsRead();

}
