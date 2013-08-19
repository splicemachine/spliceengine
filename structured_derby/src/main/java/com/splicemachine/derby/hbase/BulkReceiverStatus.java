package com.splicemachine.derby.hbase;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Created on: 8/19/13
 */
@MXBean
public interface BulkReceiverStatus {

    public long getTotalBulkWritesReceived();

    public long getTotalRowsReceived();

    public long getTotalBytesReceived();

    public long getTotalRowsFailed();

    public long getTotalErrorsThrown();

    public long getTotalDeleteFirstAfterCalls();
}
