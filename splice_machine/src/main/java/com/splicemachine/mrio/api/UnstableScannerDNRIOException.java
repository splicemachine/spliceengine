package com.splicemachine.mrio.api;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class UnstableScannerDNRIOException extends DoNotRetryIOException {
	private static final long serialVersionUID = -175989532140489323L;

}
