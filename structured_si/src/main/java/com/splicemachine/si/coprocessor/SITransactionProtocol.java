package com.splicemachine.si.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface SITransactionProtocol extends CoprocessorProtocol {
	public void registerTXNCallback(long startTimestamp, byte[] beginKey, byte[] endKey) throws IOException;
	public void rollTXNForward(SITransactionResponse transactionResponse) throws IOException;
}
