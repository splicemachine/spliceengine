package com.splicemachine.si.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface SIWriteProtocol extends CoprocessorProtocol {
	public SIWriteResponse puts(List<Put> puts) throws IOException;
	public SIWriteResponse deletes(List<Delete> deletes) throws IOException;	
}
