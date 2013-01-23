package com.ir.hbase.endpoint;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface CountGroupByEndpointProtocol extends CoprocessorProtocol {
	Map<Integer, Long> getCount(byte[] groupByFam, byte[] groupByCol)
			throws IOException;
}
