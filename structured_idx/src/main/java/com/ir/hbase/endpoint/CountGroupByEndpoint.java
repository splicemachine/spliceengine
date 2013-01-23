package com.ir.hbase.endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class CountGroupByEndpoint extends BaseEndpointCoprocessor implements CountGroupByEndpointProtocol {

	@Override
	public Map<Integer, Long> getCount(byte[] groupByFam, byte[] groupByCol) throws IOException {
		Scan scan = new Scan();
		scan.addColumn(groupByFam, groupByCol);
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion().getScanner(scan);
		Map<Integer, Long> count = new HashMap<Integer, Long>();
		try {
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			do {
				curVals.clear();
				done = scanner.next(curVals);
				Integer value = Bytes.toInt(curVals.get(0).getValue());
				if (count.containsKey(value)) {
					count.put(value, count.get(value) + 1); 
				} else {
					count.put(value, 1L);
				}
			} while (done);
		} finally {
			scanner.close();
		}
		return count;
	}
}
