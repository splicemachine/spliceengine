package com.splicemachine.si2;

import com.splicemachine.si2.data.hbase.HTableSource;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestHTableSource implements HTableSource {
	private Map<String, HTable> hTables = new HashMap<String, HTable>();

	public TestHTableSource(HBaseTestingUtility testCluster, String tableName, String[] families) {
		addTable(testCluster, tableName, families);
	}

	public void addTable(HBaseTestingUtility testCluster, String tableName, String[] families) {
		byte[][] familyBytes = new byte[families.length][];
		int i = 0;
		for (String f : families) {
			familyBytes[i] = Bytes.toBytes(f);
			i++;
		}
		try {
			hTables.put(tableName, testCluster.createTable(Bytes.toBytes(tableName), familyBytes, Integer.MAX_VALUE));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public HTable getTable(String tableName) {
		return hTables.get(tableName);
	}
}
