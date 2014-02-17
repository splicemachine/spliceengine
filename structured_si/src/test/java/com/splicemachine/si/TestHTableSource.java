package com.splicemachine.si;

import com.google.common.base.Preconditions;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.coprocessors.SIObserverUnPacked;
import com.splicemachine.si.data.hbase.HTableSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestHTableSource implements HTableSource {
		private Map<String, HTable> hTables = new HashMap<String, HTable>();

		private final HBaseTestingUtility testCluster;
		private final String[] families;

		public TestHTableSource(HBaseTestingUtility testCluster,  String[] families) {
				this.testCluster = testCluster;
				this.families = families;
//				addTable(testCluster, tableName, families);
		}

		public void addTable(HBaseTestingUtility testCluster, String tableName, String[] families) {
				byte[][] familyBytes = getFamilyBytes(families);
				try {
						hTables.put(tableName, testCluster.createTable(Bytes.toBytes(tableName), familyBytes, Integer.MAX_VALUE));
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
		}

		public void addUnpackedTable(String tableName) throws IOException {
				byte[][] familyBytes = getFamilyBytes(families);

				Preconditions.checkArgument(!hTables.containsKey(tableName),"Table already exists");
				HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(tableName));
				descriptor.addCoprocessor(SIObserverUnPacked.class.getName());
				for(byte[] familyName:familyBytes){
						HColumnDescriptor family = new HColumnDescriptor(familyName);
						family.setInMemory(true);
						descriptor.addFamily(family);
				}
				testCluster.getHBaseAdmin().createTable(descriptor);
				HTable table = new HTable(new Configuration(testCluster.getConfiguration()), tableName);
				hTables.put(tableName,table);
		}

		public void addPackedTable(String tableName) throws IOException {
				byte[][] familyBytes = getFamilyBytes(families);

				Preconditions.checkArgument(!hTables.containsKey(tableName),"Table already exists");
				HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(tableName));
				descriptor.addCoprocessor(SIObserver.class.getName());
				for(byte[] familyName:familyBytes){
						HColumnDescriptor family = new HColumnDescriptor(familyName);
						family.setInMemory(true);
						descriptor.addFamily(family);
				}
				testCluster.getHBaseAdmin().createTable(descriptor);
				HTable table = new HTable(new Configuration(testCluster.getConfiguration()), tableName);
				hTables.put(tableName,table);
		}

		@Override
		public HTable getTable(String tableName) {
				return hTables.get(tableName);
		}

		protected byte[][] getFamilyBytes(String[] families) {
				byte[][] familyBytes = new byte[families.length][];
				int i = 0;
				for (String f : families) {
						familyBytes[i] = Bytes.toBytes(f);
						i++;
				}
				return familyBytes;
		}
}
