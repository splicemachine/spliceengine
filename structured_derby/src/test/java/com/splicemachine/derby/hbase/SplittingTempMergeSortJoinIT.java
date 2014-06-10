package com.splicemachine.derby.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.test.framework.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 6/9/14
 */
public class SplittingTempMergeSortJoinIT {

		private static final String SCHEMA_NAME=SplittingTempMergeSortJoinIT.class.getSimpleName().toUpperCase();

		@BeforeClass
		public static void setup() throws Exception {
				HBaseAdmin admin = new HBaseAdmin(new Configuration());
				HTableDescriptor htd = admin.getTableDescriptor(SpliceConstants.TEMP_TABLE_BYTES);
				// This parameters cause the Temp regions to split more often
				htd.setMemStoreFlushSize(5 * 1024 * 1024); // 10 MB
				htd.setMaxFileSize(5 * 1024 * 1024); // 10 MB
//				htd.setMaxFileSize(10 * 1024 * 1024*1024l); // 10 GB
				admin.disableTable(SpliceConstants.TEMP_TABLE_BYTES);
				admin.modifyTable(SpliceConstants.TEMP_TABLE_BYTES, htd);
				admin.enableTable(SpliceConstants.TEMP_TABLE_BYTES);
				admin.close();
		}

		private static String TABLE_NAME_1 = "selfjoin";
		protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(
						SCHEMA_NAME);
		protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(
						SCHEMA_NAME);
		protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,
						SCHEMA_NAME, "(i int, j int,k double,l double)");

		@ClassRule
		public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
						.around(spliceSchemaWatcher)
						.around(spliceTableWatcher1)
						.around(new SpliceDataWatcher() {
								@Override
								protected void starting(Description description) {
										try {
												PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (i,j,k,l) values (?,?,?,?)",spliceTableWatcher1.toString()));
												int batchSize = 1000;
												for(int i=0;i<10000;i++){
														ps.setInt(1,i);ps.setInt(2,i+1);ps.setDouble(3,i);ps.setDouble(4,i);ps.addBatch();
														if(i%batchSize==0){
																ps.executeBatch();
																System.out.printf("Written %d records%n", i);
														}
												}												ps.executeBatch();
										} catch (Exception e) {
												throw new RuntimeException(e);
										}finally{
												spliceClassWatcher.closeAll();
										}
								}
						});

		@Rule
		public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(SCHEMA_NAME);

		@Test
		public void testOneSelfMergeSortJoin() throws Exception {
				ResultSet rs = methodWatcher.executeQuery(
								join(
												"select * from ",
												TABLE_NAME_1 + " a ",
												"inner join " + TABLE_NAME_1 + " b --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
												"on a.i = b.i "));
				//manually add up and count records
				int currentSumFor1 =0;
				int count=0;
				while(rs.next()){
						int val = rs.getInt(1);
						Assert.assertFalse("Null record seen!", rs.wasNull());
						currentSumFor1+=val;
						count++;
				}

				Assert.assertEquals("No results",10000,count);
				Assert.assertEquals("incorrect sum!",(10000*9999)/2,currentSumFor1);
		}

		@Test
		public void testRepeatedOneMergeSortJoin() throws Throwable {
				HBaseAdmin admin = new HBaseAdmin(new Configuration());
				for(int i=0;i<1000;i++){
						List<HRegionInfo> tableRegions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
						int numRegions = tableRegions.size();
						try{
								testOneSelfMergeSortJoin();
								tableRegions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
								System.out.println("iteration " + i + " successful. Temp space went from " + numRegions + " to " + tableRegions.size() + " during this run");
						}catch(Throwable e){
								tableRegions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
								System.out.println("iteration " + i + " failed. Temp space went from " + numRegions + " to " + tableRegions.size() + " during this run");
								throw e;
						}
				}

		}

		private String join(String... strings) {
				StringBuilder sb = new StringBuilder();
				for (String s : strings) {
						sb.append(s).append('\n');
				}
				return sb.toString();
		}
}
