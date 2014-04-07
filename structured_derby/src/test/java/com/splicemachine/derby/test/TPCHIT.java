package com.splicemachine.derby.test;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.derby.tools.ij;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.google.common.io.Closeables;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class TPCHIT extends SpliceUnitTest {
		protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
		public static final String CLASS_NAME = "TPCH1X";
		public static final String CLASS_NAME1 = "TPCH1X";
		protected static final String LINEITEM = "LINEITEM";
		protected static final String ORDERS = "ORDERS";
		protected static final String CUSTOMERS = "CUSTOMER";
		protected static final String PARTSUPP = "PARTSUPP";
		protected static final String SUPPLIER = "SUPPLIER";
		protected static final String PART = "PART";
		protected static final String NATION = "NATION";
		protected static final String REGION = "REGION";

		protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
		protected static SpliceTableWatcher lineItemTable = new SpliceTableWatcher(LINEITEM,CLASS_NAME,
						"( L_ORDERKEY INTEGER NOT NULL,L_PARTKEY INTEGER NOT NULL,"+
										"L_SUPPKEY INTEGER NOT NULL, L_LINENUMBER  INTEGER NOT NULL, L_QUANTITY DECIMAL(15,2), L_EXTENDEDPRICE DECIMAL(15,2),"+
										"L_DISCOUNT DECIMAL(15,2), L_TAX DECIMAL(15,2), L_RETURNFLAG  CHAR(1), L_LINESTATUS CHAR(1), L_SHIPDATE DATE,"+
										"L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT CHAR(25),L_SHIPMODE CHAR(10),L_COMMENT VARCHAR(44),PRIMARY KEY(L_ORDERKEY,L_LINENUMBER))");
		protected static SpliceTableWatcher orderTable = new SpliceTableWatcher(ORDERS,CLASS_NAME,
						"( O_ORDERKEY INTEGER NOT NULL PRIMARY KEY,O_CUSTKEY INTEGER,O_ORDERSTATUS CHAR(1),"+
										"O_TOTALPRICE DECIMAL(15,2),O_ORDERDATE DATE,"+
										"O_ORDERPRIORITY  CHAR(15), O_CLERK CHAR(15), O_SHIPPRIORITY INTEGER, O_COMMENT VARCHAR(79))");
		protected static SpliceTableWatcher customerTable = new SpliceTableWatcher(CUSTOMERS,CLASS_NAME,
						"( C_CUSTKEY INTEGER NOT NULL PRIMARY KEY, C_NAME VARCHAR(25), C_ADDRESS VARCHAR(40), C_NATIONKEY INTEGER NOT NULL,"+
										"C_PHONE CHAR(15), C_ACCTBAL DECIMAL(15,2), C_MKTSEGMENT  CHAR(10), C_COMMENT VARCHAR(117))");
		protected static SpliceTableWatcher partSuppTable = new SpliceTableWatcher(PARTSUPP,CLASS_NAME,
						"( PS_PARTKEY INTEGER NOT NULL, PS_SUPPKEY INTEGER NOT NULL, PS_AVAILQTY INTEGER,"+
										"PS_SUPPLYCOST  DECIMAL(15,2),PS_COMMENT     VARCHAR(199), PRIMARY KEY(PS_PARTKEY,PS_SUPPKEY))");
		protected static SpliceTableWatcher supplierTable = new SpliceTableWatcher(SUPPLIER,CLASS_NAME,
						"( S_SUPPKEY INTEGER NOT NULL PRIMARY KEY,S_NAME VARCHAR(25) ,S_ADDRESS VARCHAR(40),"+
										"S_NATIONKEY INTEGER ,S_PHONE CHAR(15) ,S_ACCTBAL DECIMAL(15,2),S_COMMENT VARCHAR(101))");
		protected static SpliceTableWatcher partTable = new SpliceTableWatcher(PART,CLASS_NAME,
						"(P_PARTKEY INTEGER NOT NULL PRIMARY KEY, P_NAME VARCHAR(55), P_MFGR CHAR(25), P_BRAND CHAR(10),"+
										"P_TYPE VARCHAR(25), P_SIZE INTEGER, P_CONTAINER CHAR(10), P_RETAILPRICE DECIMAL(15,2), P_COMMENT VARCHAR(23))");
		protected static SpliceTableWatcher nationTable = new SpliceTableWatcher(NATION,CLASS_NAME,
						"(N_NATIONKEY INTEGER NOT NULL, N_NAME VARCHAR(25), N_REGIONKEY INTEGER NOT NULL,"+
										"N_COMMENT VARCHAR(152), primary key (N_NATIONKEY))");
		protected static SpliceTableWatcher regionTable = new SpliceTableWatcher(REGION,CLASS_NAME,
						"(R_REGIONKEY INTEGER NOT NULL PRIMARY KEY, R_NAME VARCHAR(25), R_COMMENT VARCHAR(152))");

		@ClassRule
		public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
						.around(spliceSchemaWatcher)
						.around(lineItemTable)
						.around(orderTable)
						.around(customerTable)
						.around(partSuppTable)
						.around(supplierTable)
						.around(partTable)
						.around(nationTable)
						.around(regionTable)
						.around(new SpliceDataWatcher(){
								@Override
								protected void starting(Description description) {
										try {
												PreparedStatement ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)",CLASS_NAME,LINEITEM,getResource("lineitem.tbl")));
												ps.execute();
												ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)",CLASS_NAME,ORDERS,getResource("orders.tbl")));
												ps.execute();
												ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)",CLASS_NAME,CUSTOMERS,getResource("customer.tbl")));
												ps.execute();
												ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)",CLASS_NAME,PARTSUPP,getResource("partsupp.tbl")));
												ps.execute();
												ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)",CLASS_NAME,SUPPLIER,getResource("supplier.tbl")));
												ps.execute();
												ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)",CLASS_NAME,PART,getResource("part.tbl")));
												ps.execute();
												ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)",CLASS_NAME,NATION,getResource("nation.tbl")));
												ps.execute();
												ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)",CLASS_NAME,REGION,getResource("region.tbl")));
												ps.execute();
										} catch (Exception e) {
												throw new RuntimeException(e);
										}
										finally {
												spliceClassWatcher.closeAll();
										}
								}

						});

		@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

		@Test
		public void validateDataLoad() throws Exception {
				ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME1,LINEITEM));
				rs.next();
				Assert.assertEquals(9958, rs.getLong(1));
//				Assert.assertEquals(6001215, rs.getLong(1));
				rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME1,ORDERS));
				rs.next();
				Assert.assertEquals(2500, rs.getLong(1));
//				Assert.assertEquals(1500000, rs.getLong(1));
				rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME1,CUSTOMERS));
				rs.next();
				Assert.assertEquals(250, rs.getLong(1));
//				Assert.assertEquals(150000, rs.getLong(1));
				rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME1,PARTSUPP));
				rs.next();
				Assert.assertEquals(1332, rs.getLong(1));
//				Assert.assertEquals(800000, rs.getLong(1));
				rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME1,SUPPLIER));
				rs.next();
				Assert.assertEquals(16, rs.getLong(1));
//				Assert.assertEquals(10000, rs.getLong(1));
				rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME1,PART));
				rs.next();
				Assert.assertEquals(333, rs.getLong(1));
//				Assert.assertEquals(200000, rs.getLong(1));
				rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME1,NATION));
				rs.next();
				Assert.assertEquals(25, rs.getLong(1));
				rs = methodWatcher.executeQuery(format("select count(*) from %s.%s",CLASS_NAME1,REGION));
				rs.next();
				Assert.assertEquals(5, rs.getLong(1));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql1() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("1.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql2() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("2.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql3() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("3.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql4() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("4.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql5() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("5.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql6() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("6.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql7() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("7.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql8() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("8.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//        @Ignore("Bugzilla 829: Not resilient in face of random task failure")
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql9() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("9.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("Repeated test for bug 829. Only enable if you want to wait around for a while")
		public void testRepeatedSql9() throws Exception {
				for(int i=0;i<100;i++){
						sql9();
						System.out.printf("Iteration %d succeeded%n",i);
				}
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql10() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("10.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql11() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("11.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql12() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("12.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql13() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("13.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql14() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("14.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql15() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("15.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql16() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("16.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql17() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("17.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql18() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("18.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql19() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("19.sql")),methodWatcher.getOrCreateConnection()));
		}
//		@Test
//		@Ignore("Repeated test for bug 829. Only enable if you want to wait around for a while")
//		public void testRepeatedSql19() throws Exception {
//				for(int i=0;i<24;i++){
//						sql19();
//						System.out.printf("Iteration %d succeeded%n",i);
//				}
//		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql20() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("20.sql")),methodWatcher.getOrCreateConnection()));
		}

		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql21() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("21.sql")),methodWatcher.getOrCreateConnection()));
		}
		
		@Test
//		@Ignore("DoRoc. Only want to run sql19")
		public void sql22() throws Exception {
				Assert.assertTrue(runScript(new File(getSQLFile("22.sql")),methodWatcher.getOrCreateConnection()));
		}
		
		public static String getResource(String name) {
//			String t1 = getResourceDirectory()+"tcph/data/"+name;
//			String t2 = "~/Documents/workspace/data/TPCH-1G/"+name;
				return getResourceDirectory()+"tcph/data/"+name;
//				return "~/Documents/workspace/data/TPCH-1GB/"+name;
		}

		protected static String getSQLFile(String name) {
				return getResourceDirectory()+"tcph/query/"+name;
		}


		protected static boolean runScript(File scriptFile, Connection connection) {
				FileInputStream fileStream = null;
				try {
						fileStream = new FileInputStream(scriptFile);
						int result  = ij.runScript(connection,fileStream,"UTF-8",System.out,"UTF-8");
						return (result==0);
				}
				catch (Exception e) {
						return false;
				}
				finally {
						Closeables.closeQuietly(fileStream);
				}
		}

}
