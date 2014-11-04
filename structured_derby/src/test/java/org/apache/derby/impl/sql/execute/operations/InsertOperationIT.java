package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.framework.*;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.runner.Description;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

public class InsertOperationIT extends SpliceUnitTest {
		protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
		private static final Logger LOG = Logger.getLogger(InsertOperationIT.class);

		protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(InsertOperationIT.class.getSimpleName());
		protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("T",InsertOperationIT.class.getSimpleName(),"(name varchar(40))");
		protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("S",InsertOperationIT.class.getSimpleName(),"(name varchar(40))");
		protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("A",InsertOperationIT.class.getSimpleName(),"(name varchar(40), count int)");
		protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("G",InsertOperationIT.class.getSimpleName(),"(name varchar(40))");
		protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("B",InsertOperationIT.class.getSimpleName(),"(name varchar(40))");
		protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher("E",InsertOperationIT.class.getSimpleName(),"(name varchar(40))");
		protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher("J",InsertOperationIT.class.getSimpleName(),"(name varchar(40))");
		protected static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher("L",InsertOperationIT.class.getSimpleName(),"(name varchar(40))");
		protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher("Y",InsertOperationIT.class.getSimpleName(),"(name varchar(40))");
		protected static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher("Z",InsertOperationIT.class.getSimpleName(),"(name varchar(40),count int)");
		protected static SpliceTableWatcher spliceTableWatcher11 = new SpliceTableWatcher("FILES",InsertOperationIT.class.getSimpleName(),"(name varchar(32) not null primary key, doc blob(50M))");
		protected static SpliceTableWatcher spliceTableWatcher12 = new SpliceTableWatcher("HMM",InsertOperationIT.class.getSimpleName(),"(b16a char(2) for bit data, b16b char(2) for bit data, vb16a varchar(2) for bit data, vb16b varchar(2) for bit data, lbv long varchar for bit data)");
		protected static SpliceTableWatcher spliceTableWatcher13 = new SpliceTableWatcher("WARNING",InsertOperationIT.class.getSimpleName(),"(a char(1))");
		protected static SpliceTableWatcher spliceTableWatcher14 = new SpliceTableWatcher("T1",InsertOperationIT.class.getSimpleName(),"(c1 int generated always as identity, c2 int)");
    protected static SpliceTableWatcher spliceTableWatcher15 = new SpliceTableWatcher("T2",InsertOperationIT.class.getSimpleName(),"(a int, b int)");
    protected static SpliceTableWatcher spliceTableWatcher16 = new SpliceTableWatcher("T3",InsertOperationIT.class.getSimpleName(),"(a int, b decimal(16,10))");
    protected static SpliceTableWatcher spliceTableWatcher17 = new SpliceTableWatcher("T4",InsertOperationIT.class.getSimpleName(),"(c int, d int)");
    protected static SpliceTableWatcher spliceTableWatcher18 = new SpliceTableWatcher("T5",InsertOperationIT.class.getSimpleName(),"(a int, c int,b decimal(16,10), d int)");
		protected static SpliceTableWatcher sameLengthTable = new SpliceTableWatcher("SAME_LENGTH",InsertOperationIT.class.getSimpleName(),"(name varchar(40))");


		@ClassRule
		public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
						.around(spliceSchemaWatcher)
						.around(spliceTableWatcher1)
						.around(spliceTableWatcher2)
						.around(spliceTableWatcher3)
						.around(spliceTableWatcher4)
						.around(spliceTableWatcher5)
						.around(spliceTableWatcher6)
						.around(spliceTableWatcher7)
						.around(spliceTableWatcher8)
						.around(spliceTableWatcher9)
						.around(spliceTableWatcher10)
						.around(spliceTableWatcher11)
						.around(spliceTableWatcher13)
						.around(spliceTableWatcher12)
						.around(sameLengthTable)
						.around(spliceTableWatcher14)
            .around(spliceTableWatcher15)
            .around(spliceTableWatcher16)
            .around(spliceTableWatcher17)
            .around(spliceTableWatcher18).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (a,b) values (?,?)",spliceTableWatcher16));
                        ps.setInt(1,1);ps.setBigDecimal(2, BigDecimal.ONE); ps.execute();

                        ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (c,d) values (?,?)",spliceTableWatcher17));
                        ps.setInt(1,1);ps.setInt(2,1); ps.execute();
                        ps.setInt(1,2);ps.setInt(2,2); ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

		@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testInsertOverMergeSortOuterJoinIsCorrect() throws Exception {
        /*
         * Regression test for DB-1833. Tests that we can insert over a subselect that has a merge-sort
         * join present,without getting any errors.
         */
         long insertCount = methodWatcher.executeUpdate(String.format("insert into %1$s select " +
                "%2$s.a,%3$s.c,%2$s.b,%3$s.d " +
                "from %2$s --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n" +
                "right join %3$s on %2$s.a=%3$s.c",spliceTableWatcher18,spliceTableWatcher16,spliceTableWatcher17));
        Assert.assertEquals("Incorrect number of rows inserted!",2,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from "+spliceTableWatcher18);
        int count = 0;
        while(rs.next()){
            int a = rs.getInt(1);
            if(rs.wasNull()){
                BigDecimal b = rs.getBigDecimal(3);
                Assert.assertTrue("B is not null!",rs.wasNull());
            }else{
                BigDecimal b = rs.getBigDecimal(3);
                Assert.assertFalse("B is null!", rs.wasNull());
                Assert.assertTrue("Incorrect B value!", BigDecimal.ONE.subtract(b).abs().compareTo(new BigDecimal(".0000000001"))<0);
            }
            count++;
            int c = rs.getInt(2);
            Assert.assertFalse("C is null!", rs.wasNull());
            int d = rs.getInt(4);
            Assert.assertFalse("D is null!",rs.wasNull());
        }
        Assert.assertEquals("Incorrect row count!",2,count);
    }

    @Test
		public void testDataTruncationWarningIsEmitted() throws Exception {
				PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ spliceTableWatcher13+" values cast(? as char(1))");
				ps.setString(1,"12");
				int updated = ps.executeUpdate();
				Assert.assertEquals("Incorrect number of rows updated!",1,updated);

				SQLWarning warning = ps.getWarnings();
				String sqlState = warning.getSQLState();
				Assert.assertEquals("Incorrect warning code returned!","01004",sqlState);
		}

		@Test
		public void testInsertMultipleRecordsWithSameLength() throws Exception{
				/*Regression test for DB-1278*/
				Statement s = methodWatcher.getStatement();
				s.execute("insert into "+sameLengthTable+" (name) values ('ab'),('de'),('fg')");
				List<String> correctNames = Arrays.asList("ab","de","fg");
				Collections.sort(correctNames);
				ResultSet rs = methodWatcher.executeQuery("select * from "+sameLengthTable);
				List<String> names = new ArrayList<String>();
				while(rs.next()){
						names.add(rs.getString(1));
				}
				Collections.sort(names);
				Assert.assertEquals("returned named incorrect!",correctNames,names);
		}

		@Test
		public void testInsertMultipleRecords() throws Exception{
				Statement s = methodWatcher.getStatement();
				s.execute("insert into"+this.getPaddedTableReference("T")+"(name) values ('gdavis'),('mzweben'),('rreimer')");
				List<String> correctNames = Arrays.asList("gdavis","mzweben","rreimer");
				Collections.sort(correctNames);
				ResultSet rs = methodWatcher.executeQuery("select * from"+this.getPaddedTableReference("T"));
				List<String> names = new ArrayList<String>();
				while(rs.next()){
						names.add(rs.getString(1));
				}
				Collections.sort(names);
				Assert.assertEquals("returned named incorrect!",correctNames,names);
		}

		@Test
		public void testInsertSingleRecord() throws Exception{
				Statement s = methodWatcher.getStatement();
				s.execute("insert into"+this.getPaddedTableReference("S")+"(name) values ('gdavis')");
				ResultSet rs = methodWatcher.executeQuery("select * from "+this.getPaddedTableReference("S"));
				int count = 0;
				while(rs.next()){
						count++;
						Assert.assertNotNull(rs.getString(1));
				}
				Assert.assertEquals("Incorrect Number of Results Returned",1, count);
		}

		@Test
		public void testInsertFromSubselect() throws Exception{
				Statement s = methodWatcher.getStatement();
				s.execute("insert into"+this.getPaddedTableReference("G")+"values('sfines')");
				s.execute("insert into"+this.getPaddedTableReference("G")+"values('jzhang')");
				s.execute("insert into"+this.getPaddedTableReference("G")+"values('jleach')");
				methodWatcher.commit();
				List<String> correctNames = Arrays.asList("sfines","jzhang","jleach");
				Collections.sort(correctNames);
				//copy that data into table t
				s = methodWatcher.getStatement();
				s.execute("insert into"+this.getPaddedTableReference("B")+"(name) select name from"+this.getPaddedTableReference("G"));
				methodWatcher.commit();
				ResultSet rs = methodWatcher.executeQuery("select * from"+this.getPaddedTableReference("B"));
				List<String> names = new ArrayList<String>();
				while(rs.next()){
						LOG.info("name="+rs.getString(1));
						names.add(rs.getString(1));
				}
				Collections.sort(names);
				Assert.assertEquals("returned named incorrect!",correctNames,names);
				methodWatcher.commit();
		}

		@Test
		public void testInsertVarBit() throws Exception{
				methodWatcher.executeUpdate("insert into"+this.getPaddedTableReference("HMM")+" values(X'11', X'22', X'33', X'44', X'55')");
		}


		@Test
		public void testInsertReportsCorrectReturnedNumber() throws Exception{
				PreparedStatement ps = methodWatcher.prepareStatement("insert into"+this.getPaddedTableReference("E")+"(name) values (?)");
				ps.setString(1,"bob");
				int returned = ps.executeUpdate();
				Assert.assertEquals("incorrect update count returned!",1,returned);
		}
		/**
		 *
		 * The idea here is to test that PreparedStatement inserts won't barf if you do
		 * multiple inserts with different where clauses each time
		 * @throws Exception
		 */
		@Test
		public void testInsertFromBoundedSubSelectThatChanges() throws Exception{
				Statement s = methodWatcher.getStatement();
				s.execute("insert into"+this.getPaddedTableReference("L")+"(name) values ('gdavis'),('mzweben'),('rreimer')");
				PreparedStatement ps = methodWatcher.prepareStatement("insert into "+this.getPaddedTableReference("J")+" (name) select name from "+this.getPaddedTableReference("L")+" a where a.name = ?");
				ps.setString(1,"rreimer");
				ps.executeUpdate();

				ResultSet rs = methodWatcher.executeQuery("select * from "+this.getPaddedTableReference("J"));
				int count=0;
				while(rs.next()){
						Assert.assertEquals("Incorrect name inserted!","rreimer",rs.getString(1));
						count++;
				}
				Assert.assertEquals("Incorrect number of results returned!",1,count);
				ps.setString(1,"mzweben");
				ps.executeUpdate();
				List<String> correct = Arrays.asList("rreimer","mzweben");
				rs = methodWatcher.executeQuery("select * from"+this.getPaddedTableReference("J"));
				count=0;
				while(rs.next()){
						String next = rs.getString(1);
						boolean found=false;
						for(String correctName:correct){
								if(correctName.equals(next)){
										found=true;
										break;
								}
						}
						Assert.assertTrue("Value "+ next+" unexpectedly appeared!",found);
						count++;
				}
				Assert.assertEquals("Incorrect number of results returned!",correct.size(),count);
		}

		@Test
//    @Ignore("Transiently fails during Maven build, but passes when run locally. Gotta figure that out first")
		public void testInsertFromSubOperation() throws Exception{
				Map<String,Integer> nameCountMap = Maps.newHashMap();
				Statement s = methodWatcher.getStatement();
				s.execute("insert into" +this.getPaddedTableReference("Y")+ "values('sfines')");
				s.execute("insert into" +this.getPaddedTableReference("Y")+ "values('sfines')");
				nameCountMap.put("sfines",2);
				s.execute("insert into" +this.getPaddedTableReference("Y")+  "values('jzhang')");
				s.execute("insert into"  +this.getPaddedTableReference("Y")+  "values('jzhang')");
				s.execute("insert into"  +this.getPaddedTableReference("Y")+  "values('jzhang')");
				nameCountMap.put("jzhang", 3);
				s.execute("insert into" +this.getPaddedTableReference("Y")+ "values('jleach')");
				nameCountMap.put("jleach",1);
				methodWatcher.commit();
//		methodWatcher.splitTable("Y",this.getSchemaName());
				s = methodWatcher.getStatement();
				int returned = s.executeUpdate("insert into "+spliceTableWatcher10+"(name,count) select name,count(name) from "+ spliceTableWatcher9+" group by name");
				methodWatcher.commit();
				ResultSet rs = methodWatcher.executeQuery("select * from"+this.getPaddedTableReference("Z"));
				int groupCount=0;
				while(rs.next()){
						String name = rs.getString(1);
						Integer count = rs.getInt(2);
						Assert.assertNotNull("Name is null!",name);
						Assert.assertNotNull("Count is null!",count);
						int correctCount = nameCountMap.get(name);
						Assert.assertEquals("Incorrect count returned for name "+name,correctCount,count.intValue());
						groupCount++;
				}
				Assert.assertEquals("Incorrect number of groups returned!",nameCountMap.size(),groupCount);
		}

		@Test
		public void testInsertBlob() throws Exception{
				InputStream fin = new FileInputStream(getResourceDirectory()+"order_line_500K.csv");
				PreparedStatement ps = methodWatcher.prepareStatement("insert into"+this.getPaddedTableReference("FILES")+"(name, doc) values (?,?)");
				ps.setString(1, "csv_file");
				ps.setBinaryStream(2, fin);
				ps.execute();
				ResultSet rs = methodWatcher.executeQuery("SELECT doc FROM"+this.getPaddedTableReference("FILES")+"WHERE name = 'csv_file'");
				byte buff[] = new byte[1024];
				while (rs.next()) {
						Blob ablob = rs.getBlob(1);
						File newFile = new File(getBaseDirectory()+"/target/order_line_500K.csv");
						if (newFile.exists()) {
								newFile.delete();
						}
						newFile.createNewFile();
						InputStream is = ablob.getBinaryStream();
						FileOutputStream fos = new FileOutputStream(newFile);
						for (int b = is.read(buff); b != -1; b = is.read(buff)) {
								fos.write(buff, 0, b);
						}
						is.close();
						fos.close();
				}
				File file1 = new File(getResourceDirectory()+"order_line_500K.csv");
				File file2 = new File(getBaseDirectory()+"/target/order_line_500K.csv");
				Assert.assertTrue("The files contents are not equivalent",FileUtils.contentEquals(file1, file2));
		}

		@Test
		public void testInsertIdentitySingleAndFromSelfScan() throws Exception {
				methodWatcher.executeUpdate("insert into"+this.getPaddedTableReference("T1")+"(c2) values (1)");
				methodWatcher.executeUpdate("insert into"+this.getPaddedTableReference("T1")+"(c2) values (1)");
				methodWatcher.executeUpdate("insert into"+this.getPaddedTableReference("T1")+"(c2) select c1 from" + this.getPaddedTableReference("T1"));
				ResultSet rs = methodWatcher.executeQuery("select c1, c2 from"+this.getPaddedTableReference("T1"));
				int i = 0;
				while (rs.next()) {
						i++;
						Assert.assertTrue("These numbers should be contiguous",rs.getInt(1) >= 1 && rs.getInt(1) <= 4);
				}
				Assert.assertEquals("Should have returned 4 rows from identity insert",4,i);
		}

    @Test
    public void testRepeatedInsertOverSelectReportsCorrectNumbers() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        //insert a single record
        conn.createStatement().executeUpdate(String.format("insert into %s (a,b) values (1,1)",spliceTableWatcher15));
        PreparedStatement ps = conn.prepareStatement(String.format("insert into %1$s (a,b) select * from %1$s",spliceTableWatcher15));
        int iterCount = 10;
        for(int i=0;i<iterCount;i++){
            int updateCount = ps.executeUpdate();
            System.out.printf("updateCount=%d%n",updateCount);
//            Assert.assertEquals("Reported incorrect value!",(1<<i),count);
            ResultSet rs = conn.createStatement().executeQuery(String.format("select count(*) from %s",spliceTableWatcher15));
            Assert.assertTrue("Did not return rows for a count query!",rs.next());
            long count = rs.getLong(1);
            System.out.printf("scanCount=%d%n",count);
            Assert.assertEquals("Incorrect inserted records!",(1<<(i+1)),count);
        }

        ResultSet rs = conn.createStatement().executeQuery(String.format("select count(*) from %s",spliceTableWatcher15));
        Assert.assertTrue("Did not return rows for a count query!",rs.next());
        long count = rs.getLong(1);
        Assert.assertEquals("Incorrect inserted records!",(1<<iterCount),count);
    }
}
