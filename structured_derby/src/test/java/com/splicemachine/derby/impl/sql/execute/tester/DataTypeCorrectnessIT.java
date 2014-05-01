package com.splicemachine.derby.impl.sql.execute.tester;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import org.junit.Assert;




public class DataTypeCorrectnessIT extends SpliceUnitTest {
	static boolean done=false;
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = DataTypeCorrectnessIT.class.getSimpleName().toUpperCase();
	protected static String TABLE_1 = "A";
	protected static String TABLE_2 = "B";
	protected static String TABLE_3 = "C";

	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_1,spliceSchemaWatcher.schemaName,"(boolean1 boolean, boolean2 boolean, smallint1 smallint, smallint2 smallint, integer1 integer,integer2 integer, bigint1 bigint,bigint2 bigint,decimal1 decimal,decimal2 decimal,real1 real, real2 real,double1 double,double2 double,float1 float,float2 float,char1 char(10),char2 char(10),varchar1 varchar(100),varchar2 varchar(100))");
//	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_2,spliceSchemaWatcher.schemaName," ( lvarchar1 long varchar , lvarchar2 long varchar ,date1 date,date2 date,time1 time,time2 time,timestamp1 timestamp,timestamp2 timestamp)");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_2,spliceSchemaWatcher.schemaName," ( lvarchar1 long varchar , lvarchar2 long varchar , charforbitdata1 char(24) for bit data,charforbitdata2 char(24) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data,date1 date,date2 date,time1 time,time2 time,timestamp1 timestamp,timestamp2 timestamp)");
	protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_3,spliceSchemaWatcher.schemaName," (charforbitdata1 char(24) for bit data, charforbitdata2 char(24) for bit data, varcharforbitdata1 varchar(1024) for bit data, varcharforbitdata2 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data)");




    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();


	
@ClassRule
public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
        .around(spliceSchemaWatcher)
        .around(spliceTableWatcher1)
        .around(spliceTableWatcher2)
        .around(spliceTableWatcher3)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
					try {
						PreparedStatement ps = spliceClassWatcher.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA('"+CLASS_NAME+"','"+TABLE_1+"',null,null,'"+getResource("testdata.csv")+"',',','\"',null,null,null)");
						ps.execute();
					} catch (Exception e) {
							e.printStackTrace();
							throw new RuntimeException(e);
					}
					finally {
							spliceClassWatcher.closeAll();
					}
			}

	});


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		//System.out.println("Here 1");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		//System.out.println("Here 2");
	}

	@Before
	public void setUp() throws Exception {
		//System.out.println("Here 3");
		if(!done){
		try{
			String i1="insert into "+CLASS_NAME+"."+TABLE_2+"(lvarchar1,lvarchar2,charforbitdata1,charforbitdata2,varcharforbitdata1,varcharforbitdata2,longvarcharforbitdata1,longvarcharforbitdata2,date1,date2,time1,time2,timestamp1,timestamp2) values ('aaaaaaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaa',X'ABCDEF',X'ABCDEF',X'1234abcdef',X'1234abcdef',X'1234abcdef',X'1234abcdef','2014-05-01','2014-05-01','05:05:05','05:05:05','2014-05-01 00:00:00','2014-05-01 00:00:00')";
			//System.out.println(i1); 
			String i2="insert into "+CLASS_NAME+"."+TABLE_2+"(lvarchar1,lvarchar2,charforbitdata1,charforbitdata2,varcharforbitdata1,varcharforbitdata2,longvarcharforbitdata1,longvarcharforbitdata2,date1,date2,time1,time2,timestamp1,timestamp2) values ('bbbbbbbbbbbbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbbbbbbbbbbbb',X'BCDEFA',X'BCDEFA',X'234abcdef1',X'234abcdef1',X'1234abcdef',X'1234abcdef','2014-05-01','2014-05-01','05:05:05','05:05:05','2014-05-01 00:00:00','2014-05-01 00:00:00')";
			//System.out.println(i2); 
			String i3="insert into "+CLASS_NAME+"."+TABLE_2+"(lvarchar1,lvarchar2,charforbitdata1,charforbitdata2,varcharforbitdata1,varcharforbitdata2,longvarcharforbitdata1,longvarcharforbitdata2,date1,date2,time1,time2,timestamp1,timestamp2) values ('cccccccccccccccccccccccccc','cccccccccccccccccccccccccc',X'CDEFAB',X'CDEFAB',X'34abcdef12',X'34abcdef12',X'1234abcdef',X'1234abcdef','2014-05-01','2014-05-01','05:05:05','05:05:05','2014-05-01 00:00:00','2014-05-01 00:00:00')";
			//System.out.println(i3); 
			int retval = methodWatcher.executeUpdate(i1);
			//System.out.println("return value 1 = "+retval);
			retval = methodWatcher.executeUpdate(i2);
			//System.out.println("return value 2 = "+retval);
			retval = methodWatcher.executeUpdate(i3);
			//System.out.println("return value 3 = "+retval);
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
		done=true;
		}
	}

	@After
	public void tearDown() throws Exception {
		//System.out.println("Here 4");
	}

	
	@Test
//	@Ignore
	public void testField1x() throws Exception{
		
		//charforbitdata1,charforbitdata2
		try {
			String returnval;
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where charforbitdata1 = X'bcdefa202020202020202020202020202020202020202020'");
			if(rs.next()){
				returnval = rs.getString("charforbitdata1");
				System.out.println("charforbitdata = "+returnval);
				Assert.assertTrue(returnval.equals("bcdefa202020202020202020202020202020202020202020"));
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar1 as varchar(128)) between 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and 'cccccccccccccccccccccccccc'");
			while(rs.next()){
				returnval = rs.getString("lvarchar1");
				System.out.println("lvarchar1 = "+returnval);
				Assert.assertTrue(returnval.equals("aaaaaaaaaaaaaaaaaaaaaaaaaa")||returnval.equals("bbbbbbbbbbbbbbbbbbbbbbbbbb")||returnval.equals("cccccccccccccccccccccccccc"));
			}
			rs.close();
// Group by for LONG VARCHAR are not supported.			
//			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" group by lvarchar1 having CAST(lvarchar1 as varchar(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
//				int retval = rs.getInt("retval");
//			if(rs.next()){
//				System.out.println("retval = "+retval);
//				Assert.assertEquals(1,retval);
//			}
//			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar1 as varchar(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar1 as varchar(128)) = 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ");
			if(rs.next()){
				int retval = rs.getInt("retval");
				System.out.println("retval = "+retval);
				Assert.assertEquals(2,retval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar2 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
			if(rs.next()){
				returnval = rs.getString("lvarchar2");
				System.out.println("smallint1 = "+returnval);
				Assert.assertTrue(returnval.equals("aaaaaaaaaaaaaaaaaaaaaaaaaa"));
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar2 AS VARCHAR(128)) between 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and 'cccccccccccccccccccccccccc'");
			while(rs.next()){
				returnval = rs.getString("lvarchar2");
				System.out.println("lvarchar1 = "+returnval);
				Assert.assertTrue(returnval.equals("aaaaaaaaaaaaaaaaaaaaaaaaaa")||returnval.equals("bbbbbbbbbbbbbbbbbbbbbbbbbb")||returnval.equals("cccccccccccccccccccccccccc"));
			}
			rs.close();
//Group by not supported.
//			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" group by lvarchar2 having CAST(lvarchar2 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar2 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or  CAST(lvarchar2 AS VARCHAR(128)) = 'bbbbbbbbbbbbbbbbbbbbbbbbbb'");
			if(rs.next()){
				int retval = rs.getInt("retval");
				System.out.println("retval = "+retval);
				Assert.assertEquals(2,retval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	
	private boolean runandtestqueryri(String query,int lookfor,String field) throws Exception{
		int returnval;
		try{
			ResultSet rs = methodWatcher.executeQuery(query);
			if(rs.next()){
				returnval = rs.getInt(field);
				Assert.assertEquals(lookfor,returnval);
			}
			rs.close();
		}catch(Exception e){
			throw new Exception(e);
		}
		return true;
	}

	private boolean runandtestqueryrd(String query,double lookfor,String field) throws Exception{
		double returnval;
		try{
			ResultSet rs = methodWatcher.executeQuery(query);
			if(rs.next()){
				returnval = rs.getDouble(field);
				Assert.assertTrue(Math.abs(lookfor-returnval)<0.0001);
			}
			rs.close();
		}catch(Exception e){
			throw new Exception(e);
		}
		return true;
	}
	private boolean runandtestqueryrs(String query,String lookfor,String field) throws Exception{
		String returnval;
		try{
			ResultSet rs = methodWatcher.executeQuery(query);
			if(rs.next()){
				returnval = rs.getString(field);
				Assert.assertTrue(lookfor.equals(returnval));
			}
			rs.close();
		}catch(Exception e){
			throw new Exception(e);
		}
		return true;
	}

	
	private boolean runandtestqueryR3(String query,int lookfor1,int lookfor2, int lookfor3,String field) throws Exception{
		int returnval;
		try{
			ResultSet rs = methodWatcher.executeQuery(query);
			while(rs.next()){
				returnval = rs.getInt(field);
				Assert.assertTrue(returnval==lookfor1||returnval==lookfor2||returnval==lookfor3);
			}
			rs.close();
		}catch(Exception e){
			throw new Exception(e);
		}
		return true;
	}
	
	private boolean runandtestqueryR3D(String query,double lookfor1,double lookfor2, double lookfor3,String field) throws Exception{
		Double returnval;
		try{
			ResultSet rs = methodWatcher.executeQuery(query);
			while(rs.next()){
				returnval = rs.getDouble(field);
				Assert.assertTrue(Math.abs(lookfor1-returnval)<0.0001||Math.abs(lookfor2-returnval)<0.0001||Math.abs(lookfor3-returnval)<0.0001);
			}
			rs.close();
		}catch(Exception e){
			throw new Exception(e);
		}
		return true;
	}

	private boolean runandtestqueryR3S(String query,String lookfor1,String lookfor2, String lookfor3,String field) throws Exception{
		String returnval;
		try{
			ResultSet rs = methodWatcher.executeQuery(query);
			while(rs.next()){
				returnval = rs.getString(field);
				Assert.assertTrue(returnval.equals(lookfor1)||returnval.equals(lookfor2)||returnval.equals(lookfor3));
			}
			rs.close();
		}catch(Exception e){
			throw new Exception(e);
		}
		return true;
	}
	
	
	@Test
	public void testField1() throws Exception{
		try {
			Assert.assertTrue(runandtestqueryri("select * from "+CLASS_NAME+"."+TABLE_1+" where smallint1 = 1",1,"smallint1"));
			Assert.assertTrue(runandtestqueryR3("select * from "+CLASS_NAME+"."+TABLE_1+" where smallint1 between 1 and 3",1,2,3,"smallint1"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by smallint1 having smallint1 = 2",1,"retval"));
		 	Assert.assertTrue(runandtestqueryri("select * from "+CLASS_NAME+"."+TABLE_1+" where smallint2 = 1",3,"smallint1"));
		 	Assert.assertTrue(runandtestqueryri("select * from "+CLASS_NAME+"."+TABLE_1+" where smallint2 between 1 and 3",3,"smallint2"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by smallint2 having smallint2 = 2",1,"retval"));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void testField2() throws Exception{
		try {
			Assert.assertTrue(runandtestqueryri("select * from "+CLASS_NAME+"."+TABLE_1+" where integer1 = 1",1,"integer1"));
			Assert.assertTrue(runandtestqueryR3("select * from "+CLASS_NAME+"."+TABLE_1+" where integer1 between 1 and 3",1,2,3,"integer1"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by integer1 having integer1 = 2",1,"retval"));
		 	Assert.assertTrue(runandtestqueryri("select * from "+CLASS_NAME+"."+TABLE_1+" where integer2 = 1",1,"integer2"));
		 	Assert.assertTrue(runandtestqueryR3("select * from "+CLASS_NAME+"."+TABLE_1+" where integer2 between 1 and 3",1,2,3,"integer2"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by integer2 having integer2 = 2",1,"retval"));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	 
	
	@Test
	public void testField3() throws Exception{
		try {
			Assert.assertTrue(runandtestqueryri("select * from "+CLASS_NAME+"."+TABLE_1+" where bigint1 = 1",1,"bigint1"));
			Assert.assertTrue(runandtestqueryR3("select * from "+CLASS_NAME+"."+TABLE_1+" where bigint1 between 1 and 3",1,2,3,"bigint1"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by bigint1 having bigint1 = 2",1,"retval"));
		 	Assert.assertTrue(runandtestqueryri("select * from "+CLASS_NAME+"."+TABLE_1+" where bigint2 = 1",1,"bigint2"));
		 	Assert.assertTrue(runandtestqueryR3("select * from "+CLASS_NAME+"."+TABLE_1+" where bigint2 between 1 and 3",1,2,3,"bigint2"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by bigint2 having bigint2 = 2",1,"retval"));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	 
	
	@Test
	public void testField4() throws Exception{
		double returnval;
		try {
			Assert.assertTrue(runandtestqueryrd("select * from "+CLASS_NAME+"."+TABLE_1+" where decimal1 = 1",1.0,"decimal1"));
			Assert.assertTrue(runandtestqueryR3D("select * from "+CLASS_NAME+"."+TABLE_1+" where decimal1 between 1.0 and 3.0",1.0,2.0,3.0,"decimal1"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by decimal1 having decimal1 = 2.0",1,"retval"));
			Assert.assertTrue(runandtestqueryrd("select * from "+CLASS_NAME+"."+TABLE_1+" where decimal2 = 1.0",1.0,"decimal2"));
			Assert.assertTrue(runandtestqueryR3D("select * from "+CLASS_NAME+"."+TABLE_1+" where decimal2 between 1 and 3",1.0,2.0,3.0,"decimal2"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by decimal2 having decimal2 = 2.0",1,"retval"));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	 
	
	@Test
	public void testField5() throws Exception{
		try {
			Assert.assertTrue(runandtestqueryrd("select * from "+CLASS_NAME+"."+TABLE_1+" where real1 = 1",1.0,"real1"));
			Assert.assertTrue(runandtestqueryR3D("select * from "+CLASS_NAME+"."+TABLE_1+" where real1 between 1.5 and 2.5",1.5,2.0,2.5,"real1"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by real1 having real1 = 2.0",1,"retval"));
			Assert.assertTrue(runandtestqueryrd("select * from "+CLASS_NAME+"."+TABLE_1+" where real2 = 1",1.0,"real2"));
			Assert.assertTrue(runandtestqueryR3D("select * from "+CLASS_NAME+"."+TABLE_1+" where real2 between 1.5 and 2.5",1.5,2.0,2.5,"real2"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by real2 having real2 = 2.0",1,"retval"));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	 
	
	@Test
	public void testField6() throws Exception{
		double returnval;
		try {
			Assert.assertTrue(runandtestqueryrd("select * from "+CLASS_NAME+"."+TABLE_1+" where double1 = 1",1.0,"double1"));
			Assert.assertTrue(runandtestqueryR3D("select * from "+CLASS_NAME+"."+TABLE_1+" where double1 between 1.5 and 2.5",1.5,2.0,2.5,"double1"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by double1 having double1 = 2.0",1,"retval"));
			Assert.assertTrue(runandtestqueryrd("select * from "+CLASS_NAME+"."+TABLE_1+" where double2 = 1",1.0,"double2"));
			Assert.assertTrue(runandtestqueryR3D("select * from "+CLASS_NAME+"."+TABLE_1+" where double2 between 1.5 and 2.5",1.5,2.0,2.5,"double2"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by double2 having double2 = 2.0",1,"retval"));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	
	@Test
	public void testField7() throws Exception{
		double returnval;
		try {
			Assert.assertTrue(runandtestqueryrd("select * from "+CLASS_NAME+"."+TABLE_1+" where float1 = 1",1.0,"float1"));
			Assert.assertTrue(runandtestqueryR3D("select * from "+CLASS_NAME+"."+TABLE_1+" where float1 between 1.5 and 2.5",1.5,2.0,2.5,"float1"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by float1 having float1 = 2.0",1,"retval"));
			Assert.assertTrue(runandtestqueryrd("select * from "+CLASS_NAME+"."+TABLE_1+" where float2 = 1",1.0,"float2"));
			Assert.assertTrue(runandtestqueryR3D("select * from "+CLASS_NAME+"."+TABLE_1+" where float2 between 1.5 and 2.5",1.5,2.0,2.5,"float2"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by float2 having float2 = 2.0",1,"retval"));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	
	@Test
	public void testField8() throws Exception{
		String returnval;
		try {
			Assert.assertTrue(runandtestqueryrs("select * from "+CLASS_NAME+"."+TABLE_1+" where char1 = 'a'","a","char1"));
			Assert.assertTrue(runandtestqueryR3S("select * from "+CLASS_NAME+"."+TABLE_1+" where char1 between 'a' and 'c'","a","b","c","char1"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by char1 having char1 = 'b'",1,"retval"));
			Assert.assertTrue(runandtestqueryrs("select * from "+CLASS_NAME+"."+TABLE_1+" where char2 = 'a'","a","char2"));
			Assert.assertTrue(runandtestqueryR3S("select * from "+CLASS_NAME+"."+TABLE_1+" where char2 between 'a' and 'c'","a","b","c","char2"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by char2 having char2 = 'b'",1,"retval"));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		Assert.assertTrue(true);
	}
	 
	 
	
	@Test
	public void testField9() throws Exception{
		String returnval;
		try {
			Assert.assertTrue(runandtestqueryrs("select * from "+CLASS_NAME+"."+TABLE_1+" where varchar1 = 'a'","a","varchar1"));
			Assert.assertTrue(runandtestqueryR3S("select * from "+CLASS_NAME+"."+TABLE_1+" where varchar1 between 'a' and 'c'","a","b","c","varchar1"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by varchar1 having varchar1 = 'b'",1,"retval"));
			Assert.assertTrue(runandtestqueryrs("select * from "+CLASS_NAME+"."+TABLE_1+" where varchar2 = 'a'","a","varchar2"));
			Assert.assertTrue(runandtestqueryR3S("select * from "+CLASS_NAME+"."+TABLE_1+" where varchar2 between 'a' and 'c'","a","b","c","varchar2"));
		 	Assert.assertTrue(runandtestqueryri("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by varchar2 having varchar2 = 'b'",1,"retval"));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	
	@Test
//	@Ignore
	public void testField10() throws Exception{
		try {
			String returnval;
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar1 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
			if(rs.next()){
				returnval = rs.getString("lvarchar1");
				System.out.println("lvarchar1 = "+returnval);
				Assert.assertTrue("aaaaaaaaaaaaaaaaaaaaaaaaaa".equals(returnval));
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar1 as varchar(128)) between 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and 'cccccccccccccccccccccccccc'");
			while(rs.next()){
				returnval = rs.getString("lvarchar1");
				System.out.println("lvarchar1 = "+returnval);
				Assert.assertTrue(returnval.equals("aaaaaaaaaaaaaaaaaaaaaaaaaa")||returnval.equals("bbbbbbbbbbbbbbbbbbbbbbbbbb")||returnval.equals("cccccccccccccccccccccccccc"));
			}
			rs.close();
// Group by for LONG VARCHAR are not supported.			
//			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" group by lvarchar1 having CAST(lvarchar1 as varchar(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
//			if(rs.next()){
//				int retval = rs.getInt("retval");
//				System.out.println("retval = "+retval);
//				Assert.assertEquals(1,retval);
//			}
//			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar1 as varchar(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar1 as varchar(128)) = 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ");
			if(rs.next()){
				int retval = rs.getInt("retval");
				System.out.println("retval = "+retval);
				Assert.assertEquals(2,retval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar2 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
			if(rs.next()){
				returnval = rs.getString("lvarchar2");
				System.out.println("smallint1 = "+returnval);
				Assert.assertTrue(returnval.equals("aaaaaaaaaaaaaaaaaaaaaaaaaa"));
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar2 AS VARCHAR(128)) between 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and 'cccccccccccccccccccccccccc'");
			while(rs.next()){
				returnval = rs.getString("lvarchar2");
				System.out.println("lvarchar1 = "+returnval);
				Assert.assertTrue(returnval.equals("aaaaaaaaaaaaaaaaaaaaaaaaaa")||returnval.equals("bbbbbbbbbbbbbbbbbbbbbbbbbb")||returnval.equals("cccccccccccccccccccccccccc"));
			}
			rs.close();
//Group by not supported.
//			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" group by lvarchar2 having CAST(lvarchar2 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" where CAST(lvarchar2 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or  CAST(lvarchar2 AS VARCHAR(128)) = 'bbbbbbbbbbbbbbbbbbbbbbbbbb'");
			if(rs.next()){
				int retval = rs.getInt("retval");
				System.out.println("retval = "+retval);
				Assert.assertEquals(2,retval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	
	
	public static String getResource(String name) {
			return getResourceDirectory()+"/datatypedata/"+name;
//			return "/Users/leightj/Documents/workspace/data/testdata/"+name;
	}

	
}
