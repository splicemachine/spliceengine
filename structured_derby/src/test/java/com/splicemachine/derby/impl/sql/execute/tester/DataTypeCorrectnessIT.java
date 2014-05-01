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
	@Ignore
	public void testField1x() throws Exception{
		try {
			String returnval;
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where lvarchar1 = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
			if(rs.next()){
				returnval = rs.getString("lvarchar1");
				//System.out.println("lvarchar1 = "+returnval);
				Assert.assertTrue("aaaaaaaaaaaaaaaaaaaaaaaaaa".equals(returnval));
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where lvarchar1 between 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and 'cccccccccccccccccccccccccc'");
			while(rs.next()){
				returnval = rs.getString("lvarchar1");
				//System.out.println("lvarchar1 = "+returnval);
				Assert.assertTrue(returnval.equals("aaaaaaaaaaaaaaaaaaaaaaaaaa")||returnval.equals("bbbbbbbbbbbbbbbbbbbbbbbbbb")||returnval.equals("cccccccccccccccccccccccccc"));
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" group by lvarchar1 having lvarchar1 = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where lvarchar2 = 1");
			if(rs.next()){
				returnval = rs.getString("lvarchar2");
				//System.out.println("smallint1 = "+returnval);
				Assert.assertEquals(3,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_2+" where lvarchar1 between between 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and 'cccccccccccccccccccccccccc'");
			if(rs.next()){
				returnval = rs.getString("lvarchar2");
				//System.out.println("smallint2 = "+returnval);
				Assert.assertEquals(3,returnval);
			}
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_2+" group by lvarchar2 having lvarchar2 = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	
	
	
	@Test
	public void testField1() throws Exception{
		int returnval;
		try {
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where smallint1 = 1");
			if(rs.next()){
				returnval = rs.getInt("smallint1");
				//System.out.println("smallint1 = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where smallint1 between 1 and 3");
			if(rs.next()){
				returnval = rs.getInt("smallint1");
				//System.out.println("smallint1 = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by smallint1 having smallint1 = 2");
			if(rs.next()){
				returnval = rs.getInt("retval");
				//System.out.println("retval = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where smallint2 = 1");
			if(rs.next()){
				returnval = rs.getInt("smallint1");
				//System.out.println("smallint1 = "+returnval);
				Assert.assertEquals(3,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where smallint2 between 1 and 3");
			if(rs.next()){
				returnval = rs.getInt("smallint2");
				//System.out.println("smallint2 = "+returnval);
				Assert.assertEquals(3,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by smallint2 having smallint2 = 2");
			if(rs.next()){
				returnval = rs.getInt("retval");
				//System.out.println("retval = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	 
	
	@Test
	public void testField2() throws Exception{
		int returnval;
		try {
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where integer1 = 1");
			if(rs.next()){
				returnval = rs.getInt("integer1");
				//System.out.println("integer1 = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where integer1 between 1 and 3");
			if(rs.next()){
				returnval = rs.getInt("integer1");
				//System.out.println("integer1 = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by integer1 having integer1 = 2");
			if(rs.next()){
				returnval = rs.getInt("retval");
				//System.out.println("retval = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where integer2 = 1");
			if(rs.next()){
				returnval = rs.getInt("integer2");
				//System.out.println("integer2 = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where integer2 between 1 and 3");
			if(rs.next()){
				returnval = rs.getInt("integer2");
				//System.out.println("integer2 = "+returnval);
				Assert.assertEquals(3,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by integer2 having integer2 = 2");
			if(rs.next()){
				returnval = rs.getInt("retval");
				//System.out.println("retval = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	 
	
	@Test
	public void testField3() throws Exception{
		int returnval;
		try {
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where bigint1 = 1");
			if(rs.next()){
				returnval = rs.getInt("bigint1");
				//System.out.println("bigint1 = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where bigint1 between 1 and 3");
			if(rs.next()){
				returnval = rs.getInt("bigint1");
				//System.out.println("bigint1 = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by bigint1 having bigint1 = 2");
			if(rs.next()){
				returnval = rs.getInt("retval");
				//System.out.println("retval = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where bigint2 = 1");
			if(rs.next()){
				returnval = rs.getInt("bigint2");
				//System.out.println("bigint2 = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where bigint2 between 1 and 3");
			if(rs.next()){
				returnval = rs.getInt("bigint2");
				//System.out.println("bigint2 = "+returnval);
				Assert.assertEquals(3,returnval);
			}
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by bigint2 having bigint2 = 2");
			if(rs.next()){
				returnval = rs.getInt("retval");
				//System.out.println("retval = "+returnval);
				Assert.assertEquals(1,returnval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	 
	
	@Test
	public void testField4() throws Exception{
		double returnval;
		try {
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where decimal1 = 1");
			if(rs.next()){
				returnval = rs.getDouble("decimal1");
				//System.out.println("decimal1 = "+returnval);
				Assert.assertTrue(Math.abs(1.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where decimal1 between 1 and 3");
			if(rs.next()){
				returnval = rs.getDouble("decimal1");
				//System.out.println("decimal1 = "+returnval);
				Assert.assertTrue(Math.abs(1.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by decimal1 having decimal1 = 2");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where decimal2 = 1");
			if(rs.next()){
				returnval = rs.getDouble("decimal2");
				//System.out.println("decimal2 = "+returnval);
				Assert.assertTrue(Math.abs(1.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where decimal2 between 1 and 3");
			if(rs.next()){
				returnval = rs.getDouble("decimal2");
				//System.out.println("decimal2 = "+returnval);
				Assert.assertTrue(Math.abs(3.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by decimal2 having decimal2 = 2");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		Assert.assertTrue(true);
	}
	 
	
	@Test
	public void testField5() throws Exception{
		double returnval;
		try {
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where real1 = 1.0");
			if(rs.next()){
				returnval = rs.getDouble("real1");
				//System.out.println("real1 = "+returnval);
				Assert.assertTrue(Math.abs(1.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where real1 between 1.5 and 2.5");
			if(rs.next()){
				returnval = rs.getDouble("real1");
				//System.out.println("real1 = "+returnval);
				Assert.assertTrue(Math.abs(2.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by real1 having real1 = 2.0");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where real2 = 1.0");
			if(rs.next()){
				returnval = rs.getDouble("real2");
				//System.out.println("real2 = "+returnval);
				Assert.assertTrue(Math.abs(1.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where real2 between 1.5 and 2.5");
			if(rs.next()){
				returnval = rs.getDouble("real2");
				//System.out.println("real2 = "+returnval);
				Assert.assertTrue(Math.abs(2.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by real2 having real2 = 2.0");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	 
	
	@Test
	public void testField6() throws Exception{
		double returnval;
		try {
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where double1 = 1.0");
			if(rs.next()){
				returnval = rs.getDouble("double1");
				//System.out.println("double1 = "+returnval);
				Assert.assertTrue(Math.abs(1.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where double1 between 1.5 and 2.5");
			if(rs.next()){
				returnval = rs.getDouble("double1");
				//System.out.println("double1 = "+returnval);
				Assert.assertTrue(Math.abs(2.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by double1 having double1 = 2.0");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where double2 = 1.0");
			if(rs.next()){
				returnval = rs.getDouble("double2");
				//System.out.println("double2 = "+returnval);
				Assert.assertTrue(Math.abs(1.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where double2 between 1.5 and 2.5");
			if(rs.next()){
				returnval = rs.getDouble("double2");
				//System.out.println("double2 = "+returnval);
				Assert.assertTrue(Math.abs(2.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by double2 having double2 = 2.0");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		Assert.assertTrue(true);
	}

	
	@Test
	public void testField7() throws Exception{
		double returnval;
		try {
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where float1 = 1.0");
			if(rs.next()){
				returnval = rs.getDouble("float1");
				//System.out.println("float1 = "+returnval);
				Assert.assertTrue(Math.abs(1.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where float1 between 1.5 and 2.5");
			if(rs.next()){
				returnval = rs.getDouble("float1");
				//System.out.println("float1 = "+returnval);
				Assert.assertTrue(Math.abs(2.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by float1 having float1 = 2.0");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where float2 = 1.0");
			if(rs.next()){
				returnval = rs.getDouble("float2");
				//System.out.println("float2 = "+returnval);
				Assert.assertTrue(Math.abs(1.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where float2 between 1.5 and 2.5");
			if(rs.next()){
				returnval = rs.getDouble("float2");
				//System.out.println("float2 = "+returnval);
				Assert.assertTrue(Math.abs(2.0-returnval)<0.0001);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by float2 having float2 = 2.0");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	
	@Test
	public void testField8() throws Exception{
		String returnval;
		try {
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where char1 = 'a'");
			if(rs.next()){
				returnval = rs.getString("char1");
				//System.out.println("smallint1 = "+returnval);
				Assert.assertEquals("a",returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where char1 between 'a' and 'c'");
			if(rs.next()){
				returnval = rs.getString("char1");
				//System.out.println("char1 = "+returnval);
				Assert.assertEquals("a",returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by char1 having char1 = 'b'");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where char2 = 'a'");
			if(rs.next()){
				returnval = rs.getString("char2");
				//System.out.println("char2 = "+returnval);
				Assert.assertEquals("a",returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where char2 between 'a' and 'c'");
			while(rs.next()){
				returnval = rs.getString("char2");
				//System.out.println("char2 = "+returnval);
				Assert.assertTrue("abc".contains(returnval));
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by char2 having char2 = 'b'");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			Assert.assertTrue(true);;
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
			ResultSet rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where varchar1 = 'a'");
			if(rs.next()){
				returnval = rs.getString("varchar1");
				//System.out.println("varchar1 = "+returnval);
				Assert.assertEquals("a",returnval);
			}
			rs.close();
	    	rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where varchar1 between 'a' and 'c'");
			if(rs.next()){
				returnval = rs.getString("varchar1");
				//System.out.println("varchar1 = "+returnval);
				Assert.assertEquals("a",returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by varchar1 having varchar1 = 'b'");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where varchar2 = 'a'");
			if(rs.next()){
				returnval = rs.getString("varchar2");
				//System.out.println("varchar2 = "+returnval);
				Assert.assertEquals("a",returnval);
			}
			rs.close();
			rs = methodWatcher.executeQuery("select * from "+CLASS_NAME+"."+TABLE_1+" where varchar2 between 'a' and 'c'");
			while(rs.next()){
				returnval = rs.getString("varchar2");
				//System.out.println("varchar2 = "+returnval);
				Assert.assertTrue("abc".contains(returnval));
			}
			rs.close();
			rs = methodWatcher.executeQuery("select count(*) as retval from "+CLASS_NAME+"."+TABLE_1+" group by varchar2 having varchar2 = 'b'");
			if(rs.next()){
				int retval = rs.getInt("retval");
				//System.out.println("retval = "+retval);
				Assert.assertEquals(1,retval);
			}
			rs.close();
			Assert.assertTrue(true);;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		Assert.assertTrue(true);
	}
	
	
	public static String getResource(String name) {
			return getResourceDirectory()+"/datatypedata/"+name;
//			return "/Users/leightj/Documents/workspace/data/testdata/"+name;
	}

	
}
