package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class InsertOperationTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static final Logger LOG = Logger.getLogger(InsertOperationTest.class);

	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(InsertOperationTest.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("T",InsertOperationTest.class.getSimpleName(),"(name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("S",InsertOperationTest.class.getSimpleName(),"(name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("A",InsertOperationTest.class.getSimpleName(),"(name varchar(40), count int)");
	protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("G",InsertOperationTest.class.getSimpleName(),"(name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("B",InsertOperationTest.class.getSimpleName(),"(name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher("E",InsertOperationTest.class.getSimpleName(),"(name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher("J",InsertOperationTest.class.getSimpleName(),"(name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher("L",InsertOperationTest.class.getSimpleName(),"(name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher("Y",InsertOperationTest.class.getSimpleName(),"(name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher("Z",InsertOperationTest.class.getSimpleName(),"(name varchar(40))");
	
	
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
		.around(spliceTableWatcher10);	
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
	
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
    @Ignore("Ignoring until joins are handled properly")
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
    @Ignore("Transiently fails during Maven build, but passes when run locally. Gotta figure that out first")
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
		methodWatcher.splitTable("Y",this.getSchemaName());
		s = methodWatcher.getStatement();
		int returned = s.executeUpdate("insert into"+this.getPaddedTableReference("Z")+"(name,count) select name,count(name) from"+ this.getPaddedTableReference("Y")+"group by name");
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
	
}
