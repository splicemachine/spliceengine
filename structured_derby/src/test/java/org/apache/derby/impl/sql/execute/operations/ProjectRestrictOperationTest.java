package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * This tests basic table scans with and without projection/restriction
 */
public class ProjectRestrictOperationTest extends SpliceUnitTest  {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static Logger LOG = Logger.getLogger(ProjectRestrictOperationTest.class);
	public static final String CLASS_NAME = ProjectRestrictOperationTest.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME = "A";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME,"(si varchar(40),sa varchar(40),sc int)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
				PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?,?,?)",CLASS_NAME,TABLE_NAME));
				for(int i=0;i<10;i++){
					ps.setString(1,Integer.toString(i));
					ps.setString(2,Integer.toString(i) + " Times ten");
					ps.setInt(3,i);
					ps.executeUpdate();
				}
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
	public void testResrictWrapperOnTableScanPreparedStatement() throws Exception {
		PreparedStatement s = methodWatcher.prepareStatement(format("select * from %s where si like ?",this.getTableReference(TABLE_NAME)));
		s.setString(1,"%5%");
		ResultSet rs = s.executeQuery();
		int i = 0;
		while (rs.next()) {
			i++;
			Assert.assertNotNull(rs.getString(1));
			Assert.assertNotNull(rs.getString(2));
		}
		Assert.assertEquals(1, i);
	}

	@Test
	public void testRestrictOnAnythingTableScan() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select * from %s %s",this.getTableReference(TABLE_NAME),"where si like '%'"));
		int count=0;
		while(rs.next()){
			Assert.assertNotNull("a.si is null!",rs.getString(1));
			Assert.assertNotNull("b.si is null!",rs.getString(2));
			count++;
		}
		Assert.assertEquals("Incorrect number of rows returned!",10,count);
	}

	@Test
	public void testResrictWrapperOnTableScan() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select * from %s %s",this.getTableReference(TABLE_NAME),"where si like '%5%'"));
		int i = 0;
		while (rs.next()) {
			i++;
			Assert.assertNotNull(rs.getString(1));
			Assert.assertNotNull(rs.getString(2));
		}
		Assert.assertEquals(1, i);
	}

	@Test
	public void testProjectRestrictWrapperOnTableScan() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select si || 'Chicken Dumplings' from %s %s",this.getTableReference(TABLE_NAME),"where si like '%5%'"));
		int i = 0;
		while (rs.next()) {
			i++;
			Assert.assertNotNull(rs.getString(1));
			Assert.assertEquals("String Concatenation Should Match", "5"+"Chicken Dumplings", rs.getString(1));
		}
		Assert.assertEquals(1, i);
	}

	@Test
	public void testInPredicate() throws Exception{
		List<String> corrects = Arrays.asList("1","2","4");
		String correctPredicate = Joiner.on("','").join(corrects);
		ResultSet rs = methodWatcher.executeQuery(format("select * from %s where si in ('"+correctPredicate+"')",this.getTableReference(TABLE_NAME)));
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String si = rs.getString(1);
			String sa = rs.getString(2);
			Assert.assertTrue("incorrect si returned!",corrects.contains(si));
			Assert.assertNotNull("incorrect sa!",sa);
			results.add(String.format("si=%s,sa=%s",si,sa));
		}
		Assert.assertEquals("Incorrect num rows returned!",corrects.size(),results.size());
	}

	@Test
	public void testSumOperator() throws Exception{
		int increment = 2;
		ResultSet rs = methodWatcher.executeQuery(format("select sc, sc+"+increment+" from %s",this.getTableReference(TABLE_NAME)));
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			int sc = rs.getInt(1);
			int scIncr = rs.getInt(2);
			Assert.assertEquals("Sum doesn't work!",sc+increment,scIncr);
			results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testTimesOperator() throws Exception{
		int multiplier = 2;
		ResultSet rs = methodWatcher.executeQuery(format("select sc, sc*"+multiplier+" from %s",this.getTableReference(TABLE_NAME)));
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			int sc = rs.getInt(1);
			int scIncr = rs.getInt(2);
			Assert.assertEquals("Sum doesn't work!",sc*multiplier,scIncr);
			results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testModulusOperator() throws Exception{
		int modulus = 2;
		ResultSet rs = methodWatcher.executeQuery(format("select sc, mod(sc,"+modulus+") from %s",this.getTableReference(TABLE_NAME)));
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			int sc = rs.getInt(1);
			int scIncr = rs.getInt(2);
			Assert.assertEquals("Modulus doesn't work!",sc%modulus,scIncr);
			results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testSubStringOperator() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select sa, substr(sa,1,1) from %s",this.getTableReference(TABLE_NAME)));
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			String sa = rs.getString(1);
			String sub = rs.getString(2);
			String subSa = sa.substring(0,1);
			Assert.assertEquals("Incorrect sub!",subSa,sub);
			results.add(String.format("sa=%s,sub=%s",sa,sub));
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testConcatOperator() throws Exception{
		String concat = "b";
		ResultSet rs = methodWatcher.executeQuery(format("select si,si||'"+concat+"' from %s",this.getTableReference(TABLE_NAME)));
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			String sa = rs.getString(1);
			String concatAttempt = rs.getString(2);
			String correct = sa+concat;
			Assert.assertEquals("Incorrect concat!",correct,concatAttempt);
			results.add(String.format("sa=%s,concatAttempt=%s",sa,concatAttempt));
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testUpperLowerOperator() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select sa,UPPER(sa),LOWER(sa) from %s",this.getTableReference(TABLE_NAME)));
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			String sa = rs.getString(1);
			String upper = rs.getString(2);
			String lower = rs.getString(3);
			String correctUpper = sa.toUpperCase();
			String correctLower = sa.toLowerCase();
			Assert.assertEquals("upper incorrect",correctUpper,upper);
			Assert.assertEquals("lower incorrect",correctLower,lower);
			results.add(String.format("sa:%s,upper:%s,lower:%s",sa,upper,lower));
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

    @Test
    public void testConstantIntCompareFalseFilter() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("select * from " + this.getPaddedTableReference("A") +"where 2 < 1");
        Assert.assertFalse("1 or more results were found when none were expected",rs.next());
    }

    @Test
    public void testConstantBooleanFalseFilter() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("select * from " + this.getPaddedTableReference("A") +"where false");
        Assert.assertFalse("1 or more results were found when none were expected",rs.next());
    }
}
