package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
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

/**
 * @author jessiezhang
 */
public class UnionOperationTest extends SpliceUnitTest {
	private static Logger LOG = Logger.getLogger(UnionOperationTest.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(UnionOperationTest.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("ST_MARS",UnionOperationTest.class.getSimpleName(),"(empId int, empNo int, name varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("ST_EARTH",UnionOperationTest.class.getSimpleName(),"(empId int, empNo int, name varchar(40))");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)		
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					Statement s = spliceClassWatcher.getStatement();
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_mars values(6, 1, 'Mulgrew, Kate')");
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_mars values(7, 1, 'Shatner, William')");
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_mars values(3, 1, 'Nimoy, Leonard')");
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_mars values(4, 1, 'Patrick')");
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_mars values(5, 1, null)");			
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_earth values(6, 1, 'Spiner, Brent')");
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_earth values(7, 1, 'Duncan, Rebort')");
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_earth values(3, 1, 'Nimoy, Leonard')");
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_earth values(4, 1, 'Ryan, Jeri')");
					s.execute("insert into " + UnionOperationTest.class.getSimpleName() + ".st_earth values(5, 1, null)");
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
	public void testUnionAll() throws Exception {			
		ResultSet rs = methodWatcher.executeQuery("select name from"+this.getPaddedTableReference("ST_MARS")+"UNION ALL select name from "+this.getPaddedTableReference("ST_EARTH"));
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("person name="+rs.getString(1));
		}	
		Assert.assertEquals(10, i);
	}
	
	@Test
	public void testUnionOneColumn() throws Exception {			
		ResultSet rs = methodWatcher.executeQuery("select name from"+this.getPaddedTableReference("ST_MARS")+"UNION select name from"+this.getPaddedTableReference("ST_EARTH"));
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("person name="+rs.getString(1));
		}	
		Assert.assertEquals(8, i);
	}		
	
	@Test
	/**
	 * 
	 * This needs to use a provider interface for boths its traversals and not use isScan - JL
	 * 
	 * @throws SQLException
	 */
	public void testValuesUnion() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("SELECT TTABBREV, TABLE_TYPE from (VALUES ('T','TABLE'), ('S','SYSTEM TABLE'), ('V', 'VIEW'), ('A', 'SYNONYM')) T (TTABBREV,TABLE_TYPE)");
		int i = 0;
		while (rs.next()) {
			i++;
		}	
		Assert.assertTrue(i>0);
	}
	
	@Test
	public void testUnion() throws Exception {			
		ResultSet rs = methodWatcher.executeQuery("select * from"+this.getPaddedTableReference("ST_MARS")+"UNION select * from"+this.getPaddedTableReference("ST_EARTH"));
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("id="+rs.getInt(1)+",person name="+rs.getString(2));
		}	
		Assert.assertEquals(8, i);
	}

    @Test
    public void testUnionWithWhereClause() throws Exception{
        /*
         * Regression test for Bug 373
         */
        ResultSet rs = methodWatcher.executeQuery("select * from "+spliceTableWatcher1.toString()+" where empId = 6 UNION select * from "+spliceTableWatcher2.toString()+" where empId=3");
        int i = 0;
        while (rs.next()) {
            i++;
            LOG.info("id="+rs.getInt(1)+",person name="+rs.getString(2));
        }
        Assert.assertEquals(2, i);
    }

    @Test
    public void testUnionValuesInSubSelect() throws Exception{
        /*
         * regression for Bug 292
         */
//        ResultSet rs = methodWatcher.executeQuery("select empId from "+spliceTableWatcher1.toString()+" where empId in (select empId from "+spliceTableWatcher2.toString()+" union values 1 union values 2)");
        ResultSet rs = methodWatcher.executeQuery("select empId from "+spliceTableWatcher1.toString()+" where empId in (select empId from "+spliceTableWatcher2.toString()+" union all values 1)");
        int i=0;
        while(rs.next()){
            i++;
            System.out.printf("empId=%d%n",rs.getInt(1));
        }
        Assert.assertEquals(5,i);
    }
}
