package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.junit.Assert;
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
/**
 *      Per RowCountNode.java, the RowCounts are for the following types of conditions:
 *	    SELECT * FROM T FETCH FIRST ROW ONLY
 * 		SELECT * FROM T ORDER BY I OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY
 * 		SELECT * FROM T OFFSET 100 ROWS
 *
 *
 */
//@Ignore("Bug 334")
public class RowCountOperationTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = RowCountOperationTest.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME = "A";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME,"(i int)");

    private static final int size = 10;
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement s = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?)",CLASS_NAME,TABLE_NAME));
					for (int i = 1; i<=size;i++) {
						s.setInt(1, i);
						s.executeUpdate();
					}
                    spliceClassWatcher.splitTable(TABLE_NAME,CLASS_NAME,size/3);
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
	public void testCountOffsetFirstRow() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select * from %s fetch first row only",this.getTableReference(TABLE_NAME)));
		int i=0;
		while(rs.next()){
			i++;
            int val = rs.getInt(1);
            System.out.printf("val=%d%n",val);
//			Assert.assertEquals(i,rs.getInt(1));
		}
		Assert.assertEquals(1, i);	
	}

	
	@Test
	public void testCountOffset() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s offset 5 rows",this.getTableReference(TABLE_NAME)));
		int i=0;
		while(rs.next()){
			i++;
            int val = rs.getInt(1);
            System.out.printf("val=%d%n",val);
//			Assert.assertEquals(i,val);
		}
		Assert.assertEquals(size-5, i);
	}
	
	@Test
	public void testOffsetFetchNext() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s offset 3 rows fetch next 2 rows only",this.getTableReference(TABLE_NAME)));
		int i=0;
		while(rs.next()){
			i++;
//			Assert.assertEquals(i,rs.getInt(1));
		}
		Assert.assertEquals(2, i);
	}

	
}
