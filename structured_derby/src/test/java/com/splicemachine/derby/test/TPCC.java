package com.splicemachine.derby.test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
@Ignore
public class TPCC extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = OperationStatsTest.class.getSimpleName().toUpperCase();
	protected static final String STOCK = "STOCK";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(STOCK,CLASS_NAME,"(s_w_id int NOT NULL, s_i_id int NOT NULL,s_quantity decimal(7,0) NOT NULL, PRIMARY KEY (s_w_id,s_i_id))");

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement s = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?, ?, ?)", CLASS_NAME, STOCK));
					for (int i = 0; i< 50000; i++) {
						if (i%500 == 0)
							System.out.println("i: " + i);
						s.setInt(1, i);
						s.setInt(2, i);
						s.setBigDecimal(3, new BigDecimal(i));
						s.execute();
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
	public void testUpdateTCPC() throws Exception {
			PreparedStatement s = methodWatcher.prepareStatement(String.format("update %s.%s set s_quantity = s_quantity+1 where s_w_id = ? and s_i_id = ?", CLASS_NAME, STOCK));
			for (int i = 0; i<1000; i++) {
				s.setInt(1, i);
				s.setInt(2, i);
				s.executeUpdate();
			}
	}	

	@Test
	public void testSelectTCPC() throws Exception {
			PreparedStatement s = methodWatcher.prepareStatement(String.format("select * from %s.%s where s_w_id = ? and s_i_id = ?", CLASS_NAME, STOCK));
			for (int i = 0; i<1000; i++) {
				s.setInt(1, i);
				s.setInt(2, i);
				ResultSet rs = s.executeQuery();
				while (rs.next()) {

				}
			}
	}	


}