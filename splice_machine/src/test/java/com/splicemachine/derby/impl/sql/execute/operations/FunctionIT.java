/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.splicemachine.pipeline.ErrorState;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceFunctionWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * @author Scott Fines
 *         Created on: 2/22/13
 */
public class FunctionIT extends SpliceUnitTest {
    private static final Logger LOG = Logger.getLogger(FunctionIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(FunctionIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",FunctionIT.class.getSimpleName(),"(data double)");
	protected static SpliceFunctionWatcher spliceFunctionWatcher = new SpliceFunctionWatcher("SIN",FunctionIT.class.getSimpleName(),"( data double) returns double external name 'java.lang.Math.sin' language java parameter style java");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(spliceFunctionWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
		        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into "+ FunctionIT.class.getSimpleName() + ".A (data) values (?)");
		        ps.setDouble(1,1.23d);
		        ps.executeUpdate();
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
    public void testSinFunction() throws Exception{
        ResultSet funcRs = methodWatcher.executeQuery("select SIN(data) from" + this.getPaddedTableReference("A"));
        int rows = 0;
        while(funcRs.next()){
            double sin = funcRs.getDouble(1);
            double correctSin = Math.sin(1.23d);
            Assert.assertEquals("incorrect sin!",correctSin,sin,1/100000d);
            LOG.info(funcRs.getDouble(1));
            rows++;
        }
        Assert.assertTrue("Incorrect rows returned!",rows>0);
    }

	    /**
	      * If more than one of the arguments passed to COALESCE are untyped
	      * parameter markers, compilation used to fail with a NullPointerException.
	      * Fixed in DERBY-6273.
	      */
		@Test
	public void testMultipleUntypedParametersAndNVL() throws Exception {
		// All parameters cannot be untyped. This should still fail.
		try {
			methodWatcher.prepareStatement("values coalesce(?,?,?)");
		} catch (SQLException se) {
			Assert.assertEquals("Invalid sql state!", ErrorState.LANG_DB2_COALESCE_FUNCTION_ALL_PARAMS.getSqlState(),se.getSQLState());
		}
		// But as long as we know the type of one parameter, it should be
		// possible to have multiple parameters whose types are determined
		// from the context. These queries used to raise NullPointerException
		// before DERBY-6273.
		vetThreeArgCoalesce("values coalesce(cast(? as char(1)), ?, ?)");
		vetThreeArgCoalesce("values coalesce(?, cast(? as char(1)), ?)");
		vetThreeArgCoalesce("values coalesce(?, ?, cast(? as char(1)))");
		vetThreeArgCoalesce("values nvl(cast(? as char(1)), ?, ?)");
		vetThreeArgCoalesce("values nvl(?, cast(? as char(1)), ?)");
		vetThreeArgCoalesce("values nvl(?, ?, cast(? as char(1)))");

	}

		private void vetThreeArgCoalesce(String sql) throws Exception {
		// First three values in each row are arguments to COALESCE. The
				// last value is the expected return value.
						String[][] data = {
					{"a",  "b",  "c",  "a"},
					{null, "b",  "c",  "b"},
					{"a",  null, "c",  "a"},
					{"a",  "b",  null, "a"},
					{null, null, "c",  "c"},
					{"a",  null, null, "a"},
					{null, "b",  null, "b"},
					{null, null, null, null},
				};
			PreparedStatement ps = methodWatcher.prepareStatement(sql);
			for (int i = 0; i < data.length; i++) {
				ps.setString(1, data[i][0]);
				ps.setString(2, data[i][1]);
				ps.setString(3, data[i][2]);
				ResultSet rs = ps.executeQuery();
				Assert.assertTrue(rs.next());
				Assert.assertEquals("Values do not match",rs.getString(1),data[i][3]);
				Assert.assertFalse(rs.next());
			}
	}

}

