/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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

