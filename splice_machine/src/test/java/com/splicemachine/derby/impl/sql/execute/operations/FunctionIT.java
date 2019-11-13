/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.pipeline.ErrorState;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.Assert.assertEquals;

/**
 * @author Scott Fines
 *         Created on: 2/22/13
 */
public class FunctionIT extends SpliceUnitTest {
	protected static final String USER1 = "XIAYI";
	protected static final String PASSWORD1 = "xiayi";

    private static final Logger LOG = Logger.getLogger(FunctionIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(FunctionIT.class.getSimpleName());
	private static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",FunctionIT.class.getSimpleName(),"(data double)");
	protected static SpliceFunctionWatcher spliceFunctionWatcher = new SpliceFunctionWatcher("SIN",FunctionIT.class.getSimpleName(),"( data double) returns double external name 'java.lang.Math.sin' language java parameter style java");
	protected static double [] roundVals = {1.2, 2.53, 3.225, 4.1352, 5.23412, 53.2315093704, 205.130295341296824,
			13.21958329568391029385, 12.132435242330192856728391029584, 1.9082847283940982746172849098273647589099};
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("B",FunctionIT.class.getSimpleName(),"(col decimal(14,4))");

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceUserWatcher1)
		.around(spliceTableWatcher)
		.around(spliceTableWatcher1)
		.around(spliceFunctionWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into "+ FunctionIT.class.getSimpleName() + ".A (data) values (?)");
					ps.setDouble(1,1.23d);
					ps.executeUpdate();
					ps.close();
					ps = spliceClassWatcher.prepareStatement("insert into "+ FunctionIT.class.getSimpleName() + ".B (col) values (?)");
					ps.setInt(1,2);
					ps.executeUpdate();
					ps.setNull(1, Types.DECIMAL);
					ps.executeUpdate();
					ps.close();
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
	 * Tests the ROUND function which rounds the input value to the nearest whole LONG
	 * <p>
	 * If the input value is NULL, the result of this function is NULL. If the
	 * input value is equal to a mathematical integer, the result of this
	 * function is the same as the input number. If the input value is zero (0),
	 * the result of this function is zero.
	 * <p>
	 * The returned value is the closest (closest to positive infinity) long
	 * value to the input value. The
	 * returned value is equal to a mathematical long. The data type of the
	 *
	 * @throws Exception
	 */
	@Test
	public void testRound() throws Exception {
		ResultSet rs;
		//testing round to Long
 		for(double val : roundVals){
			rs = methodWatcher.executeQuery("values ROUND("+val+")");
			Assert.assertTrue(rs.next());
			Assert.assertEquals(Math.round(val),rs.getLong(1));
		}
	}

	@Test
	public void testRoundEdgeCase() throws Exception{
		ResultSet rs = methodWatcher.executeQuery("values ROUND(null)");
		Assert.assertTrue(rs.next());
		Assert.assertEquals(null,rs.getObject(1));

		rs = methodWatcher.executeQuery("values ROUND(0)");
		Assert.assertTrue(rs.next());
		Assert.assertEquals(0, rs.getDouble(1), 0.0);
	}

	/**
	 * This tests the ROUND function when the user inputs 2 parameters, where the second is the number of decimal places to round to
	 * @returns double rounded to the given number of decimal places
	 * @throws Exception
	 */
	@Test
	public void testRoundVaryScale() throws Exception{
		ResultSet rs;
		double longNumber = 1347593487534897908346789398700763453456786.9082847283940982746172849098273647589099;
		//testing round to Long
		for(int i = -40; i < 40; i++){
			rs = methodWatcher.executeQuery("values ROUND("+longNumber+","+i+")");
			Assert.assertTrue(rs.next());
			double mult = i < 18 ? i : 18;
			mult = Math.pow(10,mult);
			double x = Math.round(longNumber*mult)/(mult*1.0);
			Assert.assertEquals(x,rs.getDouble(1),0.0);
		}
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

	@Test
	public void testCallToSystemFunctionFromUserWithoutDefaultSchema() throws Exception {
		TestConnection user1Conn = spliceClassWatcher.createConnection(USER1, PASSWORD1);

		PreparedStatement ps = user1Conn.prepareStatement("VALUES rand(10)");
		ResultSet rs = ps.executeQuery();
		int count = 0;
		while (rs.next()) {
			count++;
		}
		Assert.assertEquals(1, count);
		rs.close();


		ps = user1Conn.prepareStatement("VALUES random()");
		rs = ps.executeQuery();
		count = 0;
		while (rs.next()) {
			count++;
		}
		Assert.assertEquals(1, count);
		rs.close();

	}

	@Test
	public void testNvlWithDecimalZero() throws Exception {
		String sqlText = format("select sum(val_no_null)\n" +
				"from (select nvl(col, 0) as val_no_null from %1$s.B) dt, %1$s.A --splice-properties useSpark=true", FunctionIT.class.getSimpleName());

		String expected = "1   |\n" +
				"--------\n" +
				"2.0000 |";
		ResultSet rs = methodWatcher.executeQuery(sqlText);
		assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
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

