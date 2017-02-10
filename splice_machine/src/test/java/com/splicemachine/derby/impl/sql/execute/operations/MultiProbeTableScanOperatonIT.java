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

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Unit Test for making sure MultiProbeTableScanOperation is logically correct.  Once we have metrics information,
 * the test should be expanded to show that we only filter the records required.
 *
 */
public class MultiProbeTableScanOperatonIT extends SpliceUnitTest {
	public static final String CLASS_NAME = MultiProbeTableScanOperatonIT.class.getSimpleName();
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
	protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
	protected static SpliceTableWatcher t1Watcher = new SpliceTableWatcher("user_groups",schemaWatcher.schemaName,"(user_id BIGINT NOT NULL,segment_id INT NOT NULL,unixtime BIGINT, primary key(segment_id, user_id))");
	protected static SpliceTableWatcher t2Watcher = new SpliceTableWatcher("docs",schemaWatcher.schemaName,"(id varchar(128) not null)");
	protected static SpliceTableWatcher t3Watcher = new SpliceTableWatcher("colls",schemaWatcher.schemaName,"(id varchar(128) not null,collid smallint not null)");
	protected static SpliceTableWatcher t4Watcher = new SpliceTableWatcher("b",schemaWatcher.schemaName,"(d decimal(10))");
    protected static SpliceTableWatcher t5Watcher = new SpliceTableWatcher("a",schemaWatcher.schemaName,"(d decimal(10,0))");
    protected static SpliceIndexWatcher i5Watcher = new SpliceIndexWatcher("a",schemaWatcher.schemaName,"i",schemaWatcher.schemaName,"(d)");

	protected static SpliceTableWatcher t6Watcher = new SpliceTableWatcher("tab",schemaWatcher.schemaName,"(i int, j int)");
	protected static SpliceIndexWatcher i6Watcher = new SpliceIndexWatcher("tab",schemaWatcher.schemaName,"idx",schemaWatcher.schemaName,"(i,j)");


	private static int size = 10;

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
			.around(schemaWatcher)
			.around(t1Watcher)
			.around(t2Watcher)
			.around(t3Watcher)
			.around(t4Watcher)
            .around(t5Watcher)
            .around(i5Watcher)
			.around(t6Watcher)
			.around(i6Watcher)
			.around(new SpliceDataWatcher() {
				@Override
				protected void starting(Description description) {
					try {
						PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into " + t1Watcher.toString() + " values (?,?,?)");
						for (int i = 0; i < size; i++) {
							ps.setInt(1, i);
							ps.setInt(2, i);
							ps.setLong(3, 1l);
							ps.execute();
						}

						for (int i = 0; i < size; i++) {
							if ((i == 4) || (i == 6)) {
								ps.setInt(1, size + i);
								ps.setInt(2, i);
								ps.setLong(3, 1l);
								ps.execute();
							}
						}

						ps = spliceClassWatcher.prepareStatement("insert into " + t2Watcher.toString() + " values (?)");
						ps.setString(1, "24");
						ps.addBatch();
						ps.setString(1, "25");
						ps.addBatch();
						ps.setString(1, "36");
						ps.addBatch();
						ps.setString(1, "27");
						ps.addBatch();
						ps.setString(1, "124");
						ps.addBatch();
						ps.setString(1, "567");
						ps.addBatch();
						ps.executeBatch();

						ps = spliceClassWatcher.prepareStatement("insert into " + t3Watcher.toString() + " values (?,?)");
						ps.setString(1, "123");
						ps.setShort(2, (short) 2);
						ps.addBatch();
						ps.setString(1, "124");
						ps.setShort(2, (short) -5);
						ps.addBatch();
						ps.setString(1, "24");
						ps.setShort(2, (short) 1);
						ps.addBatch();
						ps.setString(1, "26");
						ps.setShort(2, (short) -2);
						ps.addBatch();
						ps.setString(1, "36");
						ps.setShort(2, (short) 1);
						ps.addBatch();
						ps.setString(1, "37");
						ps.setShort(2, (short) 8);
						ps.addBatch();
						ps.executeBatch();

						ps = spliceClassWatcher.prepareStatement("insert into " + t4Watcher.toString() + " values (?)");
						for (int i = 0; i <= 10; ++i) {
							ps.setInt(1, i);
							ps.addBatch();
						}
						ps.executeBatch();

						ps = spliceClassWatcher.prepareStatement("insert into " + t6Watcher.toString() + " values (?,?)");
                        ps.setInt(1, 1); ps.setInt(2, 1181); ps.addBatch();
						ps.setInt(1, 2); ps.setInt(2, 1181); ps.addBatch();
						ps.setInt(1, 1); ps.setInt(2, 1181); ps.addBatch();
						ps.setInt(1, 118); ps.setInt(2, 1181); ps.addBatch();
						ps.executeBatch();

					} catch (Exception e) {
						throw new RuntimeException(e);
					} finally {
						spliceClassWatcher.closeAll();
					}
				}

			});

	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

	@Test
	public void testMultiProbeTableScanScroll() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select user_id from "+t1Watcher+" where segment_id in (1,5,8,12)");
		int i = 0;
		while (rs.next()) {
			i++;
		}
		Assert.assertEquals("Incorrect count returned!",3,i);
	}

	@Test
	//DB-2575
	public void testMultiProbeTableScanWithEqualPredicate() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select user_id from "+t1Watcher+" where segment_id in (1,5,8,12) and unixtime = 1");
		int i = 0;
		while (rs.next()) {
			i++;
		}
		Assert.assertEquals("Incorrect count returned!",3,i);
	}

	@Test
	public void testMultiProbeTableScanSink() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select count(user_id) from (" +
				"select user_id, ("+
				"max(case when segment_id = 7 then true else false end) " +
				"or " +
				"max(case when segment_id = 4 then true else false end)" +
				") as in_segment " +
				"from "+t1Watcher+ " " +
				"where segment_id in (7, 4) " +
				"group by user_id) foo where in_segment = true");
		int i = 0;
		while (rs.next()) {
			i++;
			Assert.assertEquals("Incorrect Distinct Customers",3,rs.getLong(1));
		}
		Assert.assertEquals("Incorrect records returned!",1,i);
	}

	@Test
	public void testMultiProbeInSubQueryWithIndex() throws Exception {
				/* Regression test for DB-1040 */
		SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(t3Watcher.tableName, t3Watcher.getSchema(),"new_index_3",t3Watcher.getSchema(),"(collid)");
		indexWatcher.starting(null);
		try{
			ResultSet rs = methodWatcher.executeQuery("select count(id) from docs where id > any (select id from colls where collid in (-2,1))");
			Assert.assertTrue("No results returned!",rs.next());
			int count = rs.getInt(1);
			Assert.assertEquals("Incorrect count returned!",4,count);
			Assert.assertFalse("Too many rows returned!",rs.next());
		}finally{
			indexWatcher.drop();
		}
	}

	@Test
	//DB-4854
	public void testMultiProbeIntegerValue() throws Exception {
		SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(t4Watcher.tableName, t4Watcher.getSchema(),"idxb",t4Watcher.getSchema(),"(d)");
		indexWatcher.starting(null);
		ResultSet rs = methodWatcher.executeQuery("select count(*) from b where d in (9,10)");
		Assert.assertTrue(rs.next());
		Assert.assertTrue("wrong count", rs.getInt(1) == 2);

		rs = methodWatcher.executeQuery("select count(*) from b where d in (9)");
		Assert.assertTrue(rs.next());
		Assert.assertTrue("wrong count", rs.getInt(1)==1);
	}

	@Test
	//DB-5349
	public void testMultiProbeTableScanWithProbeVariables() throws Exception {
		PreparedStatement ps = methodWatcher.prepareStatement("select user_id from "+t1Watcher+" where segment_id in (?,?,?,?) and unixtime = ?");
		ps.setInt(1,1);
		ps.setInt(2,5);
		ps.setInt(3,8);
		ps.setInt(4,12);
		ps.setLong(5,1);
		ResultSet rs = ps.executeQuery();
		int i = 0;
		while (rs.next()) {
			i++;
		}
		Assert.assertEquals("Incorrect count returned!",3,i);
	}


	// DB-4857
    @Test
    public void testMultiProbeWithComputations() throws Exception {
        this.thirdRowContainsQuery("explain select * from a --splice-properties index=i\n" +
                " where d in (10.0+10, 11.0+10)","preds=[(D[0:1] IN ((10.0 + 10),(11.0 + 10)))]",methodWatcher);
    }

    // DB-1323
    @Test
	public void testMultiProbeWithSpark() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select count(*) from "+ t6Watcher+ "--splice-properties useSpark=true\n" +
						" where j in (1181) and (i = 1 or i = 118 )");
		Assert.assertTrue(rs.next());
		Assert.assertTrue("wrong count", rs.getInt(1) == 3);
        rs.close();

		rs = methodWatcher.executeQuery("select count(*) from "+ t6Watcher+ "--splice-properties useSpark=true\n" +
				" where i in (1, 118)");
		Assert.assertTrue(rs.next());
		Assert.assertTrue("wrong count", rs.getInt(1) == 3);

	}
}
