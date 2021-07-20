/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

    protected static SpliceTableWatcher t7Watcher = new SpliceTableWatcher("t7",schemaWatcher.schemaName,"(a7 int, b7 int, c7 int)");
    protected static SpliceIndexWatcher i7Watcher = new SpliceIndexWatcher("t7",schemaWatcher.schemaName,"ix_t7",schemaWatcher.schemaName,"(b7, c7)");

    protected static SpliceTableWatcher t11Watcher = new SpliceTableWatcher("t11",schemaWatcher.schemaName,"   (a1 int,\n" +
																										   "    b1 int,\n" +
																										   "    c1 int,\n" +
																										   "    d1 int,\n" +
																										   "    e1 int,\n" +
																										   "    f1 int,\n" +
																										   "    g1 int,\n" +
																										   "    h1 char(80),\n" +
																										   "    primary key (a1, b1, c1))");
    protected static SpliceIndexWatcher i11Watcher = new SpliceIndexWatcher("t11",schemaWatcher.schemaName,"idx_t11",schemaWatcher.schemaName,"(g1, f1, e1)");
    protected static SpliceTableWatcher t22Watcher = new SpliceTableWatcher("t22",schemaWatcher.schemaName,"   (a2 int,\n" +
																										   "    b2 int,\n" +
																										   "    c2 int,\n" +
																										   "    d2 int,\n" +
																										   "    e2 int,\n" +
																										   "    f2 int,\n" +
																										   "    g2 int,\n" +
																										   "    h2 char(80),\n" +
																										   "    primary key (a2, b2, c2))");

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
            .around(t7Watcher)
            .around(i7Watcher)
            .around(t11Watcher)
            .around(i11Watcher)
            .around(t22Watcher)
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

                        ps = spliceClassWatcher.prepareStatement("insert into " + t7Watcher.toString() + " values (?,?,?)");
                        for (int i = 0; i < size*200; i++) {
                            ps.setInt(1, i);
                            ps.setInt(2, i);
                            ps.setLong(3, 1l);
                            ps.addBatch();
                        }
                        ps.executeBatch();

                        ps = spliceClassWatcher.prepareStatement("insert into " + t11Watcher.toString() + " values (?,?,?,?,?,?,?,?)");
                        for (int i = 0; i < 3; i++) {
                            ps.setInt(1, i);
                            ps.setInt(2, i);
                            ps.setInt(3, i);
                            ps.setInt(4, i);
                            ps.setInt(5, i);
                            ps.setInt(6, i);
                            ps.setInt(7, i);
                            ps.setString(8, "a");
                            ps.addBatch();
                        }
                        ps.executeBatch();

                        spliceClassWatcher.executeUpdate("insert into t22 select * from t11");

					} catch (Exception e) {
						throw new RuntimeException(e);
					} finally {
						spliceClassWatcher.closeAll();
					}
				}

			});

	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

	@Test
	public void testMultiColumnMultiProbeWithAdditionalJoinPredicate() throws Exception {
		String sql = "select a1, t22.a2 from t11 INNER JOIN t22 on t11.a1=t22.a2 " +
					 "where a1=(SELECT MAX(a2) FROM t22   )   and  a1 in (0,1,2) "   +
					 "and b1 in (0,1,2,3,4,5) and c1 in (0,1,2,3,4,5,6,7,8) order by t22.a2";
		String expected = "A1 |A2 |\n" +
						  "--------\n" +
						  " 2 | 2 |";
		testQuery(sql, expected, methodWatcher);
	}

	@Test
	public void testMultiProbeTableScanScroll() throws Exception {
		try (ResultSet rs = methodWatcher.executeQuery("select user_id from "+t1Watcher+" where segment_id in (1,5,8,12)")) {
			int i = 0;
			while (rs.next()) {
				i++;
			}
			Assert.assertEquals("Incorrect count returned!", 3, i);
		}
	}

	@Test
	//DB-2575
	public void testMultiProbeTableScanWithEqualPredicate() throws Exception {
		try (ResultSet rs = methodWatcher.executeQuery("select user_id from "+t1Watcher+" where segment_id in (1,5,8,12) and unixtime = 1")) {
			int i = 0;
			while (rs.next()) {
				i++;
			}
			Assert.assertEquals("Incorrect count returned!", 3, i);
		}
	}

	@Test
	public void testMultiProbeTableScanSink() throws Exception {
		try (ResultSet rs = methodWatcher.executeQuery("select count(user_id) from (" +
				"select user_id, ("+
				"max(case when segment_id = 7 then true else false end) " +
				"or " +
				"max(case when segment_id = 4 then true else false end)" +
				") as in_segment " +
				"from "+t1Watcher+ " " +
				"where segment_id in (7, 4) " +
				"group by user_id) foo where in_segment = true")) {
			int i = 0;
			while (rs.next()) {
				i++;
				Assert.assertEquals("Incorrect Distinct Customers", 3, rs.getLong(1));
			}
			Assert.assertEquals("Incorrect records returned!", 1, i);
		}
	}

	@Test
	public void testMultiProbeInSubQueryWithIndex() throws Exception {
				/* Regression test for DB-1040 */
		SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(t3Watcher.tableName, t3Watcher.getSchema(),"new_index_3",t3Watcher.getSchema(),"(collid)");
		indexWatcher.starting(null);
		try (ResultSet rs = methodWatcher.executeQuery("select count(id) from docs where id > any (select id from colls where collid in (-2,1))")) {
			Assert.assertTrue("No results returned!",rs.next());
			int count = rs.getInt(1);
			Assert.assertEquals("Incorrect count returned!",4,count);
			Assert.assertFalse("Too many rows returned!",rs.next());
		} finally{
			indexWatcher.drop();
		}
	}

	@Test
	//DB-4854
	public void testMultiProbeIntegerValue() throws Exception {
		SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(t4Watcher.tableName, t4Watcher.getSchema(),"idxb",t4Watcher.getSchema(),"(d)");
		indexWatcher.starting(null);
		try (ResultSet rs = methodWatcher.executeQuery("select count(*) from b where d in (9,10)")) {
			Assert.assertTrue(rs.next());
			Assert.assertTrue("wrong count", rs.getInt(1) == 2);
		}
		try (ResultSet rs = methodWatcher.executeQuery("select count(*) from b where d in (9)")) {
			Assert.assertTrue(rs.next());
			Assert.assertTrue("wrong count", rs.getInt(1) == 1);
		}
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
		try (ResultSet rs = ps.executeQuery()) {
			int i = 0;
			while (rs.next()) {
				i++;
			}
			Assert.assertEquals("Incorrect count returned!", 3, i);
		}
	}


	// DB-4857
    @Test
    public void testMultiProbeWithComputations() throws Exception {
        this.thirdRowContainsQuery("explain select * from a --splice-properties index=i\n" +
                " where d in (10.0+10, 11.0+10)","preds=[(D[0:1] IN (20.0,21.0))]",methodWatcher);
    }

    // DB-1323
    @Test
	public void testMultiProbeWithSpark() throws Exception {
		try (ResultSet rs = methodWatcher.executeQuery("select count(*) from "+ t6Watcher+ "--splice-properties useSpark=true\n" +
						" where j in (1181) and (i = 1 or i = 118 )")) {
			Assert.assertTrue(rs.next());
			Assert.assertTrue("wrong count", rs.getInt(1) == 3);
		}

		try (ResultSet rs = methodWatcher.executeQuery("select count(*) from "+ t6Watcher+ "--splice-properties useSpark=true\n" +
				" where i in (1, 118)")) {
			Assert.assertTrue(rs.next());
			Assert.assertTrue("wrong count", rs.getInt(1) == 3);
		}
	}

	@Test
    public void testMultiProbeWithLargeInListThroughSpark() throws Exception {
        try (ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s --splice-properties useSpark=true, index=%s\n" +
                " where b7 in (0,1,2,3,4,5,6,7,8,9,\n" +
                "10,11,12,13,14,15,16,17,18,19,\n" +
                "20,21,22,23,24,25,26,27,28,29,\n" +
                "30,31,32,33,34,35,36,37,38,39,\n" +
                "40,41,42,43,44,45,46,47,48,49,\n" +
                "50,51,52,53,54,55,56,57,58,59,\n" +
                "60,61,62,63,64,65,66,67,68,69,\n" +
                "70,71,72,73,74,75,76,77,78,79,\n" +
                "80,81,82,83,84,85,86,87,88,89,\n" +
                "90,91,92,93,94,95,96,97,98,99,\n" +
                "100,101,102,103,104,105,106,107,108,109,\n" +
                "110,111,112,113,114,115,116,117,118,119,\n" +
                "120,121,122,123,124,125,126,127,128,129,\n" +
                "130,131,132,133,134,135,136,137,138,139,\n" +
                "140,141,142,143,144,145,146,147,148,149,\n" +
                "150,151,152,153,154,155,156,157,158,159,\n" +
                "160,161,162,163,164,165,166,167,168,169,\n" +
                "170,171,172,173,174,175,176,177,178,179,\n" +
                "180,181,182,183,184,185,186,187,188,189,\n" +
                "190,191,192,193,194,195,196,197,198,199,\n" +
                "200,201,202,203,204,205,206,207,208,209,\n" +
                "210,211,212,213,214,215,216,217,218,219,\n" +
                "220,221,222,223,224,225,226,227,228,229,\n" +
                "230,231,232,233,234,235,236,237,238,239,\n" +
                "240,241,242,243,244,245,246,247,248,249,\n" +
                "250,251,252,253,254,255,256,257,258,259,\n" +
                "260,261,262,263,264,265,266,267,268,269,\n" +
                "270,271,272,273,274,275,276,277,278,279,\n" +
                "280,281,282,283,284,285,286,287,288,289,\n" +
                "290,291,292,293,294,295,296,297,298,299,\n" +
                "300,301,302,303,304,305,306,307,308,309,\n" +
                "310,311,312,313,314,315,316,317,318,319,\n" +
                "320,321,322,323,324,325,326,327,328,329,\n" +
                "330,331,332,333,334,335,336,337,338,339,\n" +
                "340,341,342,343,344,345,346,347,348,349,\n" +
                "350,351,352,353,354,355,356,357,358,359,\n" +
                "360,361,362,363,364,365,366,367,368,369,\n" +
                "370,371,372,373,374,375,376,377,378,379,\n" +
                "380,381,382,383,384,385,386,387,388,389,\n" +
                "390,391,392,393,394,395,396,397,398,399,\n" +
                "400,401,402,403,404,405,406,407,408,409,\n" +
                "410,411,412,413,414,415,416,417,418,419,\n" +
                "420,421,422,423,424,425,426,427,428,429,\n" +
                "430,431,432,433,434,435,436,437,438,439,\n" +
                "440,441,442,443,444,445,446,447,448,449,\n" +
                "450,451,452,453,454,455,456,457,458,459,\n" +
                "460,461,462,463,464,465,466,467,468,469,\n" +
                "470,471,472,473,474,475,476,477,478,479,\n" +
                "480,481,482,483,484,485,486,487,488,489,\n" +
                "490,491,492,493,494,495,496,497,498,499)", t7Watcher, "ix_t7"))) {
			Assert.assertTrue(rs.next());
			Assert.assertTrue(format("wrong count: expected: %d, actual: %d", 500, rs.getInt(1)), rs.getInt(1) == 500);
		}
    }
}
