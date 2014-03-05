package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.perf.runner.qualifiers.Result;
import org.apache.derby.iapi.error.StandardException;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static com.splicemachine.homeless.TestUtils.o;

/**
 * @author Scott Fines
 *         Created on: 5/17/13
 */
public class SubqueryIT { 
    private static List<String> t3RowVals = Arrays.asList("('E1','P1',40)",
            "('E1','P2',20)",
            "('E1','P3',80)",
            "('E1','P4',20)",
            "('E1','P5',12)",
            "('E1','P6',12)",
            "('E2','P1',40)",
            "('E2','P2',80)",
            "('E3','P2',20)",
            "('E4','P2',20)",
            "('E4','P4',40)",
            "('E4','P5',80)",
            "('E8','P8',NULL)");

    public static final String CLASS_NAME = SubqueryIT.class.getSimpleName();

    protected static SpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    protected static SpliceTableWatcher t1Watcher = new SpliceTableWatcher("t1",schemaWatcher.schemaName,"(k int, l int)");
    protected static SpliceTableWatcher t2Watcher = new SpliceTableWatcher("t2",schemaWatcher.schemaName,"(k int, l int)");

		private static SpliceTableWatcher intTable1 = new SpliceTableWatcher("t3",schemaWatcher.schemaName,"(i int)");
		private static SpliceTableWatcher intTable2 = new SpliceTableWatcher("t4",schemaWatcher.schemaName,"(i int)");

		private static SpliceTableWatcher t5Watcher = new SpliceTableWatcher("t5",schemaWatcher.schemaName,"(k int)");
    protected static SpliceTableWatcher t3Watcher = new SpliceTableWatcher("WORKS8",schemaWatcher.schemaName,
            "(EMPNUM VARCHAR(3) NOT NULL, PNUM VARCHAR(3) NOT NULL,HOURS DECIMAL(5))");

		private static SpliceTableWatcher collTable = new SpliceTableWatcher("colls",schemaWatcher.schemaName,"(ID VARCHAR(128) NOT NULL, COLLID SMALLINT NOT NULL)");
		private static SpliceTableWatcher docsTable = new SpliceTableWatcher("docs",schemaWatcher.schemaName,"(ID VARCHAR(128) NOT NULL)");

		private static SpliceTableWatcher scanSubqueryT1 = new SpliceTableWatcher("sT1",schemaWatcher.schemaName,"(toid int,rd int)");
		private static SpliceTableWatcher scanSubqueryT2 = new SpliceTableWatcher("sT2",schemaWatcher.schemaName,"(userid int,pmnew int,pmtotal int)");
    private static final int size = 10;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(t1Watcher)
            .around(t2Watcher)
            .around(t3Watcher)
						.around(t5Watcher)
						.around(intTable1)
						.around(intTable2)
						.around(docsTable)
						.around(collTable)
						.around(scanSubqueryT1)
						.around(scanSubqueryT2)
            .around(new SpliceDataWatcher() {
								@Override
								protected void starting(Description description) {
										try {
												PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s values (?,?)", t2Watcher.toString()));
												for (int i = 0; i < size; i++) {
														//(0,1),(1,2),...,(size,size+1)
														ps.setInt(1, i); ps.setInt(2, i + 1); ps.addBatch();
												}
												ps.executeBatch();

												ps = spliceClassWatcher.prepareStatement(String.format("insert into %s values (?,?)", t1Watcher.toString()));
												for (int i = 0; i < size; i++) {
														//(0,1),(0,1),(1,2),...,(size,size+1)
														ps.setInt(1, i); ps.setInt(2, i + 1); ps.addBatch();
														if (i % 2 == 0) {
																//put in a duplicate
																ps.setInt(1, i); ps.setInt(2, i + 1); ps.addBatch();
														}
												}
												ps.executeBatch();

												//  load t3
												for (String rowVal : t3RowVals) {
														spliceClassWatcher.getStatement().executeUpdate("insert into " + t3Watcher.toString() + " values " + rowVal);
												}

												//load t5
												spliceClassWatcher.executeUpdate(String.format("insert into %s values %d", t5Watcher, 2));

												ps = spliceClassWatcher.prepareStatement(String.format("insert into %s values (?)", intTable1));
												ps.setInt(1, 10); ps.addBatch();
												ps.setInt(1, 20); ps.addBatch();
												ps.executeBatch();

												ps = spliceClassWatcher.prepareStatement(String.format("insert into %s values (?)", intTable2));
												ps.setInt(1, 30); ps.addBatch();
												ps.setInt(1, 40); ps.addBatch();
												ps.executeBatch();

												String collLoadQuery = String.format("insert into %s values (?,?)", collTable);
												ps = spliceClassWatcher.prepareStatement(collLoadQuery);
												ps.setString(1, "123"); ps.setInt(2, 2); ps.addBatch();
												ps.setString(1, "124"); ps.setInt(2, -5); ps.addBatch();
												ps.setString(1, "24"); ps.setInt(2, 1); ps.addBatch();
												ps.setString(1, "26"); ps.setInt(2, -2); ps.addBatch();
												ps.setString(1, "36"); ps.setInt(2, 1); ps.addBatch();
												ps.setString(1, "37"); ps.setInt(2, 8); ps.addBatch();
												ps.executeBatch();

												String docLoadQuery = String.format("insert into %s values (?)", docsTable);
												ps = spliceClassWatcher.prepareStatement(docLoadQuery);
												ps.setString(1, "24"); ps.addBatch();
												ps.setString(1, "25"); ps.addBatch();
												ps.setString(1, "27"); ps.addBatch();
												ps.setString(1, "36"); ps.addBatch();
												ps.setString(1, "124"); ps.addBatch();
												ps.setString(1, "567"); ps.addBatch();
												ps.executeBatch();


												String subqueryT1Load = String.format("insert into %s values (?,?)",scanSubqueryT1);
												ps = spliceClassWatcher.prepareStatement(subqueryT1Load);
												ps.setInt(1,1); ps.setInt(2,0); ps.addBatch();
												ps.setInt(1,1); ps.setInt(2,0); ps.addBatch();
												ps.setInt(1,1); ps.setInt(2,0); ps.addBatch();
												ps.setInt(1,1); ps.setInt(2,12); ps.addBatch();
												ps.setInt(1,1); ps.setInt(2,15); ps.addBatch();
												ps.setInt(1,1); ps.setInt(2,123); ps.addBatch();
												ps.setInt(1,1); ps.setInt(2,12312); ps.addBatch();
												ps.setInt(1,1); ps.setInt(2,12312); ps.addBatch();
												ps.setInt(1,1); ps.setInt(2,123); ps.addBatch();
												ps.setInt(1,2); ps.setInt(2,0); ps.addBatch();
												ps.setInt(1,2); ps.setInt(2,0); ps.addBatch();
												ps.setInt(1,2); ps.setInt(2,1); ps.addBatch();
												ps.setInt(1,2); ps.setInt(2,2); ps.addBatch();
												ps.executeBatch();

												ps = spliceClassWatcher.prepareStatement(String.format("insert into %s values (?,?,?)",scanSubqueryT2));
												ps.setInt(1,1);ps.setInt(2,0);ps.setInt(3,0);ps.addBatch();
												ps.setInt(1,2);ps.setInt(2,0);ps.setInt(3,0);ps.addBatch();
												ps.executeBatch();
										} catch (Exception e) {
												throw new RuntimeException(e);
										} finally {
												spliceClassWatcher.closeAll();
										}
								}
						})
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/employee.sql", CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/content.sql", CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "null_int_data.sql", schemaWatcher.schemaName))
            .around(TestUtils.createStringDataWatcher(spliceClassWatcher,
                    "create table s (a int, b int, c int, d int, e int, f int);" +
                    "insert into s values (0,1,2,3,4,5);" +
                    "insert into s values (10,11,12,13,14,15);", CLASS_NAME))
            .around(TestUtils.createStringDataWatcher(spliceClassWatcher,
                    "create table tWithNulls1 (c1 int, c2 int); \n" +
                    "create table tWithNulls2 (c1 int, c2 int); \n" +
                    "insert into tWithNulls1 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10); \n" +
                    "insert into tWithNulls2 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10); "
                    , CLASS_NAME));

    @Rule public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    @Test
    public void testSubqueryWithSum() throws Exception {
        /* Regression test for Bug 883/884*/
        ResultSet rs = methodWatcher.executeQuery("select k from "+t1Watcher+" where k not in (select sum(k) from "+t2Watcher+")");
        List<Integer> correctResults = Lists.newArrayList();
        for(int i=0;i<size;i++){
            correctResults.add(i);
            if(i%2==0)
                correctResults.add(i);
        }

        int count = 0;
        while(rs.next()){
            count++;
        }

        Assert.assertEquals("Incorrect count returned!",size+size/2,count);
    }

    @Test
    public void testValuesSubSelect() throws Exception {
        /*
         * Regression for Bug 285. Make sure that values ((select ..)) works as expected
         */
        ResultSet rs = methodWatcher.executeQuery("values ((select k from "+t2Watcher.toString()+" where k=1),2)");
        Set<Integer> correctResults = Sets.newHashSet(1);
        List<Integer> retResults = Lists.newArrayList();

        while(rs.next()){
            int val = rs.getInt(1);
            int sec = rs.getInt(2);
            System.out.printf("value=%d,sec=%d%n",val,sec);
            Assert.assertTrue("value "+ val+" is not contained in the correct results!",correctResults.contains(val));
            retResults.add(val);
        }
        Assert.assertEquals("Incorrect number of rows returned!",correctResults.size(),retResults.size());

        for(int correct:retResults){
            int numFound=0;
            for(int ret:retResults){
                if(ret==correct){
                    numFound++;
                    if(numFound>1)
                        Assert.fail("Value "+ ret+" was returned more than once!");
                }
            }
        }
    }

    @Test
    public void testValuesSubselectWithTwoSelects() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("values ((select k from "+t2Watcher.toString()+" where k = 1), (select l from "+t2Watcher.toString()+" where l =4))");
        Set<Integer> correctResults = Sets.newHashSet(1);
        Set<Integer> correctSecondResults = Sets.newHashSet(4);

        List<Integer> retResults = Lists.newArrayList();
        List<Integer> retSecResults = Lists.newArrayList();

        while(rs.next()){
            int val = rs.getInt(1);
            int sec = rs.getInt(2);
            System.out.printf("value=%d,sec=%d%n",val,sec);
            Assert.assertTrue("value "+ val+" is not contained in the correct results!",correctResults.contains(val));
            retResults.add(val);

            Assert.assertTrue("second value "+ sec+" is not contained in the correct results!",correctSecondResults.contains(sec));
        }
        Assert.assertEquals("Incorrect number of rows returned!",correctResults.size(),retResults.size());

        for(int correct:retResults){
            int numFound=0;
            for(int ret:retResults){
                if(ret==correct){
                    numFound++;
                    if(numFound>1)
                        Assert.fail("Value "+ ret+" was returned more than once!");
                }
            }
        }
    }

    @Test
    public void testSubqueryInJoinClause() throws Exception {
        /* Regression test for bug 273 */
        ResultSet rs = methodWatcher.executeQuery("select * from "+t1Watcher.toString()+" a join "+ t2Watcher.toString()+" b on a.k = b.k and a.k in (select k from "+ t1Watcher.toString()+" where a.k ="+t1Watcher.toString()+".k)");
        while(rs.next()){
            System.out.printf("k=%d,l=%d",rs.getInt(1),rs.getInt(2));
        }
    }

    @Test
    public void testInDoesNotReturnDuplicates() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(String.format("select k from %s a where k in (select k from %s )",t2Watcher.toString(),t1Watcher.toString()));
        Set<Integer> priorResults = Sets.newHashSet();
        while(rs.next()){
            Integer nextK = rs.getInt(1);
            System.out.printf("nextK=%d%n",nextK);

            Assert.assertTrue("duplicate result "+ nextK +" returned!",!priorResults.contains(nextK));
            priorResults.add(nextK);
        }
        Assert.assertTrue("No Rows returned!",priorResults.size()>0);
    }

    @Test
    @Ignore("Bugzilla #510 Incorrect results for queries involving not null filters")
    public void testNullSubqueryCompare() throws Exception {
        TestUtils.tableLookupByNumber(spliceClassWatcher);
        ResultSet rs = methodWatcher.executeQuery(
                String.format("SELECT EMPNUM, PNUM FROM %1$s WHERE HOURS > (SELECT W2.HOURS FROM %1$s W2 WHERE W2.EMPNUM = 'E8')",
                        t3Watcher.toString()));
        Assert.assertEquals(0, TestUtils.resultSetToMaps(rs).size());
    }

    @Test
    public void testSubqueryWithAny() throws Exception {
        TestUtils.tableLookupByNumber(spliceClassWatcher);
        ResultSet rs = methodWatcher.executeQuery(
                String.format("select * from %s.z1 t1 where t1.s >= ANY (select t2.b from %s.z2 t2)",schemaWatcher.schemaName,schemaWatcher.schemaName));
        List<Map> results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(2, results.size());

        for(Map result : results){
            Assert.assertNotNull("Value for column I should not be null", result.get("I"));
            Assert.assertNotNull("Value for column S should not be null", result.get("S"));
            Assert.assertNotNull("Value for column C should not be null", result.get("C"));
        }
    }

    @Test
    public void testCorrelatedExpressionSubqueryOnlyReturnOneRow() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select w.empnum from works w where empnum = (select empnum from staff s where w.empnum = s.empnum)");
        // WORKS has 12 rows, each should be returned since STAFF contains one of every EMPNUM
        Assert.assertEquals(12, TestUtils.resultSetToMaps(rs).size());
    }

    @Test
    public void testCorrelatedExpressionSubqueryWithBoundaryCrossingReference() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select works.* from proj, works, staff " +
                "where proj.pnum = works.pnum and staff.empnum = works.empnum and staff.city = " +
                "(select distinct staff.city from staff where proj.city = staff.city)");

        Assert.assertEquals(6, TestUtils.resultSetToMaps(rs).size());
    }

    @Test
    public void testJoinOfTwoSubqueries() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from (select a.a, b.a from s a, s b) a (b, a), " +
                "(select a.a, b.a from s a, s b) b (b, a) where a.b = b.b");

        Assert.assertEquals(8, TestUtils.resultSetToArrays(rs).size());
    }

    @Test
    public void testAggWithDoublyNestedCorrelatedSubquery() throws Exception {
        List<Object[]> expected = Arrays.asList(o("P1", BigDecimal.valueOf(80)),
                                                o("P5", BigDecimal.valueOf(92)));

        ResultSet rs = methodWatcher.executeQuery("SELECT pnum, " +
                "       Sum(hours) " +
                "FROM   works c " +
                "GROUP  BY pnum " +
                "HAVING EXISTS (SELECT pname " +
                "               FROM   proj, " +
                "                      works a " +
                "               WHERE  proj.pnum = a.pnum " +
                "                      AND proj.budget / 200 < (SELECT Sum(hours) " +
                "                                               FROM   works b " +
                "                                               WHERE  a.pnum = b.pnum " +
                "                                                      AND a.pnum = c.pnum))" +
                "ORDER BY pnum");

        Assert.assertArrayEquals(expected.toArray(), TestUtils.resultSetToArrays(rs).toArray());
    }

    @Test
    public void testJoinOfAggSubquery() throws Exception {
        List<Object[]> expected = Arrays.asList(
                o("BIRD", 4.5, "title1", "http://url.1"),
                o("CAR", 4.5, "title1", "http://url.1"));

        ResultSet rs = methodWatcher.executeQuery(
                "SELECT S.DESCRIPTION, FAV.MAXRATE, C.TITLE, C.URL " +
                "FROM RATING R, " +
                "      CONTENT C, " +
                "      STYLE S, " +
                "      CONTENT_STYLE CS, " +
                "      (select S.ID, max(rating) " +
                "         from RATING R, CONTENT C, STYLE S," +
                "            CONTENT_STYLE CS group by S.ID) AS FAV(FID,MAXRATE) " +
                "WHERE R.ID = C.ID" +
                "   AND C.ID = CS.CONTENT_ID " +
                "   AND CS.STYLE_ID = FAV.FID " +
                "   AND FAV.FID = S.ID AND" +
                "   FAV.MAXRATE = R.RATING " +
                "ORDER BY S.DESCRIPTION" );

        Assert.assertArrayEquals(expected.toArray(), TestUtils.resultSetToArrays(rs).toArray());
    }

    @Ignore("Bugzilla 626")
    @Test
    public void testCorrelatedDoubleNestedNotExists() throws Exception {
        List<Object[]> expected = Collections.singletonList(o("Alice"));

        ResultSet rs = methodWatcher.executeQuery(
                "SELECT STAFF.EMPNAME" +
                "          FROM STAFF" +
                "          WHERE NOT EXISTS" +
                "                 (SELECT *" +
                "                       FROM PROJ" +
                "                       WHERE NOT EXISTS" +
                "                             (SELECT *" +
                "                                   FROM WORKS" +
                "                                   WHERE STAFF.EMPNUM = WORKS.EMPNUM" +
                "                                   AND WORKS.PNUM=PROJ.PNUM))" );

        Assert.assertArrayEquals(expected.toArray(),
                TestUtils.resultSetToArrays(rs).toArray());
    }

    @Test
    public void testSubqueryWithLiteralProjection() throws Exception {
        List<Object[]> expected = Arrays.asList(o("E3"), o("E5"));

        ResultSet rs = methodWatcher.executeQuery(
                "SELECT EMPNUM FROM STAFF O " +
                        "WHERE EXISTS (" +
                        // make subquery a union so is not converted to join
                        "   SELECT 1 FROM STAFF WHERE 1 = 0 " +
                        "   UNION " +
                        "   SELECT 1 FROM STAFF I " +
                        "   WHERE O.EMPNUM = I.EMPNUM AND I.GRADE > 12) " +
                        "ORDER BY EMPNUM");

        Assert.assertArrayEquals(expected.toArray(),
                TestUtils.resultSetToArrays(rs).toArray());
    }

    @Test
    // test for JIRA 960
    public void testMaterializationOfSubquery() throws Exception {
        List<Object[]> expected = Arrays.asList(o(3), o(10));

        ResultSet rs = methodWatcher.executeQuery(
                "select c1 from tWithNulls1 where c1 in (select max(c1) from tWithNulls2 group by c2)" +
                        " order by c1");

        List<Object[]> actual = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

		@Test(expected = SQLException.class)
		public void testScalarSubqueryExpected() throws Exception {
				/*Regression test for DB-945*/
			try{
					methodWatcher.executeQuery("select ( select t1.k from t1 where t1.k = t2.k union all select t5.k from t5 where t5.k = t2.k),k from t2");
			}catch(StandardException se){
					String correctSqlState = ErrorState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION.getSqlState();
					Assert.assertEquals("Incorrect sql state returned!",correctSqlState,se.getSqlState());
			}
		}

		@Test
		public void testWeirdQuery() throws Exception {
				/*Regression test for DB-954*/
				ResultSet resultSet = methodWatcher.executeQuery("select i from t3 a where i in (select a.i from t4 where a.i < i union all select i from t4 where 1 < 0)");
				List<Integer> correctResults = Arrays.asList(10,20);
				List<Integer> actual = Lists.newArrayListWithExpectedSize(2);
				while(resultSet.next()){
						int next = resultSet.getInt(1);
						Assert.assertFalse("Returned null accidentally!",resultSet.wasNull());
						actual.add(next);
				}
				Collections.sort(actual);
				Assert.assertEquals("Incorrect results!",correctResults,actual);
		}

		@Test
		public void testCountOverSubqueryWithJoin() throws Exception {
				/*Regression test for DB-1027*/
				String query = String.format("SELECT COUNT(*) FROM " +
								"(SELECT ID FROM %1$s WHERE " +
								"(ID NOT IN (SELECT ID FROM %2$s WHERE COLLID IN (-2,1)))) AS TAB",docsTable,collTable);
				ResultSet resultSet = methodWatcher.executeQuery(query);
				int correctCount = 4;
				Assert.assertTrue("No Results returned!",resultSet.next());
				Assert.assertEquals("Incorrect count!",correctCount,resultSet.getInt(1));
		}

		@Test
		public void testSubqueryInProjection() throws Exception {
				/*Regression test for DB-1086*/
				String query = String.format("select userid,pmtotal,pmnew, " +
								"(select count(rd) from %1$s where toid=%2$s.userid) calc_total, " +
								"(select count(rd) from %1$s where rd=0 and toid=%2$s.userid) calc_new from %2$s",scanSubqueryT1,scanSubqueryT2);

				ResultSet rs = methodWatcher.executeQuery(query);
				List<int[]> correct = Arrays.asList(
								new int[]{1,0,0,9,3},
								new int[]{2,0,0,4,2}
				);

				List<int[]> actual = Lists.newArrayListWithExpectedSize(correct.size());
				while(rs.next()){
						int[] nextRow = new int[5];
						for(int i=0;i<5;i++){
								int n = rs.getInt(i+1);
								Assert.assertFalse("Incorrectly returned null!",rs.wasNull());
								nextRow[i] = n;
						}
						actual.add(nextRow);
				}
				Assert.assertEquals("Incorrect number of rows returned!",correct.size(),actual.size());
				Collections.sort(actual,new Comparator<int[]>() {
						@Override
						public int compare(int[] o1, int[] o2) {
								if(o1==null){
										if(o2==null) return 0;
										else return -1;
								}else if(o2==null)
										return 1;

								return Ints.compare(o1[0], o2[0]);
						}
				});

				Iterator<int[]> actualIter = actual.iterator();
				for(int[] correctRow:correct){
						int[] actualRow = actualIter.next();
						Assert.assertArrayEquals("Incorrect row!",correctRow,actualRow);
				}
		}
}
