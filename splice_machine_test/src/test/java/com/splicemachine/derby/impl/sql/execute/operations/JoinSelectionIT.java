package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsEqual.equalTo;

// Note - we are using the format of the EXPLAIN output to pass these tests.  They will need to be updated if the EXPLAIN
// output changes.

public class JoinSelectionIT extends SpliceUnitTest  {

    public static final String CLASS_NAME = JoinSelectionIT.class.getSimpleName().toUpperCase();
    public static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    public static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    public static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("PERSON",CLASS_NAME,"(pid int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), i int)");
    public static final SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("RP_BC_14_1",CLASS_NAME,"(pid int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), i int)");
    public static final SpliceTableWatcher spliceTableRegion = new SpliceTableWatcher("REGION2",CLASS_NAME,
    	"(R_REGIONKEY INTEGER NOT NULL PRIMARY KEY, R_NAME VARCHAR(25))");
    public static final SpliceTableWatcher spliceTableNation = new SpliceTableWatcher("NATION2",CLASS_NAME,
		"(N_NATIONKEY INTEGER NOT NULL PRIMARY KEY, N_NAME VARCHAR(25), N_REGIONKEY INTEGER NOT NULL)");
    
    private static final String PLAN_LINE_LEADER = "->  ";
    private static final String JOIN_STRATEGY_TERMINATOR = "(";
    private static final String NESTED_LOOP_JOIN = "NestedLoopJoin";
    private static final String MERGE_SORT_JOIN = "MergeSortJoin";
    private static final String LO_MERGE_SORT_JOIN = "LeftOuterMergeSortJoin";
    private static final String BROADCAST_JOIN = "BroadcastJoin";
    private static final String LO_BROADCAST_JOIN = "LeftOuterBroadcastJoin";

    @ClassRule
    public static TestRule rule = RuleChain.outerRule(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(spliceTableRegion)
            .around(spliceTableNation)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.setAutoCommit(true);
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) values 1,2,3,4,5,6,7,8,9,10",
							spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher, spliceTableWatcher));

                        spliceClassWatcher.executeUpdate(format("insert into %s (i) values 1,2,3,4,5,6,7,8,9,10",
							spliceTableWatcher2));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher2, spliceTableWatcher2));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher2, spliceTableWatcher2));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher2, spliceTableWatcher2));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher2, spliceTableWatcher2));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher2, spliceTableWatcher2));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher2, spliceTableWatcher2));
                        spliceClassWatcher.executeUpdate(format("insert into %s (i) select i from %s", spliceTableWatcher2, spliceTableWatcher2));

                        spliceClassWatcher.executeUpdate(format(
                    		"insert into %s (r_regionkey, r_name) values " +
                            "(0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA'), (3, 'EUROPE'), (4, 'MIDDLE EAST'), " +
                            "(5, 'AMERICA'), (6, 'AMERICA'), (7, 'AMERICA'), (8, 'AMERICA'), (9, 'AMERICA')",
                            spliceTableRegion));
                        
                        spliceClassWatcher.executeUpdate(format(
                    		"insert into %s (n_nationkey, n_name, n_regionkey) values " +
                            "(0, 'ALGERIA', 0), " +
                            "(1, 'ARGENTINA', 1), " +
                            "(2, 'BRAZIL', 1), " +
                            "(4, 'EGYPT', 4), " +
                            "(5, 'ETHIOPIA', 0), " +
                            "(6, 'FRANCE', 3)",
                            spliceTableNation));

						//TODO: move call to statistics in setup here
						spliceClassWatcher.execute(format("call syscs_util.COLLECT_SCHEMA_STATISTICS('%s',false)",CLASS_NAME));
                        
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    // should be NestedLoopJoin
    @Test
    public void testInnerJoinWithSubqueryFilterExactCriteria() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from %s a2 join " +
            		  "(select person.pid from %s) as a3 " +
            		  " on a2.pid = a3.pid " + 
            		  " where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(NESTED_LOOP_JOIN, joinStrategy);
            	break;
            }
        }   
    }
    
    @Test
    public void testInnerJoinWithSubquery() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from %s a2 join " +
            		  "(select person.pid from %s) as a3 " +
            		  " on a2.pid = a3.pid ", spliceTableWatcher2, spliceTableWatcher));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER) + PLAN_LINE_LEADER.length(),
                        row.indexOf(JOIN_STRATEGY_TERMINATOR));
                Assert.assertThat("Join strategy must be either MERGE_SORT_JOIN or BROADCAST_JOIN", joinStrategy,
                        anyOf(equalTo(MERGE_SORT_JOIN), equalTo(BROADCAST_JOIN)));
            	break;
            }
        }   
    }


    // should be NLJ?  Comes back with MergeSortJoin
    @Test
    public void testLeftOuterJoinWithSubqueryFilterExactCriteria() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from %s a2 left outer join " +
            		  "(select person.pid from %s) as a3 " +
            		  " on a2.pid = a3.pid " + 
            		  " where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(LO_MERGE_SORT_JOIN, joinStrategy);
            	break;
            }
        }     
    }
    
    // should be Broadcast but comes back with MergeSort?
    @Test
    public void testLeftOuterJoinWithSubquery() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from %s a2 left outer join " +
            		  "(select person.pid from %s) as a3 " +
            		  " on a2.pid = a3.pid " , spliceTableWatcher2, spliceTableWatcher));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(LO_MERGE_SORT_JOIN, joinStrategy);
            	break;
            }
        }     
    }

    @Test
    public void testRPLeftOuterJoinWithNestedSubqueries() throws Exception {
		explainQueryNoNestedLoops(
        		format("explain SELECT a2.pid FROM %s a2 " + 
        			  "LEFT OUTER JOIN " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            				  "(SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            				  "ON a2.PID = a3.PID", spliceTableWatcher2, spliceTableWatcher2, spliceTableWatcher), 0);
    }
    
    @Test
    public void testRPLeftOuterJoinWithNestedSubqueriesFilterExactCriteria() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
              format("explain SELECT a2.pid FROM %s a2 " + 
            		  "LEFT OUTER JOIN " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            				  "(SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            				  "ON a2.PID = a3.PID " +
            				  "WHERE a2.PID = 100", spliceTableWatcher2, spliceTableWatcher2, spliceTableWatcher));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(LO_MERGE_SORT_JOIN, joinStrategy);
            	break;
            }
        }    
    }


    @Test
    public void testInnerJoinWithNestedSubqueries() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
              format("explain SELECT a2.pid FROM %s a2 " + 
            		  "INNER JOIN " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            				  "(SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            				  "ON a2.PID = a3.PID", spliceTableWatcher2, spliceTableWatcher2, spliceTableWatcher));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(BROADCAST_JOIN, joinStrategy);
            	break;
            }
        }   
    }
    
    @Test
    public void testInnerJoinWithNestedSubqueriesFilterExactCriteria() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
              format("explain SELECT a2.pid FROM %s a2 " + 
            		  "INNER JOIN " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            				  "(SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            				  "ON a2.PID = a3.PID" +
            				  " where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher2, spliceTableWatcher));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(NESTED_LOOP_JOIN, joinStrategy);
            	break;
            }
        }   
    }


    @Test
    public void testInnerJoinWithSubqueryLHSFilterExactCriteria() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from (select person.pid from %s) as a3 " +
            		  " join %s a2 " +
            		  " on a2.pid = a3.pid " + 
            		  " where a2.pid = 100", spliceTableWatcher, spliceTableWatcher2));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(NESTED_LOOP_JOIN, joinStrategy);
            	break;
            }
        }   
    }
    
    @Test
    public void testInnerJoinWithSubqueryLHS() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from (select person.pid from %s) as a3 " +
            		  " join %s a2 " +
            		  " on a2.pid = a3.pid ", spliceTableWatcher, spliceTableWatcher2));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(), row.indexOf(JOIN_STRATEGY_TERMINATOR));
                Assert.assertTrue(MERGE_SORT_JOIN.equals(joinStrategy) || BROADCAST_JOIN.equals(joinStrategy));
            	break;
            }
        }   
    }


    // should be NLJ
    @Test
    public void testLeftOuterJoinWithSubqueryLHSFilterExactCriteria() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from (select person.pid from %s) as a3 " +
            		  " left outer join %s a2 " +
            		  " on a2.pid = a3.pid " + 
            		  " where a2.pid = 100", spliceTableWatcher, spliceTableWatcher2));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(NESTED_LOOP_JOIN, joinStrategy);
            	break;
            }
        }   
    }

    @Test
    public void testLeftOuterJoinWithSubqueryLHS() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from (select person.pid from %s) as a3 " +
            		  " left outer join %s a2 " +
            		  " on a2.pid = a3.pid ", spliceTableWatcher, spliceTableWatcher2));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(LO_BROADCAST_JOIN, joinStrategy);
            	break;
            }
        }   
    }

    @Test
    public void testLeftOuterJoinWithNestedSubqueryLHSFilterExactCriteria() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            		  "   (SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            		  " left outer join %s a2 " +
            		  " on a2.pid = a3.pid " + 
            		  " where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher, spliceTableWatcher2));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(NESTED_LOOP_JOIN, joinStrategy);
            	break;
            }
        }   
    }
    
    @Test
    public void testInnerJoinWithNestedSubqueryLHSFilterExactCriteria() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            		  "   (SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            		  " join %s a2 " +
            		  " on a2.pid = a3.pid " + 
            		  " where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher, spliceTableWatcher2));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(NESTED_LOOP_JOIN, joinStrategy);
            	break;
            }
        }   
    }

    @Test
    public void testInnerJoinWithNestedSubqueryLHS() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            		  "   (SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            		  " join %s a2 " +
            		  " on a2.pid = a3.pid ", spliceTableWatcher2, spliceTableWatcher, spliceTableWatcher2));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(BROADCAST_JOIN, joinStrategy);
            	break;
            }
        }   
    }

    @Test
    public void testLeftOuterJoinWithNestedSubqueryLHS() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(
              format("explain select a2.pid from " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            		  "   (SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            		  " left outer join %s a2 " +
            		  " on a2.pid = a3.pid ", spliceTableWatcher2, spliceTableWatcher, spliceTableWatcher2));
        int count = 0;
        while (rs.next()) {
            count++;
            if (count == 2) {
    			String row = rs.getString(1);
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(LO_BROADCAST_JOIN, joinStrategy);
            	break;
            }
        }   
    }

    @Test
    public void testNoNestedLoops1() throws Exception {
    	// This tests DB-3608 (wrong row estimate for frequent element match of type varchar).
    	// If it fails, do not ignore it or comment it out. Let it fail until it is fixed.
    	explainQueryNoNestedLoops(
            "explain select * from region2, nation2 where n_regionkey = r_regionkey and r_name = 'AMERICA'", 0);
    }
    
    private void explainQueryNoNestedLoops(String query, int maxJoinChecks) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);

        int rowCount = 0;
        int joinCount = 0;
        String joinStrategy = null;
        while (rs.next()) {
        	rowCount++;
			String row = rs.getString(1);
			if (!row.contains("Join")) continue;
			if (rowCount == 1)
			    joinStrategy = row.substring(0, row.indexOf(JOIN_STRATEGY_TERMINATOR));
			else
				joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER) + PLAN_LINE_LEADER.length(), row.indexOf(JOIN_STRATEGY_TERMINATOR));
			joinCount++;
            Assert.assertNotEquals("Found unexpected bad join strategy", NESTED_LOOP_JOIN, joinStrategy);
            if (maxJoinChecks > 0 && joinCount >= maxJoinChecks) break;
        }
        Assert.assertTrue("Did not find join strategy in plan", joinCount > 0);
    }

}
