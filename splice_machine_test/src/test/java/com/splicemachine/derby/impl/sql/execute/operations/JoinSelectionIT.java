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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowId;

// Note - we are using the format of the EXPLAIN output to pass these tests.  They will need to be updated if the EXPLAIN
// output changes.

public class JoinSelectionIT extends SpliceUnitTest  {

    public static final String CLASS_NAME = JoinSelectionIT.class.getSimpleName().toUpperCase();
    public static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    public static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    public static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("PERSON",CLASS_NAME,"(pid int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), i int)");
    public static final SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("RP_BC_14_1",CLASS_NAME,"(pid int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), i int)");
    
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
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(MERGE_SORT_JOIN, joinStrategy);
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


    
    // RedPoint test - should return BroadcastJoin but not NestedLoopJoin
    // un-Ignore once DB-3460 is fixed
    @Ignore
    @Test
    public void testRPLeftOuterJoinWithNestedSubqueries() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
              format("explain SELECT a2.pid FROM %s a2 " + 
            		  "LEFT OUTER JOIN " +
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
    			String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER)+PLAN_LINE_LEADER.length(),row.indexOf(JOIN_STRATEGY_TERMINATOR));
    			Assert.assertEquals(MERGE_SORT_JOIN, joinStrategy);
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

}
