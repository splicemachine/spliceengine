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
import com.splicemachine.test.SlowTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import splice.com.google.common.base.Joiner;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsEqual.equalTo;

// Note - we are using the format of the EXPLAIN output to pass these tests.  They will need to be updated if the EXPLAIN
// output changes

public class JoinSelectionIT extends SpliceUnitTest  {

    public static final String CLASS_NAME = JoinSelectionIT.class.getSimpleName().toUpperCase();
    public static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    public static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    public static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("PERSON",CLASS_NAME,"(pid int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), i int)");
    public static final SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("RP_BC_14_1",CLASS_NAME,"(pid int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), i int)");
    public static final SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("T",CLASS_NAME,"(i int)");
    public static final SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("A",CLASS_NAME,"(i int,j int,k int)");
    public static final SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("B",CLASS_NAME,"(i int,j int,k int)");
    public static final SpliceTableWatcher spliceTableRegion = new SpliceTableWatcher("REGION2",CLASS_NAME,
    	"(R_REGIONKEY INTEGER NOT NULL PRIMARY KEY, R_NAME VARCHAR(25))");
    public static final SpliceTableWatcher spliceTableNation = new SpliceTableWatcher("NATION2",CLASS_NAME,
		"(N_NATIONKEY INTEGER NOT NULL PRIMARY KEY, N_NAME VARCHAR(25), N_REGIONKEY INTEGER NOT NULL)");
    
    private static final String PLAN_LINE_LEADER = "->  ";
    private static final String JOIN_STRATEGY_TERMINATOR = "(";
    private static final String NESTED_LOOP_JOIN = "NestedLoopJoin";
    private static final String MERGE_SORT_JOIN = "MergeSortJoin";
    private static final String LO_MERGE_SORT_JOIN = "MergeSortLeftOuterJoin";
    private static final String BROADCAST_JOIN = "BroadcastJoin";
    private static final String LO_BROADCAST_JOIN = "BroadcastLeftOuterJoin";
    private static final String LO_NESTED_LOOP_JOIN = "NestedLoopLeftOuterJoin";


    @ClassRule
    public static TestRule rule = RuleChain.outerRule(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
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

                        spliceClassWatcher.executeUpdate(format("insert into %s values (1,1,1), (2,2,2), (3,3,3)", spliceTableWatcher4));
                        spliceClassWatcher.executeUpdate(format("insert into %s values (1,1,1), (2,2,2), (3,3,3)", spliceTableWatcher5));

                        spliceClassWatcher.executeUpdate(format("insert into %s (i) values 1,2,3,4,5,6,7,8,9,10",
                                spliceTableWatcher3));

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
            if (count == 4) {
                String row = rs.getString(1);
                String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER) + PLAN_LINE_LEADER.length(),
                        row.indexOf(JOIN_STRATEGY_TERMINATOR));
                Assert.assertThat("Join strategy must be either NESTED_LOOP_JOIN or BROADCAST_JOIN", joinStrategy,
                        anyOf(equalTo(NESTED_LOOP_JOIN), equalTo(BROADCAST_JOIN)));
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
            if (count == 4) {
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
        fourthRowContainsQuery(
            format("explain select a2.pid from %s a2 left outer join " +
            		  "(select person.pid from %s) as a3 " +
            		  " on a2.pid = a3.pid " +
            		  " where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher),
                LO_NESTED_LOOP_JOIN, methodWatcher);
    }
    
    // should be Broadcast but comes back with MergeSort?
    @Test
    public void testLeftOuterJoinWithSubquery() throws Exception {
        fourthRowContainsQuery(
        	format("explain select a2.pid from %s a2 left outer join " +
            		  "(select person.pid from %s) as a3 " +
            		  " on a2.pid = a3.pid ", spliceTableWatcher2, spliceTableWatcher),
    		LO_MERGE_SORT_JOIN, methodWatcher);
    }

    @Test
    public void testRPLeftOuterJoinWithNestedSubqueries() throws Exception {
		explainQueryNoNestedLoops(
            format("explain SELECT a2.pid FROM %s a2 " + 
        			  "LEFT OUTER JOIN " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            				  "(SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            				  "ON a2.PID = a3.PID", spliceTableWatcher2, spliceTableWatcher2, spliceTableWatcher));
    }
    
    @Test
    public void testRPLeftOuterJoinWithNestedSubqueriesFilterExactCriteria() throws Exception {
        fourthRowContainsQuery(
            format("explain SELECT a2.pid FROM %s a2 " + 
            		  "LEFT OUTER JOIN " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            				  "(SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            				  "ON a2.PID = a3.PID " +
            				  "WHERE a2.PID = 100", spliceTableWatcher2, spliceTableWatcher2, spliceTableWatcher),
                LO_NESTED_LOOP_JOIN, methodWatcher);
    }

    @Test
    public void testInnerJoinWithNestedSubqueries() throws Exception {
    	explainQueryNoNestedLoops(
            format("explain SELECT a2.pid FROM %s a2 " + 
            		  "INNER JOIN " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            				  "(SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            				  "ON a2.PID = a3.PID", spliceTableWatcher2, spliceTableWatcher2, spliceTableWatcher));
//            BROADCAST_JOIN, methodWatcher);
    }
    
    @Test
    public void testInnerJoinWithNestedSubqueriesFilterExactCriteria() throws Exception {
    	fourthRowContainsQuery(
            format("explain SELECT a2.pid FROM %s a2 " + 
            		  "INNER JOIN " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            				  "(SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            				  "ON a2.PID = a3.PID" +
            				  " where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher2, spliceTableWatcher),
            BROADCAST_JOIN, methodWatcher);
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
            if (count == 4) {
                String row = rs.getString(1);
                String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER) + PLAN_LINE_LEADER.length(),
                        row.indexOf(JOIN_STRATEGY_TERMINATOR));
                Assert.assertThat("Join strategy must be either NESTED_LOOP_JOIN or BROADCAST_JOIN", joinStrategy,
                        anyOf(equalTo(NESTED_LOOP_JOIN), equalTo(BROADCAST_JOIN)));
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
            if (count == 4) {
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
            if (count == 4) {
                String row = rs.getString(1);
                String joinStrategy = row.substring(row.indexOf(PLAN_LINE_LEADER) + PLAN_LINE_LEADER.length(),
                        row.indexOf(JOIN_STRATEGY_TERMINATOR));
                Assert.assertThat("Join strategy must be either NESTED_LOOP_JOIN or BROADCAST_JOIN", joinStrategy,
                        anyOf(equalTo(NESTED_LOOP_JOIN), equalTo(BROADCAST_JOIN)));
                break;
            }
        }
    }

    @Test
    public void testLeftOuterJoinWithSubqueryLHS() throws Exception {
    	fourthRowContainsQuery(
            format("explain select a2.pid from (select person.pid from %s) as a3 " +
            		  " left outer join %s a2 " +
            		  " on a2.pid = a3.pid ", spliceTableWatcher, spliceTableWatcher2),
            LO_BROADCAST_JOIN, methodWatcher);
    }

    @Test
    public void testLeftOuterJoinWithNestedSubqueryLHSFilterExactCriteria() throws Exception {
    	fourthRowContainsQuery(
            format("explain select a2.pid from " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            		  "   (SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            		  " left outer join %s a2 " +
            		  " on a2.pid = a3.pid " + 
            		  " where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher, spliceTableWatcher2),
           BROADCAST_JOIN, methodWatcher);
    }
    
    @Test
    public void testInnerJoinWithNestedSubqueryLHSFilterExactCriteria() throws Exception {
    	fourthRowContainsQuery(
            format("explain select a2.pid from " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            		  "   (SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            		  " join %s a2 " +
            		  " on a2.pid = a3.pid " + 
            		  " where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher, spliceTableWatcher2),
            BROADCAST_JOIN, methodWatcher);
    }

    @Test
    public void testInnerJoinWithNestedSubqueryLHS() throws Exception {
    	fourthRowContainsQuery(
            format("explain select a2.pid from " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            		  "   (SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            		  " join %s a2 " +
            		  " on a2.pid = a3.pid ", spliceTableWatcher2, spliceTableWatcher, spliceTableWatcher2),
            BROADCAST_JOIN, methodWatcher);
    }

    @Test
    public void testLeftOuterJoinWithNestedSubqueryLHS() throws Exception {
    	fourthRowContainsQuery(
            format("explain select a2.pid from " +
            		  "(SELECT a4.PID FROM %s a4 WHERE EXISTS " +
            		  "   (SELECT a5.PID FROM %s a5 WHERE a4.PID = a5.PID)) AS a3 " +
            		  " left outer join %s a2 " +
            		  " on a2.pid = a3.pid ", spliceTableWatcher2, spliceTableWatcher, spliceTableWatcher2),
    	    LO_BROADCAST_JOIN, methodWatcher);
    }

    @Test
    public void testNoNestedLoops1() throws Exception {
    	// This tests DB-3608 (wrong row estimate for frequent element match of type varchar).
    	// If it fails, do not ignore it or comment it out. Let it fail until it is fixed.
    	explainQueryNoNestedLoops(
            "explain select * from region2, nation2 where n_regionkey = r_regionkey and r_name = 'AMERICA'");
    }
    
    private void explainQueryNoNestedLoops(String query) throws Exception {
        queryDoesNotContainString(query, NESTED_LOOP_JOIN, methodWatcher);
    }
    
    /* Regression test for DB-3614 */
    @Test
    @Category(SlowTest.class)
    @Ignore("-sf- takes way too long to fail and interferes rest of build")
    public void testTenTableJoinExplainDuration() throws Exception {
    	int size = 10;
    	List<String> tables = new ArrayList<String>(size);
    	List<String> joins = new ArrayList<String>(size - 1);
    	for (int i = 0; i < size; i++) {
	        methodWatcher.executeUpdate(format("create table tentab%s (c1 int primary key)", i));
	        methodWatcher.executeUpdate(format("insert into tentab%s values (1)", i));
	        tables.add(format("tentab%s", i));
	        if (i > 0) {
	        	joins.add(format("tentab%s.c1 = tentab%s.c1", i, i - 1));
	        }
		}
        System.out.println("Tables created");
        final String fromClause = Joiner.on(", ").join(tables);
        final String joinCriteria = Joiner.on(" AND ").join(joins);

        ExecutorService es =Executors.newSingleThreadExecutor(new ThreadFactory(){
            @Override
            public Thread newThread(Runnable r){
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        });
        try{
            final CyclicBarrier startLatch = new CyclicBarrier(2);
            final CountDownLatch finishLatch = new CountDownLatch(1);
            Future<Void> f=es.submit(new Callable<Void>(){
                @Override
                public Void call() throws Exception{
                    String query=format("EXPLAIN SELECT * FROM %s WHERE %s ",fromClause,joinCriteria);
                    startLatch.await();
                    try{
                        ResultSet rs=methodWatcher.executeQuery(query);
                        // Loose check that explain statement took a few seconds or less,
                        // because we're just making sure the short circuit logic in
                        // OptimizerImpl.checkTimeout() blocks this from taking several minutes.
                        Assert.assertTrue("Explain did not return result!",rs.next());
                    }finally{
                        finishLatch.countDown();
                    }
                    return null;
                }
            });
            System.out.println("Starting wait");
            startLatch.await();
            f.get(1,TimeUnit.SECONDS);
            System.out.println("Finished waiting");
        }finally{
            System.out.println("shutting down");
        }
    }

    //DB-3865
    @Test
    public void testLeftJoin() throws Exception {
        thirdRowContainsQuery(
                format("explain select * from %s t1 left join %s t2 on t1.i=t2.i",
                        spliceTableWatcher3, spliceTableWatcher3),
                LO_BROADCAST_JOIN, methodWatcher);
    }

    @Test
    public void testRepetivePredicate() throws Exception {

        // predicates that repeat in all sub-clauses are all extracted as hashable join predicates
        rowContainsQuery(4,
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i and a.j=2)",
                BROADCAST_JOIN, methodWatcher);

        rowContainsQuery(4,
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i and a.j=2)",
                "preds=[(A.I[4:4] = B.I[4:1])]", methodWatcher);

        rowContainsQuery(5,
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i and a.j=2)",
                "preds=[(A.J[2:2] IN (1,2))]", methodWatcher);

        rowContainsQuery(4,
                "explain select * from a, b where (a.i = b.i and a.j=1 and a.j=b.j) or (a.i = b.i and a.j=2 and a.j=b.j)",
                BROADCAST_JOIN, methodWatcher);

        rowContainsQuery(4,
                "explain select * from a, b where (a.i = b.i and a.j=1 and a.j=b.j) or (a.i = b.i and a.j=2 and a.j=b.j)",
                "preds=[(A.I[4:4] = B.I[4:1]),(A.J[4:5] = B.J[4:2])]", methodWatcher);

        rowContainsQuery(5,
                "explain select * from a, b where (a.i = b.i and a.j=1 and a.j=b.j) or (a.i = b.i and a.j=2 and a.j=b.j)",
                "preds=[(A.J[2:2] IN (1,2))]", methodWatcher);

        // Negative test: predicate does not repeat in all clauses
        thirdRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i and a.j=2) or (b.j=1)",
                NESTED_LOOP_JOIN, methodWatcher);

        fourthRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i and a.j=2) or (b.j=1)",
                "preds=[(((A.I[1:1] = B.I[2:1]) and ((A.J[1:2] = 1) and true)) or (((A.I[1:1] = B.I[2:1]) and ((A.J[1:2] = 2) and true)) or ((B.J[2:2] = 1) or false)))]", methodWatcher);

        thirdRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i+1 and a.j=2)",
                NESTED_LOOP_JOIN, methodWatcher);

        fourthRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i+1 and a.j=2)",
                "preds=[(((A.I[1:1] = B.I[2:1]) and ((A.J[1:2] = 1) and true)) or (((A.I[1:1] = (B.I[2:1] + 1)) and ((A.J[1:2] = 2) and true)) or false))]", methodWatcher);

        // Negative test: predicate is under a NOT node
        thirdRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or (not(a.i = b.i) and a.j=2)",
                NESTED_LOOP_JOIN, methodWatcher);

        fourthRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or (not(a.i = b.i) and a.j=2)",
                "preds=[(((A.I[1:1] = B.I[2:1]) and ((A.J[1:2] = 1) and true)) or (((A.I[1:1] <> B.I[2:1]) and ((A.J[1:2] = 2) and true)) or false))]", methodWatcher);

        // Negative test: Clause is not in a DNF form and not subject to optimization
        thirdRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or ((a.i = b.i or a.j=2) and a.i=1)",
                NESTED_LOOP_JOIN, methodWatcher);

        fourthRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or ((a.i = b.i or a.j=2) and a.i=1)",
                "preds=[(((A.I[1:1] = B.I[2:1]) and ((A.J[1:2] = 1) and true)) or ((((A.I[1:1] = B.I[2:1]) or ((A.J[1:2] = 2) or false)) and ((A.I[1:1] = 1) and true)) or false))]", methodWatcher);

    }

    @Test
    public void testBroadcastJoinWithSelect() throws Exception {
        fourthRowContainsQuery(
                format("explain select a2.pid from --SPLICE-PROPERTIES joinOrder=fixed \n" +
                        "%s a2 join (select person.pid from %s person) as a3 --SPLICE-PROPERTIES joinStrategy=broadcast\n" +
                        " on a2.pid = a3.pid where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher),
                BROADCAST_JOIN, methodWatcher);

        String sqlText =  format("select a2.pid from --SPLICE-PROPERTIES joinOrder=fixed \n" +
                "%s a2 join (select person.pid from %s person) as a3 --SPLICE-PROPERTIES joinStrategy=broadcast\n" +
                        " on a2.pid = a3.pid where a2.pid = 100", spliceTableWatcher2, spliceTableWatcher);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("wrong result", 100, rs.getInt(1));

        sqlText =  format("select count(*) from --SPLICE-PROPERTIES joinOrder=fixed \n" +
                "%s a2 join (select person.pid from %s person) as a3 --SPLICE-PROPERTIES joinStrategy=broadcast\n" +
                " on a2.pid = a3.pid", spliceTableWatcher2, spliceTableWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("wrong result", 1280, rs.getInt(1));
    }

    @Test
    public void testRepetivePredicateWithAndedPreds() throws Exception {

        // predicates that repeat in all sub-clauses are all extracted as hashable join predicates
        rowContainsQuery(4,
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i and a.j=2)",
                BROADCAST_JOIN, methodWatcher);

        rowContainsQuery(4,
                "explain select * from a, b where ((a.i = b.i and a.j=1) or (a.i = b.i and a.j=2)) and a.k=3",
                "preds=[(A.I[4:4] = B.I[4:1])]", methodWatcher);

        rowContainsQuery(5,
                "explain select * from a, b where ((a.i = b.i and a.j=1) or (a.i = b.i and a.j=2)) and b.k=3",
                "preds=[(A.J[0:2] IN (1,2))]", methodWatcher);

        rowContainsQuery(4,
                "explain select * from a, b where (a.i = b.i and a.j=1 and a.j=b.j) or (a.i = b.i and a.j=2 and a.j=b.j)",
                BROADCAST_JOIN, methodWatcher);

        rowContainsQuery(3,
                "explain select * from a, b where ((a.i = b.i and a.j=1 and a.j=b.j) or (a.i = b.i and a.j=2 and a.j=b.j)) and b.k=3",
                "preds=[(A.I[4:1] = B.I[4:4]),(A.J[4:2] = B.J[4:5])]", methodWatcher);

        rowContainsQuery(5,
                "explain select * from a, b where ((a.i = b.i and a.j=1 and a.j=b.j) or (a.i = b.i and a.j=2 and a.j=b.j)) and b.k=3",
                "preds=[(A.J[0:2] IN (1,2))]", methodWatcher);

        rowContainsQuery(4,
                "explain select * from a, b where ((a.i = b.i and a.j=1 and a.j=b.j) or (a.i = b.i and a.j=2 and a.j=b.j)) and b.k=3",
                "preds=[(B.K[2:3] = 3)]", methodWatcher);

        // Test DNF in nested level
        rowContainsQuery(5,
                "explain select * from a, b where (((a.i = b.i and a.j=1) or (a.i = b.i and a.j=2)) and a.k=3) and b.k=3",
                "preds=[(A.J[2:2] IN (1,2))]", methodWatcher);


        // Negative test: predicate does not repeat in all clauses
        thirdRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i and a.j=2) or (b.j=1)",
                NESTED_LOOP_JOIN, methodWatcher);

        fourthRowContainsQuery(
                "explain select * from a, b where (a.i = b.i and a.j=1) or (a.i = b.i and a.j=2) or (b.j=1)",
                "preds=[(((A.I[1:1] = B.I[2:1]) and ((A.J[1:2] = 1) and true)) or (((A.I[1:1] = B.I[2:1]) and ((A.J[1:2] = 2) and true)) or ((B.J[2:2] = 1) or false)))]", methodWatcher);

        thirdRowContainsQuery(
                "explain select * from a, b where ((a.i = b.i and a.j=1) or (a.i = b.i+1 and a.j=2)) and a.k=3",
                NESTED_LOOP_JOIN, methodWatcher);


    }

}
