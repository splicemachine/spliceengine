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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Tests around joining two tables using MergeJoin strategies. The idea here is to test different feasibility
 * constraints.
 * @author Scott Fines
 *         Date: 5/6/15
 */
public class TwoTableMergeJoinIT{
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(TwoTableMergeJoinIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher a= new SpliceTableWatcher("A",schemaWatcher.schemaName,"(c1 int, c2 int,c3 int,c4 int, primary key (c1,c2,c3))");
    public static final SpliceTableWatcher b= new SpliceTableWatcher("B",schemaWatcher.schemaName,"(c2 int,c3 int,c4 int, primary key (c3,c2))");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(a)
            .around(b)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(PreparedStatement ps = classWatcher.prepareStatement("insert into "+a+"(c1,c2,c3,c4) values (?,?,?,?)")){
                        ps.setInt(1,1);ps.setInt(2,1);ps.setInt(3,1);ps.setInt(4,1);ps.execute();
                        ps.setInt(1,2);ps.setInt(2,1);ps.setInt(3,1);ps.setInt(4,2);ps.execute();
                        ps.setInt(1,2);ps.setInt(2,2);ps.setInt(3,1);ps.setInt(4,3);ps.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(PreparedStatement ps = classWatcher.prepareStatement("insert into "+b+"(c2,c3,c4) values (?,?,?)")){
                        ps.setInt(1,1);ps.setInt(2,1);ps.setInt(3,1);ps.execute();
                        ps.setInt(1,2);ps.setInt(2,1);ps.setInt(3,1);ps.execute();
                        ps.setInt(1,2);ps.setInt(2,2);ps.setInt(3,1);ps.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });
    private static final String STRATEGY_REGEX="STRATEGERY";

    private static TestConnection conn;

    @BeforeClass
    public static void setUpClass() throws Exception{
        conn = classWatcher.getOrCreateConnection();
    }


    /*Infeasibility tests*/
    @Test(expected=SQLException.class)
    public void testCannotUseMergeOnNonContiguousJoinBToB() throws Exception{
        /*
         * predicates: a.c2 = b.c2
         * expected result: INFEASIBLE
         */
        String sql="select * from "+a+" a,"+b+" b --SPLICE-PROPERTIES joinStrategy=MERGE\n"
                +"where a.c2 = b.c2";
        assertInfeasible(sql);
    }


    @Test(expected=SQLException.class)
    public void testCannotUseMergeOnNonContiguousJoinCol3Col2() throws Exception{
        /*
         * predicates: a.c3 = b.c2
         * expected result: INFEASIBLE
         */
        String sql="select * from "+a+" a ,"+b+" b --SPLICE-PROPERTIES joinStrategy=MERGE\n"
                +"where a.c3 = b.c2";
        assertInfeasible(sql);
    }

    @Test(expected=SQLException.class)
    public void infeasibleMisMatchedColumns() throws Exception{
        /*
         * predicates: a.c1 = b.c2 and a.c2 = b.c3
         * expected result: INFEASIBLE
         */
        String sql="select * from "+a+" a ,"+b+" b --SPLICE-PROPERTIES joinStrategy=MERGE\n"
                +"where a.c1 = b.c2 and a.c2 = b.c3";
        assertInfeasible(sql);
    }

    @Test(expected=SQLException.class)
    public void testCannotMergeWithoutEqualsPredicate() throws Exception{
        /*
         * predicates: a.c1 > b.c2
         * expected result: INFEASIBLE
         */
        String sql="select * from "+a+" a,"+b+" b --SPLICE-PROPERTIES joinStrategy=MERGE\n"
                +"where a.c1 > b.c3";
        assertInfeasible(sql);
    }

    @Test(expected=SQLException.class)
    public void testCannotMergeACrossJoin() throws Exception{
        /*
         * predicates: NONE
         * expected result: INFEASIBLE
         */
        String sql="select * from "+a+" a,"+b+" b --SPLICE-PROPERTIES joinStrategy=MERGE\n";
        assertInfeasible(sql);
    }

    @Test(expected=SQLException.class)
    public void testCannotMergeACrossJoinWithInnerPredicatesOnly() throws Exception{
        /*
         * predicates: b.c3 = 1
         * expected result: INFEASIBLE
         */
        String sql="select * from "+a+" a,"+b+" b --SPLICE-PROPERTIES joinStrategy=MERGE\n"
                +"where b.c3 = 1";
        assertInfeasible(sql);
    }

    @Test(expected=SQLException.class)
    public void testCannotMergeACrossJoinWithOuterPredicatesOnly() throws Exception{
        /*
         * predicates: a.c1 = 1 and a.c2 = 1
         * expected result: INFEASIBLE
         */
        String sql="select * from "+a+" a,"+b+" b --SPLICE-PROPERTIES joinStrategy=MERGE\n"
                +"where a.c1 = 1 and a.c2 = 1";
        assertInfeasible(sql);
    }

    @Test(expected=SQLException.class)
    public void testCannotMergeACrossJoinWithBothInnerAndOuterPredicates() throws Exception{
        /*
         * predicates: a.c1 = 1 and a.c2 = 1 and b.c3 = 1
         * expected result: INFEASIBLE
         */
        String sql="select * from "+a+" a,"+b+" b--SPLICE-PROPERTIES joinStrategy=MERGE\n"
                +"where a.c1 = 1 and a.c2 = 1 and b.c3 = 1 and b.c2 = 1";
        assertInfeasible(sql);
    }

    /*Correctness tests*/

    /*
     * These tests test correctness by issueing the same query twice--once with MERGE,
     * and once with NESTEDLOOP (we use NESTEDLOOP since it's the most Derby-like, and hence the least
     * likely to have a hidden bug). If the two results match, then we are good, otherwise, time to explode
     */
    @Test
    public void testMatchingFirstPredicate() throws Exception{
        /*
         * predicates: a.c1 = b.c2
         */
        String sql = "select * from "+a+" a,"+b+" b --SPLICE-PROPERTIES joinStrategy="+STRATEGY_REGEX+"\n"
                +" where a.c1 = b.c3";
        assertCorrect(sql);
    }


    @Test
    public void testMatchingTwoPredicates() throws Exception{
        /*
         * predicates: a.c1 = b.c3 and a.c2 = b.c2
         */
        String sql = "select * from "+a+" a,"+b+" b --SPLICE-PROPERTIES joinStrategy="+STRATEGY_REGEX+"\n"
                +" where a.c1 = b.c3 and a.c2 = b.c2";
        assertCorrect(sql);
    }

    @Test
    public void testMatchingFirstPredicateAndEqualsInnerSecondColumn() throws Exception{
        /*
         * predicates: a.c1 = b.c3 and b.c2 = 1
         */
        String sql = "select * from "+a+" a,"+b+" b --SPLICE-PROPERTIES joinStrategy="+STRATEGY_REGEX+"\n"
                +" where a.c1 = b.c3 and b.c2 = 1";
        assertCorrect(sql);
    }

    @Test
    public void testMatchingFirstPredicateAndEqualsInnerFirstColumn() throws Exception{
        /*
         * predicates: a.c1 = b.c2 and b.c3 = 1
         */
        String sql = "select * from "+a+" a,"+b+" b --SPLICE-PROPERTIES joinStrategy="+STRATEGY_REGEX+"\n"
                +" where a.c1 = b.c2 and b.c3 = 1";
        assertCorrect(sql);
    }

    @Test
    public void testMatchingFirstPredicateAndEqualsOuterFirstColumn() throws Exception{
        /*
         * predicates: a.c2 = b.c3 and a.c1 = 1
         */
        String sql = "select * from "+a+" a,"+b+" b--SPLICE-PROPERTIES joinStrategy="+STRATEGY_REGEX+"\n"
                +" where a.c1 = 1 and a.c2 = b.c3";
        assertCorrect(sql);
    }

    /* **************************************************************************************************************/
    /*private helper methods*/
    private void assertInfeasible(String sql) throws SQLException{
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery(sql)){
                rs.next();
            }
            Assert.fail("Did not detect infeasible plan!");
        }catch(SQLException se){
            String code = se.getSQLState();
            Assert.assertEquals("Incorrect error code!",SQLState.LANG_NO_BEST_PLAN_FOUND,code);
            throw se;
        }
    }

    private static final Comparator<int[]> lexArrayComparator=new Comparator<int[]>(){
        @Override
        public int compare(int[] o1,int[] o2){
            //sort nulls first
            if(o1==null) return -1;
            else if(o2==null) return 1;
            int l=Math.min(o1.length,o2.length);
            for(int i=0;i<l;i++){
                int compare=Integer.compare(o1[i],o2[i]);
                if(compare!=0) return compare;
            }
                /*
                 * The two arrays are equal on their overlap. Now we sort by length
                 * so that shorter arrays sort first
                 */
            if(o2.length>l)
                return -1;
            else if(o1.length>l) return 1;
            else return 0;
        }
    };

    private void assertCorrect(String sql) throws SQLException{
        /*
         * Row schema assumed to be:
         *  a.c1 | a.c2 | a.c3 |a.c4 | b.c2 | b.c3 | b.c4
         *
         * If you change the tables, be careful to change the schema also
         */
        List<int[]> mergeAnswers = query(sql.replace(STRATEGY_REGEX,"MERGE"));
        List<int[]> nljAnswers = query(sql.replace(STRATEGY_REGEX,"NESTEDLOOP"));

        Collections.sort(mergeAnswers,lexArrayComparator);
        Collections.sort(nljAnswers,lexArrayComparator);
        Assert.assertEquals("Different row counts returned!",nljAnswers.size(),mergeAnswers.size());
        for(int i=0;i<nljAnswers.size();i++){
            int[] mergeA = mergeAnswers.get(i);
            int[] nljA = nljAnswers.get(i);
            Assert.assertArrayEquals("Incorrect answers at position "+i+"!",nljA,mergeA);
        }
    }

    private List<int[]> query(String sql) throws SQLException{
        List<int[]> rows = new ArrayList<>();
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery(sql)){
                while(rs.next()){
                    int[] newValues=new int[7];
                    for(int i=0;i<newValues.length;i++){
                        int next=rs.getInt(i+1);
                        Assert.assertFalse("Incorrectly saw a null value!",rs.wasNull());
                        newValues[i]=next;
                    }
                    rows.add(newValues);
                }
            }
        }
        Assert.assertTrue("No rows found!",rows.size()>0);
        return rows;
    }
}
