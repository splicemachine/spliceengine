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

package com.splicemachine.subquery;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test scalar subqueries -- subqueries that return a single row and single column.  Can appear anywhere an
 * expression can appear.  Subqueries that appear in an ANY/EXISTS/IN operator but return one row/column are still
 * scalar subqueries.
 */
public class Subquery_Scalar_IT {

    private static final String SCHEMA = Subquery_Scalar_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);


    @BeforeClass
    public static void createdSharedTables() throws Exception {
        classWatcher.executeUpdate("create table T1 (k int, l int)");
        classWatcher.executeUpdate("insert into T1 values (0,1),(0,1),(1,2),(2,3),(2,3),(3,4),(4,5),(4,5),(5,6),(6,7),(6,7),(7,8),(8,9),(8,9),(9,10)");

        classWatcher.executeUpdate("create table T2 (k int, l int)");
        classWatcher.executeUpdate("insert into T2 values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8),(8,9),(9,10)");

        classWatcher.executeUpdate("create table T3 (i int)");
        classWatcher.executeUpdate("insert into T3 values (10),(20)");

        classWatcher.executeUpdate("create table T4 (i int)");
        classWatcher.executeUpdate("insert into T4 values (30),(40)");

        classWatcher.executeUpdate("create table T5 (k int)");
        classWatcher.executeUpdate("insert into T5 values (2)");

        classWatcher.executeUpdate("create table sT1 (toid int,rd int)");
        classWatcher.executeUpdate("insert into sT1 values (1,0),(1,0),(1,0),(1,12),(1,15),(1,123),(1,12312),(1,12312),(1,123),(2,0),(2,0),(2,1),(2,2)");

        classWatcher.executeUpdate("create table sT2 (userid int,pmnew int,pmtotal int)");
        classWatcher.executeUpdate("insert into sT2 values (1,0,0),(2,0,0)");

        classWatcher.executeUpdate("CREATE TABLE STAFF\n" +
                "   (EMPNUM   VARCHAR(3) NOT NULL,\n" +
                "    EMPNAME  VARCHAR(20),\n" +
                "    GRADE    DECIMAL(4),\n" +
                "    CITY     VARCHAR(15))");
        classWatcher.executeUpdate("CREATE TABLE PROJ\n" +
                "   (PNUM     VARCHAR(3) NOT NULL,\n" +
                "    PNAME  VARCHAR(20),\n" +
                "    PTYPE    CHAR(6),\n" +
                "    BUDGET   DECIMAL(9),\n" +
                "    CITY     VARCHAR(15)) ");
        classWatcher.executeUpdate("CREATE TABLE WORKS\n" +
                "   (EMPNUM   VARCHAR(3) NOT NULL,\n" +
                "    PNUM     VARCHAR(3) NOT NULL,\n" +
                "    HOURS    DECIMAL(5)\n" +
                "    )");
        classWatcher.getStatement().executeUpdate("insert into STAFF VALUES ('E1','Alice',12,'Deale')");
        classWatcher.getStatement().executeUpdate("insert into STAFF VALUES ('E2','Betty',10,'Vienna')");
        classWatcher.getStatement().executeUpdate("insert into STAFF VALUES ('E3','Carmen',13,'Vienna')");
        classWatcher.getStatement().executeUpdate("insert into STAFF VALUES ('E4','Don',12,'Deale')");
        classWatcher.getStatement().executeUpdate("insert into STAFF VALUES ('E5','Ed',13,'Akron')");
        classWatcher.getStatement().executeUpdate("insert into PROJ VALUES ('P1','MXSS','Design',10000,'Deale')");
        classWatcher.getStatement().executeUpdate("insert into PROJ VALUES ('P2','CALM','Code',30000,'Vienna')");
        classWatcher.getStatement().executeUpdate("insert into PROJ VALUES ('P3','SDP','Test',30000,'Tampa')");
        classWatcher.getStatement().executeUpdate("insert into PROJ VALUES ('P4','SDP','Design',20000,'Deale')");
        classWatcher.getStatement().executeUpdate("insert into PROJ VALUES ('P5','IRM','Test',10000,'Vienna')");
        classWatcher.getStatement().executeUpdate("insert into PROJ VALUES ('P6','PAYR','Design',50000,'Deale')");
        classWatcher.getStatement().executeUpdate("insert into WORKS VALUES ('E1','P1',40), ('E1','P3',80), ('E1','P2',20), ('E1','P4',20), ('E1','P5',12), ('E1','P6',12)");
        classWatcher.getStatement().executeUpdate("insert into WORKS VALUES ('E2','P1',40), ('E2','P2',80)");
        classWatcher.getStatement().executeUpdate("insert into WORKS VALUES ('E3','P2',20)");
        classWatcher.getStatement().executeUpdate("insert into WORKS VALUES ('E4','P2',20), ('E4','P4',40), ('E4','P5',80)");

        TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "test_data/employee.sql", SCHEMA);

        TestUtils.executeSql(classWatcher.getOrCreateConnection(), "" +
                "create table tWithNulls1 (c1 int, c2 int); \n" +
                "create table tWithNulls2 (c1 int, c2 int); \n" +
                "insert into tWithNulls1 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10); \n" +
                "insert into tWithNulls2 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10); ", SCHEMA);



    }

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // VALUES ( <subquery> )
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /* Regression for Bug 285. Make sure that values ((select ..)) works as expected */
    @Test
    public void valuesSubSelect() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("values ((select k from T2 where k=1),2)");
        assertUnorderedResult(rs, "" +
                "1 | 2 |\n" +
                "--------\n" +
                " 1 | 2 |");
    }

    @Test
    public void valuesSubselectWithTwoSelects() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("values ((select k from T2 where k = 1), (select l from T2 where l =4))");
        assertUnorderedResult(rs, "" +
                "1 | 2 |\n" +
                "--------\n" +
                " 1 | 4 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // WHERE <subquery>
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void nullSubqueryCompare() throws Exception {
        classWatcher.executeUpdate("create table works8 (EMPNUM VARCHAR(3) NOT NULL, PNUM VARCHAR(3) NOT NULL,HOURS DECIMAL(5))");
        classWatcher.executeUpdate("insert into works8 values " +
                "('E1','P1',40), ('E1','P2',20), ('E1','P3',80), ('E1','P4',20), ('E1','P5',12), " +
                "('E1','P6',12), ('E2','P1',40), ('E2','P2',80), ('E3','P2',20), ('E4','P2',20), " +
                "('E4','P4',40), ('E4','P5',80), ('E8','P8',NULL)");

        ResultSet rs = methodWatcher.executeQuery(
                "SELECT EMPNUM, PNUM FROM works8 WHERE HOURS > (SELECT W2.HOURS FROM works8 W2 WHERE W2.EMPNUM = 'E8')");
        assertFalse(rs.next());
    }

    @Test
    public void correlatedExpressionSubqueryOnlyReturnOneRow() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("" +
                "select w.empnum from works w where empnum = " +
                "(select empnum from staff s where w.empnum = s.empnum)");
        // WORKS has 12 rows, each should be returned since STAFF contains one of every EMPNUM
        assertUnorderedResult(rs, "" +
                "EMPNUM |\n" +
                "--------\n" +
                "  E1   |\n" +
                "  E1   |\n" +
                "  E1   |\n" +
                "  E1   |\n" +
                "  E1   |\n" +
                "  E1   |\n" +
                "  E2   |\n" +
                "  E2   |\n" +
                "  E3   |\n" +
                "  E4   |\n" +
                "  E4   |\n" +
                "  E4   |");
    }

    @Test
    public void correlatedExpressionSubqueryWithBoundaryCrossingReference() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("" +
                "select works.* from proj, works, staff " +
                "where proj.pnum = works.pnum and staff.empnum = works.empnum and staff.city = " +
                "(select distinct staff.city from staff where proj.city = staff.city)");
        assertUnorderedResult(rs, "" +
                "EMPNUM |PNUM | HOURS |\n" +
                "----------------------\n" +
                "  E1   | P1  |  40   |\n" +
                "  E1   | P4  |  20   |\n" +
                "  E1   | P6  |  12   |\n" +
                "  E2   | P2  |  80   |\n" +
                "  E3   | P2  |  20   |\n" +
                "  E4   | P4  |  40   |");
    }

    @Test
    public void  testDoublyNestedNotExistsSubquery() throws Exception{
        methodWatcher.executeUpdate("SET SCHEMA " + schemaWatcher.schemaName);
        ResultSet rs = methodWatcher.executeQuery("SELECT STAFF.EMPNAME\n" +
                "          FROM STAFF\n" +
                "          WHERE NOT EXISTS\n" +
                "                 (SELECT *\n" +
                "                       FROM PROJ\n" +
                "                       WHERE NOT EXISTS\n" +
                "                             (SELECT *\n" +
                "                                   FROM WORKS\n" +
                "                                   WHERE STAFF.EMPNUM = WORKS.EMPNUM\n" +
                "                                   AND WORKS.PNUM=PROJ.PNUM))");

        assertEquals("the returned resultset has no entry!", true, rs.next());
        assertEquals("The returned result is not correct!", "Alice", rs.getString(1));
        assertEquals("The returned resultset has more than 1 entry!", false, rs.next());
    }

    @Test
    public void correlatedWhereSubqueryWithJoinInSubquery() throws Exception {
        methodWatcher.executeUpdate("create table A (a1 int, a2 int)");
        methodWatcher.executeUpdate("create table B (b1 int)");
        methodWatcher.executeUpdate("create table C (c1 int, c2 int)");
        methodWatcher.executeUpdate("create table D (d1 int)");

        methodWatcher.executeUpdate("insert into A values(1,1),(2,2),(3,3),(4,4),(4,100)");
        methodWatcher.executeUpdate("insert into B values(1),(2),(3),(4)");
        methodWatcher.executeUpdate("insert into C values(1,1),(2,2),(3,100),(4, 100)");
        methodWatcher.executeUpdate("insert into D values(1),(2),(3),(4)");

        ResultSet rs = methodWatcher.executeQuery("" +
                "select a1,b1 from " +
                "A " +
                "join B on A.a1=B.b1  " +
                "where  " +
                "A.a2 = (select max(c2) " +
                "        from " +
                "        C " +
                "        join D on C.c1=D.d1 " +
                "        where C.c1 = A.a1)");

        assertUnorderedResult(rs, "" +
                "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 2 | 2 |\n" +
                " 4 | 4 |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // SELECT <subquery> -- The subquery is part of the selected columns
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    /* Regression test for DB-945 */
    @Test(expected = SQLException.class)
    public void scalarSubqueryExpected() throws Exception {
        try {
            methodWatcher.executeQuery("select ( select t1.k from t1 where t1.k = t2.k union all select t5.k from t5 where t5.k = t2.k),k from t2");
        } catch (SQLException se) {
            String correctSqlState = SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION;
            assertEquals("Incorrect sql state returned!", correctSqlState, se.getSQLState());
            throw se;
        }
    }

    /* Regression test for DB-1086 */
    @Test
    public void subqueryInProjection() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("" +
                "select userid,pmtotal,pmnew, " +
                "(select count(rd) from sT1 where toid=sT2.userid) calc_total, " +
                "(select count(rd) from sT1 where rd=0 and toid=sT2.userid) calc_new from sT2");
        assertUnorderedResult(rs, "" +
                "USERID | PMTOTAL | PMNEW |CALC_TOTAL |CALC_NEW |\n" +
                "------------------------------------------------\n" +
                "   1   |    0    |   0   |     9     |    3    |\n" +
                "   2   |    0    |   0   |     4     |    2    |");
    }

    @Test
    public void selectSubqueryWithNestedFromSubqueryWithGroupBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select " +
                "T3.i," +
                "(select avg(k)     from (select k, sum(l) as max_l from T1 group by k) x) avg_k, " +
                "(select max(max_l) from (select k, sum(l) as max_l from T1 group by k) x) max_l " +
                "from T3");
        assertUnorderedResult(rs, "" +
                "I | AVG_K | MAX_L |\n" +
                "--------------------\n" +
                "10 |   4   |  18   |\n" +
                "20 |   4   |  18   |");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // IN ( <subquery> )
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /* Regression test for Bug 883/884*/
    @Test
    public void subqueryWithSum() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select k from T1 where k not in (select sum(k) from T2)");
        assertUnorderedResult(rs, "" +
                "K |\n" +
                "----\n" +
                " 0 |\n" +
                " 0 |\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 2 |\n" +
                " 3 |\n" +
                " 4 |\n" +
                " 4 |\n" +
                " 5 |\n" +
                " 6 |\n" +
                " 6 |\n" +
                " 7 |\n" +
                " 8 |\n" +
                " 8 |\n" +
                " 9 |");
    }

    // test for JIRA 960
    @Test
    public void materializationOfSubquery() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                "select c1 from tWithNulls1 where c1 in (select max(c1) from tWithNulls2 group by c2) order by c1");
        assertUnorderedResult(rs, "" +
                "C1 |\n" +
                "----\n" +
                "10 |\n" +
                " 3 |");
    }

    /*Test for db-3649*/
    @Test
    public void testFromlistSubqueryWithGroupBy() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("select c1 from tWithNulls1, (select max(c1) as col from tWithNulls2 group by c2) foo where c1 = foo.col order by c1");
        assertUnorderedResult(rs, "" +
                "C1 |\n" +
                "----\n" +
                "10 |\n" +
                " 3 |");
    }

    /* Regression test for DB-1085 */
    @Test
    public void subqueryOverCountReturnsCorrectly() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from T3 where i in (select sum(T3.i) from T4)");
        assertFalse("Rows incorrectly returned!", rs.next());
    }


    //DB-4398
    @Test
    public void testHashJoinWithNestedQuery() throws Exception {
        String query = "explain \n" +
                "SELECT COUNT(*) FROM t1 a1 \n" +
                "WHERE EXISTS \n" +
                "  (SELECT 1 FROM (SELECT a3.k FROM t1 a3 \n" +
                "    LEFT JOIN (SELECT a4.k, MAX(a4.l) AS col FROM t2 a4 WHERE EXISTS \n" +
                "      (SELECT 1 FROM t1 a5 WHERE a5.k = a4.k AND EXISTS \n" +
                "         (SELECT 1 FROM (SELECT a7.k FROM t1 a7  UNION ALL \n" +
                "            SELECT a8.k FROM t1 a8 ) AS a6 \n" +
                "            WHERE a6.k = a5.k)) GROUP BY a4.k) \n" +
                "      AS a9 ON a3.k=a9.k\n" +
                "      ) AS a2 \n" +
                "    WHERE a2.k = a1.k)";
        ResultSet rs = methodWatcher.executeQuery(query);
        int count = 0;
        while(rs.next()) {
            count++;
        }
        assertTrue("Rows incorrectly returned!", count > 0);
    }

    private static void assertUnorderedResult(ResultSet rs, String expectedResult) throws Exception {
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

}
