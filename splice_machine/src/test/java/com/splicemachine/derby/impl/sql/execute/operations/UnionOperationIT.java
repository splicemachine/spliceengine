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

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.util.StatementUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.collect.Ordering;
import splice.com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.resultSetSize;
import static org.junit.Assert.*;

public class UnionOperationIT extends SpliceUnitTest {

    private static final String CLASS_NAME = UnionOperationIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);

    private static final Comparator<int[]> intArrayComparator= (o1, o2) -> {
        int compare;
        for(int i=0;i<Math.min(o1.length,o2.length);i++){
            compare = Integer.compare(o1[i],o2[i]);
            if(compare!=0) return compare;
        }
        return 0;
    };

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(new SpliceSchemaWatcher(CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/UnionOperationIT.sql", CLASS_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private TestConnection conn;

    @Before
    public void setUp() throws Exception{
        conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
        conn.reset();
    }

    @Test
    public void testUnionAll() throws Exception {
        try(Statement s = conn.createStatement()){
            List<String> names=queryToStringList(s,"select name from ST_MARS UNION ALL select name from ST_EARTH");
            assertEquals(10,names.size());
            assertEquals(Ordering.natural().nullsLast().sortedCopy(names),
                    Lists.newArrayList(
                            "Duncan-Robert","Mulgrew-Kate","Nimoy-Leonard","Nimoy-Leonard","Patrick","Ryan-Jeri",
                            "Shatner-William","Spiner-Brent",null,null
                    ));
        }
    }


    @Test
    public void testUnionOneColumn() throws Exception {
        try(Statement s = conn.createStatement()){
            List<String> names=queryToStringList(s,"select name from ST_MARS UNION select name from ST_EARTH");
            assertEquals(8,names.size());
            assertEquals(Ordering.natural().nullsLast().sortedCopy(names),
                    Lists.newArrayList(
                            "Duncan-Robert","Mulgrew-Kate","Nimoy-Leonard","Patrick","Ryan-Jeri","Shatner-William",
                            "Spiner-Brent",null
                    ));
        }
    }

    /* This needs to use a provider interface for both of its traversals and not use isScan - JL */
    @Test
    public void testValuesUnion() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("SELECT TTABBREV, TABLE_TYPE from (VALUES ('T','TABLE'), ('S','SYSTEM TABLE'), ('V', 'VIEW'), ('A', 'SYNONYM')) T (TTABBREV,TABLE_TYPE)")){
                assertTrue(resultSetSize(rs)>0);
            }
        }
    }

    @Test
    public void testUnion() throws Exception {
        try(Statement s = conn.createStatement()){
            List<Integer> idList=queryToIntList(s,"select empId from ST_MARS UNION select empId from ST_EARTH");
            Set<Integer> idSet=Sets.newHashSet(idList);
            assertEquals(5,idSet.size());
            assertEquals("Expected no duplicates in query result",idList.size(),idSet.size());
        }
    }

    @Test
    public void testUnionNoSort() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select * from ST_MARS UNION select * from ST_EARTH")){
                assertEquals(8,resultSetSize(rs));
            }
        }
    }

    @Test
    public void testUnionWithSort() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select * from ST_MARS UNION select * from ST_EARTH order by 1 desc")){
                assertEquals(8,resultSetSize(rs));
            }
        }
    }

    /* Regression test for Bug 373 */
    @Test
    public void testUnionWithWhereClause() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select * from ST_MARS where empId = 6 UNION select * from ST_EARTH where empId=3")){
                assertEquals(2,resultSetSize(rs));
            }
        }
    }

    /* Regression for Bug 292 */
    @Test
    public void testUnionValuesInSubSelect() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select empId from ST_MARS where empId in (select empId from ST_EARTH union all values 1)")){
                assertEquals(5,resultSetSize(rs));
            }
        }
    }

    @Test
    public void testValuesFirstInUnionAll() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("values (9,10) union all select a.i, b.i from T2 a, DUPS b union all select b.i, a.i from T2 a, DUPS b")){
                assertEquals(33,resultSetSize(rs));
            }
        }
    }

    @Test
    public void testValuesLastInUnionAll() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery(
                    "select a.i, b.i from T2 a, DUPS b union all select b.i, a.i from T2 a, DUPS b union all values (9,10)")){
                assertEquals(33,resultSetSize(rs));
            }
        }
    }

    // 792
    @Test
    public void unionOverScalarAggregate_max() throws Exception {
        try(Statement s= conn.createStatement()){
            List<Integer> maxList=queryToIntList(s,"select max(a.i) from T1 a union select max(b.i) from T1 b");
            assertFalse(maxList.contains(null));
            assertEquals("union should return 1 rows",1,maxList.size());
        }
    }

    // Bug 791
    @Test
    public void unionAllOverScalarAggregate_max() throws Exception {
        try(Statement s= conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select max(a.i) from T1 a UNION ALL select max(b.i) from T1 b")){
                assertEquals("union all should return 2 rows",2,resultSetSize(rs));
            }
        }
    }

    /* bug DB-1304 */
    @Test
    public void unionOverScalarAggregate_count() throws Exception {
        try(Statement s = conn.createStatement()){
            long count2=StatementUtils.onlyLong(s,"select count(*) from empty_table_1 UNION select count(*) from empty_table_2");
            long count3=StatementUtils.onlyLong(s,"select count(*) from empty_table_1 UNION select count(*) from empty_table_2 UNION select count(*) from empty_table_3");
            long count4=StatementUtils.onlyLong(s,"select count(*) from empty_table_1 UNION select count(*) from empty_table_2 UNION select count(*) from empty_table_3 UNION select count(*) from empty_table_4");
            assertEquals("count2 incorrect",0,count2);
            assertEquals("count3 incorrect!",0,count3);
            assertEquals("count4 incorrect!",0,count4);
        }
    }

    /* bug DB-1304 */
    @Test
    public void unionAllOverScalarAggregate_count() throws Exception {
        try(Statement s = conn.createStatement()){
            List<Long> counts=queryToLongList(s,"select count(*) from empty_table_1 UNION ALL select count(*) from empty_table_2");
            assertEquals(Arrays.asList(0L,0L),counts);

            counts=queryToLongList(s,"select count(*) from empty_table_1 UNION ALL select count(*) from empty_table_2 UNION ALL select count(*) from empty_table_3");
            assertEquals(Arrays.asList(0L,0L,0L),counts);

            counts=queryToLongList(s,"select count(*) from empty_table_1 UNION ALL select count(*) from empty_table_2 UNION ALL select count(*) from empty_table_3 UNION ALL select count(*) from empty_table_4");
            assertEquals(Arrays.asList(0L,0L,0L,0L),counts);
        }
    }

    /* bug DB-1304 */
    @Test
    public void unionAllOverScalarAggregate_countNonZero() throws Exception {
        try(Statement s= conn.createStatement()){
            long COUNT1=1+new Random().nextInt(9);
            long COUNT2=1+COUNT1+new Random().nextInt(9);
            insert(s,COUNT1,"insert into empty_table_1 values(100, 200, '')");
            insert(s,COUNT2,"insert into empty_table_4 values(100, 200, '')");

            List<Long> counts=queryToLongList(s,""+
                    "          select count(*) from empty_table_1 "+
                    "UNION ALL select count(*) from empty_table_2 "+
                    "UNION ALL select count(*) from empty_table_3 "+
                    "UNION ALL select count(*) from empty_table_4");
            Collections.sort(counts);

            assertEquals(Arrays.asList(0L,0L,COUNT1,COUNT2),counts);
        }
    }


    // Bug 852
    @Test
    public void testMultipleUnionsInASubSelect() throws Exception {
        try(Statement s = conn.createStatement()){
            List<Integer> actual=queryToIntList(s,
                    "select i from T1 where exists (select i from T2 where T1.i < i union \n"+
                            "select i from T2 where 1 = 0 union select i from T2 where T1.i < i union select\n"+
                            "i from T2 where 1 = 0)"
            );
            Collections.sort(actual);
            assertEquals("Incorrect result contents!",Arrays.asList(1,2),actual);
        }
    }


    /* Regression test #1 for DB-1038 */
    @Test
    public void testUnionDistinctValues() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("values (1,2,3,4) union distinct values (5,6,7,8) union distinct values (9,10,11,12)")){
                int[][] correct=new int[][]{
                        new int[]{9,10,11,12},
                        new int[]{5,6,7,8},
                        new int[]{1,2,3,4}
                };
                Arrays.sort(correct,intArrayComparator);
                int[][] actual=new int[correct.length][];
                int count=0;
                while(rs.next()){
                    int first=rs.getInt(1);
                    int second=rs.getInt(2);
                    int third=rs.getInt(3);
                    int fourth=rs.getInt(4);
                    actual[count]=new int[]{first,second,third,fourth};
                    count++;
                }
                Arrays.sort(actual,intArrayComparator);
                for(int i=0;i<correct.length;i++){
                    assertArrayEquals("Incorrect value!",correct[i],actual[i]);
                }
            }
        }
    }

    /* Regression test #2 for DB-1038 */
    @Test
    public void testUnionValues() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("values (1,2,3,4) union values (5,6,7,8) union values (9,10,11,12)")){
                int[][] correct=new int[][]{
                        new int[]{1,2,3,4},
                        new int[]{5,6,7,8},
                        new int[]{9,10,11,12}
                };
                Arrays.sort(correct,intArrayComparator);
                int[][] actual=new int[correct.length][];
                int count=0;
                while(rs.next()){
                    int first=rs.getInt(1);
                    int second=rs.getInt(2);
                    int third=rs.getInt(3);
                    int fourth=rs.getInt(4);
                    actual[count]=new int[]{first,second,third,fourth};
                    count++;
                }
                Arrays.sort(actual,intArrayComparator);
                for(int i=0;i<correct.length;i++){
                    assertArrayEquals("Incorrect value!",correct[i],actual[i]);
                }
            }
        }
    }

    /* Regression test for DB-7154 */
    @Test
    public void testUnionMaterializedSubquery() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("\n" +
                    "select IBMREQD from SYSIBM.SYSDUMMY1 where IBMREQD in\n" +
                    "(select a.IBMREQD from SYSIBM.SYSDUMMY1 a join SYSIBM.SYSDUMMY1 b\n" +
                    "on 1 = 1 \n" +
                    "UNION \n" +
                    "select IBMREQD from SYSIBM.SYSDUMMY1 )")){
                assertTrue(rs.next());
                assertEquals("Y", rs.getString(1));
            }
        }
    }

    /* Regression test for DB-1026 */
    @Test
    public void testMultipleUnionValues() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select distinct * from (values 2.0,2.1,2.2,2.2) v1 order by 1")){
                float[] correct=new float[]{2.0f,2.1f,2.2f};
                float[] actual=new float[correct.length];
                int count=0;
                while(rs.next()){
                    assertTrue("Too many rows returned!",count<correct.length);
                    float n=rs.getFloat(1);
                    actual[count]=n;
                    count++;
                }

                assertArrayEquals("Incorrect values, there should be no rounding error present!",correct,actual,1e-5f);
            }
        }
    }

    // Regression test for DB-2437
    @Test
    public void testValuesUnionQuery() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery("values 2 union select a.col1 from empty_table_1 a where 1=0")){
                assertEquals(1,resultSetSize(rs));
            }
        }
    }

    @Test
    public void testUnionOfCharTypeWithDifferentLength() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery("select '--' || A || '--' from (select 'HOME' as A from sys.sysschemas union select 'OFFICE' as A from sys.sysschemas) dt")){
                Assert.assertEquals("1     |\n" +
                        "------------\n" +
                        " --HOME--  |\n" +
                        "--OFFICE-- |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
        }
    }

    @Test
    public void testUnionAllAliasInFirstSubqueryOnly() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery("SELECT SUM(SUMMARY) AS S\n" +
                    "  FROM (\n" +
                    "      SELECT COUNT(*) AS SUMMARY FROM ST_MARS WHERE empId=3\n" +
                    "      UNION ALL\n" +
                    "      SELECT COUNT(*) FROM ST_EARTH WHERE empId=4\n" +
                    "      UNION ALL\n" +
                    "      SELECT COUNT(*) FROM ST_MARS WHERE empId=5\n" +
                    "  ) T")) {
                Assert.assertEquals("S |\n" +
                                "----\n" +
                                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
        }
    }

    @Test
    public void testUnionAliasInFirstSubqueryOnly() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery("SELECT SUM(SUMMARY) AS S\n" +
                    "  FROM (\n" +
                    "      SELECT COUNT(*) AS SUMMARY FROM ST_MARS WHERE empId=3\n" +
                    "      UNION\n" +
                    "      SELECT COUNT(*) FROM ST_EARTH WHERE empId=4\n" +
                    "      UNION\n" +
                    "      SELECT COUNT(*) FROM ST_MARS WHERE empId=5\n" +
                    "  ) T")) {
                Assert.assertEquals("S |\n" +
                        "----\n" +
                        " 1 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
        }
    }

    @Test
    public void testUnionConflictingAliasInSecondUnion() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery("SELECT SUM(SUMMARY) AS S\n" +
                    "  FROM (\n" +
                    "      SELECT COUNT(*) AS SUMMARY FROM ST_MARS WHERE empId=3\n" +
                    "      UNION ALL\n" +
                    "      SELECT COUNT(*) AS NOT_SUMMARY FROM ST_EARTH WHERE empId=4\n" +
                    "      UNION ALL\n" +
                    "      SELECT COUNT(*) FROM ST_MARS WHERE empId=5\n" +
                    "  ) T")) {
                Assert.assertEquals("S |\n" +
                        "----\n" +
                        " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
        }
    }

    @Test
    public void testUnionUsingAliasInWhereClause() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery("SELECT *\n" +
                    "  FROM (\n" +
                    "      SELECT COUNT(*) AS SUMMARY FROM ST_MARS WHERE empId=3\n" +
                    "      UNION ALL\n" +
                    "      SELECT COUNT(*) AS NOT_SUMMARY FROM ST_EARTH WHERE empId=4\n" +
                    "      UNION ALL\n" +
                    "      SELECT COUNT(*) FROM ST_MARS WHERE empId=5\n" +
                    "  ) T\n" +
                    "  WHERE SUMMARY = 1")) {
                Assert.assertEquals("1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 1 |\n" +
                        " 1 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
        }
    }

    @Test
    public void testLongValuesClause() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery("SELECT count(asof_ts) FROM (VALUES (TIMESTAMPADD(SQL_TSI_DAY, 0, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 1, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 2, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 3, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 4, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 5, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 6, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 7, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 8, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 9, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 10, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 11, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 12, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 13, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 14, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 15, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 16, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 17, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 18, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 19, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 20, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 21, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 22, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 23, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 24, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 25, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 26, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 27, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 28, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 29, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 30, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 31, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 32, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 33, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 34, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 35, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 36, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 37, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 38, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 39, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 40, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 41, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 42, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 43, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 44, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 45, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 46, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 47, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 48, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 49, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 50, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 51, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 52, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 53, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 54, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 55, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 56, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 57, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 58, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 59, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 60, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 61, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 62, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 63, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 64, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 65, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 66, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 67, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 68, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 69, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 70, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 71, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 72, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 73, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 74, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 75, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 76, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 77, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 78, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 79, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 80, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 81, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 82, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 83, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 84, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 85, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 86, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 87, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 88, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 89, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 90, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 91, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 92, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 93, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 94, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 95, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 96, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 97, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 98, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 99, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 100, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 101, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 102, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 103, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 104, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 105, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 106, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 107, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 108, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 109, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 110, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 111, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 112, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 113, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 114, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 115, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 116, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 117, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 118, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 119, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 120, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 121, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 122, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 123, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 124, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 125, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 126, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 127, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 128, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 129, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 130, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 131, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 132, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 133, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 134, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 135, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 136, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 137, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 138, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 139, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 140, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 141, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 142, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 143, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 144, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 145, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 146, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 147, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 148, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 149, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 150, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 151, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 152, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 153, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 154, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 155, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 156, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 157, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 158, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 159, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 160, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 161, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 162, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 163, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 164, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 165, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 166, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 167, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 168, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 169, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 170, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 171, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 172, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 173, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 174, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 175, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 176, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 177, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 178, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 179, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 180, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 181, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 182, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 183, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 184, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 185, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 186, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 187, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 188, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 189, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 190, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 191, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 192, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 193, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 194, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 195, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 196, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 197, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 198, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 199, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 200, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 201, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 202, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 203, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 204, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 205, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 206, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 207, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 208, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 209, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 210, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 211, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 212, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 213, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 214, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 215, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 216, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 217, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 218, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 219, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 220, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 221, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 222, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 223, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 224, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 225, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 226, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 227, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 228, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 229, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 230, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 231, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 232, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 233, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 234, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 235, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 236, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 237, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 238, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 239, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 240, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 241, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 242, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 243, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 244, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 245, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 246, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 247, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 248, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 249, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 250, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 251, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 252, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 253, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 254, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 255, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 256, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 257, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 258, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 259, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 260, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 261, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 262, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 263, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 264, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 265, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 266, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 267, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 268, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 269, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 270, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 271, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 272, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 273, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 274, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 275, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 276, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 277, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 278, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 279, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 280, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 281, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 282, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 283, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 284, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 285, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 286, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 287, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 288, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 289, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 290, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 291, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 292, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 293, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 294, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 295, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 296, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 297, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 298, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 299, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 300, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 301, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 302, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 303, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 304, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 305, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 306, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 307, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 308, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 309, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 310, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 311, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 312, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 313, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 314, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 315, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 316, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 317, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 318, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 319, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 320, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 321, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 322, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 323, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 324, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 325, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 326, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 327, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 328, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 329, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 330, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 331, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 332, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 333, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 334, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 335, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 336, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 337, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 338, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 339, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 340, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 341, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 342, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 343, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 344, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 345, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 346, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 347, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 348, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 349, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 350, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 351, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 352, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 353, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 354, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 355, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 356, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 357, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 358, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 359, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 360, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 361, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 362, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 363, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 364, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 365, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 366, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 367, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 368, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 369, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 370, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 371, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 372, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 373, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 374, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 375, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 376, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 377, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 378, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 379, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 380, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 381, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 382, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 383, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 384, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 385, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 386, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 387, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 388, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 389, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 390, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 391, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 392, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 393, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 394, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 395, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 396, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 397, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 398, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 399, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 400, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 401, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 402, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 403, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 404, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 405, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 406, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 407, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 408, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 409, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 410, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 411, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 412, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 413, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 414, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 415, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 416, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 417, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 418, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 419, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 420, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 421, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 422, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 423, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 424, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 425, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 426, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 427, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 428, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 429, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 430, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 431, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 432, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 433, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 434, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 435, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 436, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 437, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 438, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 439, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 440, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 441, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 442, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 443, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 444, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 445, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 446, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 447, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 448, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 449, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 450, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 451, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 452, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 453, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 454, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 455, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 456, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 457, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 458, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 459, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 460, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 461, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 462, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 463, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 464, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 465, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 466, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 467, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 468, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 469, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 470, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 471, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 472, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 473, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 474, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 475, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 476, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 477, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 478, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 479, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 480, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 481, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 482, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 483, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 484, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 485, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 486, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 487, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 488, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 489, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 490, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 491, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 492, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 493, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 494, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 495, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 496, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 497, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 498, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 499, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 500, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 501, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 502, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 503, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 504, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 505, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 506, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 507, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 508, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 509, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 510, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 511, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 512, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 513, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 514, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 515, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 516, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 517, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 518, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 519, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 520, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 521, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 522, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 523, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 524, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 525, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 526, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 527, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 528, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 529, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 530, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 531, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 532, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 533, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 534, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 535, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 536, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 537, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 538, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 539, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 540, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 541, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 542, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 543, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 544, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 545, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 546, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 547, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 548, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 549, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 550, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 551, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 552, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 553, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 554, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 555, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 556, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 557, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 558, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 559, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 560, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 561, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 562, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 563, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 564, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 565, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 566, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 567, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 568, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 569, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 570, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 571, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 572, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 573, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 574, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 575, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 576, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 577, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 578, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 579, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 580, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 581, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 582, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 583, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 584, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 585, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 586, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 587, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 588, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 589, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 590, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 591, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 592, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 593, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 594, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 595, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 596, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 597, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 598, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 599, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 600, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 601, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 602, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 603, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 604, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 605, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 606, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 607, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 608, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 609, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 610, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 611, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 612, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 613, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 614, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 615, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 616, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 617, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 618, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 619, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 620, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 621, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 622, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 623, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 624, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 625, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 626, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 627, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 628, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 629, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 630, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 631, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 632, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 633, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 634, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 635, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 636, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 637, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 638, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 639, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 640, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 641, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 642, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 643, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 644, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 645, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 646, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 647, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 648, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 649, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 650, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 651, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 652, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 653, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 654, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 655, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 656, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 657, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 658, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 659, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 660, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 661, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 662, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 663, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 664, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 665, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 666, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 667, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 668, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 669, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 670, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 671, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 672, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 673, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 674, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 675, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 676, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 677, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 678, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 679, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 680, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 681, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 682, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 683, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 684, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 685, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 686, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 687, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 688, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 689, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 690, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 691, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 692, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 693, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 694, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 695, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 696, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 697, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 698, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 699, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 700, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 701, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 702, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 703, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 704, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 705, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 706, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 707, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 708, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 709, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 710, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 711, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 712, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 713, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 714, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 715, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 716, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 717, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 718, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 719, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 720, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 721, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 722, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 723, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 724, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 725, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 726, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 727, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 728, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 729, timestamp('2019-01-01 00:00:00'))),(TIMESTAMPADD(SQL_TSI_DAY, 730, timestamp('2019-01-01 00:00:00')))) tt(asof_ts)")) {
                Assert.assertEquals("1  |\n" +
                                    "-----\n" +
                                    "731 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
        }
    }

    @Category(HBaseTest.class)
    @Test
    public void testUnionAllReturnFirstAvailableBranchFirst() throws Exception {
        String sqlText = "select * from (select A.name from ST_EARTH as A, ST_EARTH as B, ST_MARS as C " +
                "where A.name=B.name and B.name=C.name and C.name in (select D.name from ST_MARS as D, ST_MARS as E where D.name=E.name and D.name=C.name) " +
                "union all values ('from_values')) dt {limit 1}";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1      |\n" +
                        "-------------\n" +
                        "from_values |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));

    }
    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void insert(Statement s, long times, String sql) throws Exception {
        for (long i = 0; i < times; i++) {
            s.executeUpdate(sql);
        }
    }

    private List<Integer> queryToIntList(Statement s,String query) throws SQLException{
        try(ResultSet rs = s.executeQuery(query)){
            List<Integer> strs = new LinkedList<>();
            while(rs.next()){
                int val=rs.getInt(1);
                if(rs.wasNull())
                    strs.add(null);
                else
                    strs.add(val);
            }
            return strs;
        }
    }

    private List<Long> queryToLongList(Statement s,String query) throws SQLException{
        try(ResultSet rs = s.executeQuery(query)){
            List<Long> strs = new LinkedList<>();
            while(rs.next()){
                long val=rs.getLong(1);
                if(rs.wasNull())
                    strs.add(null);
                else
                    strs.add(val);
            }
            return strs;
        }
    }

    private List<String> queryToStringList(Statement s,String query) throws SQLException{
        try(ResultSet rs = s.executeQuery(query)){
            List<String> strs = new LinkedList<>();
            while(rs.next()){
                String string=rs.getString(1);
                if(rs.wasNull())
                    strs.add(null);
                else
                    strs.add(string);
            }
            return strs;
        }
    }

}
