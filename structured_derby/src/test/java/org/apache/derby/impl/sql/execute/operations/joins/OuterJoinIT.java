package org.apache.derby.impl.sql.execute.operations.joins;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.splicemachine.homeless.TestUtils.o;


public class OuterJoinIT extends SpliceUnitTest { 

    private static Logger LOG = Logger.getLogger(OuterJoinIT.class);

    public static final String CLASS_NAME = OuterJoinIT.class.getSimpleName().toUpperCase()+ "_2"; 
    public static final String TABLE_NAME_1 = "A";
    public static final String TABLE_NAME_2 = "CC";
    public static final String TABLE_NAME_3 = "DD";
    public static final String TABLE_NAME_4 = "D";
    public static final String TABLE_NAME_5 = "E";
    public static final String TABLE_NAME_6 = "F";
    public static final String TABLE_NAME_7 = "G";
    public static final String TABLE_NAME_8 = "t1";
    public static final String TABLE_NAME_9 = "t2";
    public static final String TABLE_NAME_10 = "dupes";


    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1, CLASS_NAME, "(si varchar(40),sa character varying(40),sc varchar(40),sd int,se float)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2, CLASS_NAME, "(si varchar(40), sa varchar(40))");
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3, CLASS_NAME, "(si varchar(40), sa varchar(40))");
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4, CLASS_NAME, "(a varchar(20), b varchar(20), c varchar(10), d decimal, e varchar(15))");
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5, CLASS_NAME, "(a varchar(20), b varchar(20), w decimal(4),e varchar(15))");
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6, CLASS_NAME, "(a varchar(20), b varchar(20), c varchar(10), d decimal, e varchar(15))");
    protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_NAME_7, CLASS_NAME, "(a varchar(20), b varchar(20), w decimal(4),e varchar(15))");
    protected static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher(TABLE_NAME_8, CLASS_NAME, "(i int, s smallint, d double precision, r real, c10 char(10), c30 char(30), vc10 varchar(10), vc30 varchar(30))");
    protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher(TABLE_NAME_9, CLASS_NAME, "(i int, s smallint, d double precision, r real, c10 char(10), c30 char(30), vc10 varchar(10), vc30 varchar(30))");
    protected static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher(TABLE_NAME_10, CLASS_NAME, "(i int, s smallint, d double precision, r real, c10 char(10), c30 char(30), vc10 varchar(10), vc30 varchar(30))");

    /*

     
     */
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6)
            .around(spliceTableWatcher7)
            .around(spliceTableWatcher8)
            .around(spliceTableWatcher9)
            .around(spliceTableWatcher10)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s (si, sa, sc,sd,se) values (?,?,?,?,?)", TABLE_NAME_1));
                        for (int i = 0; i < 10; i++) {
                            ps.setString(1, "" + i);
                            ps.setString(2, "i");
                            ps.setString(3, "" + i * 10);
                            ps.setInt(4, i);
                            ps.setFloat(5, 10.0f * i);
                            ps.executeUpdate();
                        }
                        String insertT1 = "insert into %s values (null, null, null, null, null, null, null, null),"
                        		+ "(1, 1, 1e1, 1e1, '11111', '11111 11', '11111', '11111 11')," +
                        		"(2, 2, 2e1, 2e1, '22222', '22222 22', '22222', '22222 22')";                        
                        String insertT2 = "insert into %s values (null, null, null, null, null, null, null, null),"
                        		+ "(3, 3, 3e1, 3e1, '33333', '33333 33', '33333', '33333 33')," +
                        		"(4, 4, 4e1, 4e1, '44444', '44444 44','44444', '44444 44')";                        
                        String insertDupes = "insert into %s select * from t1 union all select * from t2";
                        Statement statement = spliceClassWatcher.getStatement();
                        statement.executeUpdate(format(insertT1,TABLE_NAME_8));
                        statement.executeUpdate(format(insertT2,TABLE_NAME_9));
                        statement.executeUpdate(format(insertDupes,TABLE_NAME_10));
                    
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        Statement statement = spliceClassWatcher.getStatement();
                        statement.execute(String.format("insert into %s values  ('p1','mxss','design',10000,'deale')", TABLE_NAME_4));
                        statement.execute(String.format("insert into %s values  ('e2','alice',12,'deale')", TABLE_NAME_5));
                        statement.execute(String.format("insert into %s values  ('e3','alice',12,'deale')", TABLE_NAME_5));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            }).around(new SpliceDataWatcher() {

                @Override
                protected void starting(Description description) {
                    try {
                        insertData(TABLE_NAME_2, TABLE_NAME_3, spliceClassWatcher);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            }).around(TestUtils.createFileDataWatcher(spliceClassWatcher, "small_msdatasample/startup.sql", CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/employee.sql", CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/basic_join_dataset.sql", CLASS_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    public static void insertData(String t1, String t2, SpliceWatcher spliceWatcher) throws Exception {
        PreparedStatement psC = spliceWatcher.prepareStatement("insert into " + t1 + " values (?,?)");
        PreparedStatement psD = spliceWatcher.prepareStatement("insert into " + t2 + " values (?,?)");
        for (int i = 0; i < 10; i++) {
            psC.setString(1, "" + i);
            psC.setString(2, "i");
            psC.executeUpdate();
            if (i != 9) {
                psD.setString(1, "" + i);
                psD.setString(2, "i");
                psD.executeUpdate();
            }
        }
        spliceWatcher.commit();
    }

    // TESTS


    @Test
    public void testNestedLoopLeftOuterJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select t1.EMPNAME, t1.CITY, t2.PTYPE from STAFF t1 left outer join PROJ t2 --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP \n" +
                " on t1.CITY = t2.CITY");

        List<Map> results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(11, results.size());

    }

    @Test
    public void testScrollableVarcharLeftOuterJoinWithJoinStrategy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc left outer join dd --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
        int j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
            if (!rs.getString(1).equals("9")) {
                Assert.assertNotNull(rs.getString(2));
                Assert.assertEquals(rs.getString(1), rs.getString(2));
            } else {
                Assert.assertNull(rs.getString(2));
            }
        }
        Assert.assertEquals(10, j);
    }

    @Test
    public void testSinkableVarcharLeftOuterJoinWithJoinStrategy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc left outer join dd --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
        int j = 0;
        while (rs.next()) {
            j++;
            LOG.info(String.format("cc.sa=%s,count=%dd", rs.getString(1), rs.getInt(2)));
//			Assert.assertNotNull(rs.getString(1));
//			if (!rs.getString(1).equals("9")) {
//				Assert.assertEquals(1l,rs.getLong(2));
//			}
        }
        Assert.assertEquals(10, j);
    }

    @Test
    @Ignore("DB-216")
    public void testScrollableVarcharRightOuterJoinWithJoinStrategy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc right outer join dd --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
        int j = 0;
        while (rs.next()) {
            j++;
            LOG.info("cc.si=" + rs.getString(1) + ",dd.si=" + rs.getString(2));
            Assert.assertNotNull(rs.getString(2));
            if (!rs.getString(2).equals("9")) {
                Assert.assertNotNull(rs.getString(1));
                Assert.assertEquals(rs.getString(1), rs.getString(2));
            } else {
                Assert.assertNull(rs.getString(1));
            }
        }
        Assert.assertEquals(9, j);
    }

    @Test
    @Ignore("DB-216")
    public void testSinkableVarcharRightOuterJoinWithJoinStrategy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc right outer join dd --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
        int j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
            if (!rs.getString(1).equals("9")) {
                Assert.assertEquals(1l, rs.getLong(2));
            } else {
                Assert.assertNotNull(null);
            }
        }
        Assert.assertEquals(9, j);
    }

    @Test
    public void testScrollableVarcharLeftOuterJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc left outer join dd on cc.si = dd.si");
        int j = 0;
        while (rs.next()) {
            j++;
            String left = rs.getString(1);
            String right = rs.getString(2);
            System.out.printf("left=%s, right=%s%n", left, right);
            Assert.assertNotNull("left side is null", left);
            if (!rs.getString(1).equals("9")) {
                Assert.assertNotNull("right side is null", right);
                Assert.assertEquals(left, right);
            } else {
                Assert.assertNull("right side is not null", rs.getString(2));
            }
        }
        Assert.assertEquals(10, j);
    }

    @Test
    public void testSinkableVarcharLeftOuterJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc left outer join dd on cc.si = dd.si group by cc.si");
        int j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
            if (!rs.getString(1).equals("9")) {
                Assert.assertEquals(1l, rs.getLong(2));
            }
        }
        Assert.assertEquals(10, j);
    }

    @Test
    public void testScrollableVarcharRightOuterJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc right outer join dd on cc.si = dd.si");
        int j = 0;
        while (rs.next()) {
            j++;
            LOG.info("c.si=" + rs.getString(1) + ",d.si=" + rs.getString(2));
            Assert.assertNotNull(rs.getString(2));
            if (!rs.getString(2).equals("9")) {
                Assert.assertNotNull(rs.getString(1));
                Assert.assertEquals(rs.getString(1), rs.getString(2));
            } else {
                Assert.assertNull(rs.getString(1));
            }
        }
        Assert.assertEquals(9, j);
    }

    @Test
    public void testRightOuterJoinWithColsOfMixedType() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, cc.sd, dd.sd, dd.si " +
                "from (select si, 1 AS sd from cc ) cc " +
                "   right outer join " +
                "       (select si, 1.0 AS sd from dd) dd " +
                "   on cc.si = dd.si");
        int j = 0;
        while (rs.next()) {
            j++;
            LOG.info("c.si=" + rs.getString(1) + ",d.si=" + rs.getString(4));
            Assert.assertNotNull(rs.getString(4));
            if (!rs.getString(4).equals("9")) {
                Assert.assertNotNull(rs.getString(1));
                Assert.assertEquals(rs.getString(1), rs.getString(4));
            } else {
                Assert.assertNull(rs.getString(1));
            }
        }
        Assert.assertEquals(9, j);
    }

    @Test
    public void testSinkableVarcharRightOuterJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc right outer join dd on cc.si = dd.si group by cc.si");
        int j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
            if (!rs.getString(1).equals("9")) {
                Assert.assertEquals(1l, rs.getLong(2));
            } else {
                Assert.assertNotNull(null);
            }
        }
        Assert.assertEquals(9, j);
    }

    @Test
    public void testLeftOuterJoinWithIsNull() throws Exception {
        List<Object[]> expected = Collections.singletonList( o("E5") );

        ResultSet rs = methodWatcher.executeQuery("select a.empnum from staff a left outer join works b on a.empnum = b.empnum where b.empnum is null");
        List results = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), results.toArray());
    }

    @Test
    public void testLeftOuterWithLessThan() throws Exception {
        List<Object[]> expected = Arrays.asList(
                o("E2"),
                o("E4"),
                o("E5"));

        ResultSet rs = methodWatcher.executeQuery("select a.empnum from staff a left outer join staff b " +
                "on a.empnum < b.empnum and a.grade = b.grade " +
                "where b.grade is null order by a.empnum");
        List results = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), results.toArray());
    }

    @Test
    public void testRepeatedLeftOuterWithLessThan() throws Exception {
        for(int i=0;i<100;i++){
            System.out.println(i);
            testLeftOuterWithLessThan();
        }
    }
    
    @Test
    public void testUnionLeftJoinedToTable() throws Exception {
    	ResultSet rs = methodWatcher.executeQuery("select * from (select * from t1 union select * from t2) x2 left join t2 on x2.i = t2.i");
    	int count = 0;
    	while (rs.next()) {
    		count++;
    	}
    	Assert.assertEquals("Returned the wrong number of rows", 5,count);
    }

    @Test
    public void testThatPredicatesAreNotDiscarded() throws Exception {
        // Regression tests for DB-1006, which dupes DB-1005 & DB-1084
        Map<String,Integer> expectedCounts =
            ImmutableMap.of("SELECT STAFF.CITY,EMPNAME,PNAME,BUDGET " +
                                  "     FROM STAFF left outer JOIN PROJ " +
                                  "       ON STAFF.CITY = PROJ.CITY ",
                               11,
                               "SELECT STAFF.CITY,EMPNAME,PNAME,BUDGET " +
                                   "     FROM STAFF left outer JOIN PROJ " +
                                   "       ON STAFF.CITY = PROJ.CITY " +
                                   "      AND STAFF.CITY <> 'Vienna' ",
                                9,
                               "SELECT STAFF.CITY,EMPNAME,PNAME,BUDGET " +
                                   "     FROM STAFF left outer JOIN PROJ " +
                                   "on EMPNAME <> 'Don' ",
                               25,
                               "SELECT STAFF.CITY,EMPNAME,PNAME,BUDGET " +
                                   "     FROM STAFF left outer JOIN PROJ " +
                                   "       ON STAFF.CITY = PROJ.CITY " +
                                   "      AND STAFF.CITY <> 'Vienna' " +
                                   "      AND EMPNAME <> 'Don' " +
                                   "     WHERE BUDGET > 15000 OR BUDGET IS NULL " +
                                   "   ORDER BY STAFF.CITY, EMPNAME, BUDGET ",
                                6);

        for (Map.Entry<String,Integer> q: expectedCounts.entrySet()){
            List results = TestUtils.resultSetToArrays(methodWatcher.executeQuery(q.getKey()));
            Assert.assertEquals("Outer join query produced incorrect number of results", (int) q.getValue(), results.size());
        }
    }
}

