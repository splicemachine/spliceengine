package org.apache.derby.impl.sql.execute.operations.joins;

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


public class OuterJoinTest extends SpliceUnitTest {

    private static Logger LOG = Logger.getLogger(OuterJoinTest.class);
    private static final Map<String, String> tableMap = Maps.newHashMap();

    public static final String CLASS_NAME = OuterJoinTest.class.getSimpleName().toUpperCase() + "_2";
    public static final String TABLE_NAME_1 = "A";
    public static final String TABLE_NAME_2 = "CC";
    public static final String TABLE_NAME_3 = "DD";
    public static final String TABLE_NAME_4 = "D";
    public static final String TABLE_NAME_5 = "E";
    public static final String TABLE_NAME_6 = "F";
    public static final String TABLE_NAME_7 = "G";


    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1, CLASS_NAME, "(si varchar(40),sa character varying(40),sc varchar(40),sd int,se float)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2, CLASS_NAME, "(si varchar(40), sa varchar(40))");
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3, CLASS_NAME, "(si varchar(40), sa varchar(40))");
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4, CLASS_NAME, "(a varchar(20), b varchar(20), c varchar(10), d decimal, e varchar(15))");
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5, CLASS_NAME, "(a varchar(20), b varchar(20), w decimal(4),e varchar(15))");
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6, CLASS_NAME, "(a varchar(20), b varchar(20), c varchar(10), d decimal, e varchar(15))");
    protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_NAME_7, CLASS_NAME, "(a varchar(20), b varchar(20), w decimal(4),e varchar(15))");

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
        ResultSet rs = methodWatcher.executeQuery("select t1.EMPNAME, t1.CITY, t2.PTYPE from STAFF t1 left outer join PROJ t2 --DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n" +
                " on t1.CITY = t2.CITY");

        List<Map> results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(11, results.size());

    }

    @Test
    public void testScrollableVarcharLeftOuterJoinWithJoinStrategy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc left outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
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
        ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc left outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
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
    @Ignore("Bug 325")
    public void testScrollableVarcharRightOuterJoinWithJoinStrategy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc right outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
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
    @Ignore("Bug 325")
    public void testSinkableVarcharRightOuterJoinWithJoinStrategy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc right outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
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

}

