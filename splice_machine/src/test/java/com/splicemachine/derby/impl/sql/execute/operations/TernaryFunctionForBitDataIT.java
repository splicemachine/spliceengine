package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLSyntaxErrorException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TernaryFunctionForBitDataIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(TernaryFunctionForBitDataIT.class);
    public static final String CLASS_NAME = TernaryFunctionForBitDataIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"true"});
        params.add(new Object[]{"false"});

        return params;
    }

    private String useSpark;

    public TernaryFunctionForBitDataIT(String useSpark) {
        this.useSpark = useSpark;
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(id int, a1 char(10), a2 varchar(10), a3 long varchar, a4 clob, " +
                        "a5 char(10) for bit data, a6 varchar(10) for bit data, a7 long varchar for bit data, a8 blob, " +
                        "b1 char(5), b2 varchar(5), b3 long varchar, b4 clob," +
                        "b5 char(5) for bit data, b6 varchar(5) for bit data, b7 long varchar for bit data, b8 blob)")
                .withInsert("insert into t1 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ")
                .withRows(rows(
                        row(1, "abac bc", "abac bc", "abac bc", "abac bc",
                                "abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes(),
                                "aaa", "aaa", "aaa", "aaa", "aaa".getBytes(), "aaa".getBytes(), "aaa".getBytes(), "aaa".getBytes()),
                        row(2, "abac bc", "abac bc", "abac bc", "abac bc", "abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes(),
                               "ab", "ab", "ab", "ab", "ab".getBytes(), "ab".getBytes(), "ab".getBytes(), "ab".getBytes()),
                        row(3, "abac bc", "abac bc", "abac bc", "abac bc",
                                "abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes(),
                                "bc", "bc", "bc", "bc", "bc".getBytes(), "bc".getBytes(), "bc".getBytes(), "bc".getBytes()),
                        row(4, "ab", "ab", "ab", "ab", "ab".getBytes(), "ab".getBytes(), "ab".getBytes(), "ab".getBytes(),
                                null, null, null, null, null, null, null, null),
                        row(5, "", "", "", "", "".getBytes(), "".getBytes(), "".getBytes(), "".getBytes(), null, null, null, null, null, null, null, null),
                        row(6, "ab", "ab", "ab", "ab", "ab".getBytes(), "ab".getBytes(), "ab".getBytes(), "ab".getBytes(),
                                "", "", "", "", "".getBytes(), "".getBytes(), "".getBytes(), "".getBytes()),
                        row(7, null, null, null, null, null, null, null, null,
                                "aa", "aa", "aa", "aa", "aa".getBytes(), "aa".getBytes(),"aa".getBytes(),"aa".getBytes())
                            ))
                .create();
        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testLocateSearchFromSearchForOfDifferentTypeCombination1() throws Exception {
        /* fixed type - fixed type */
        String[] col1set = {"a1", "a5"};
        String[] col2set = {"b1", "b5"};

        String expected = "ID |      2      |   3    |  4  |\n" +
                "---------------------------------\n" +
                " 1 |-abac bc   - |-aaa  - |  0  |\n" +
                " 2 |-abac bc   - |-ab   - |  0  |\n" +
                " 3 |-abac bc   - |-bc   - |  6  |\n" +
                " 4 |-ab        - | NULL   |NULL |\n" +
                " 5 |-          - | NULL   |NULL |\n" +
                " 6 |-ab        - |-     - |  3  |\n" +
                " 7 |    NULL     |-aa   - |NULL |";

        String expected2 = "ID |      2      |   3    | 4 |\n" +
                "-------------------------------\n" +
                " 3 |-abac bc   - |-bc   - | 6 |\n" +
                " 6 |-ab        - |-     - | 3 |";

        for (String col1: col1set) {
            for (String col2 : col2set) {
                /* Q1 */
                String sqlText = format("select id, '-' || a1 || '-', '-' || b1 || '-', locate(%1$s,%2$s) from t1 --splice-properties useSpark=%3$s\n order by id", col2, col1, useSpark);

                ResultSet rs = methodWatcher.executeQuery(sqlText);
                Assert.assertEquals("\nlocate(" + col2 + "," + col1 + "):" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();

                /* Q2 */
                sqlText = format("select id, '-' || a1 || '-', '-' || b1 || '-', locate(%1$s,%2$s) from t1 --splice-properties useSpark=%3$s\n where locate(%1$s,%2$s)>1 order by id", col2, col1, useSpark);

                rs = methodWatcher.executeQuery(sqlText);
                Assert.assertEquals("\nlocate(" + col2 + "," + col1 + "):" + sqlText + "\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();
            }
        }
    }


    @Test
    public void testLocateSearchFromSearchForOfDifferentTypeCombination2() throws Exception {
        /* fixed type - var type(no long varchar */
        String[] col1set = {"a1", "a5"};
        String[] col2set = {"b2", "b3", "b4", "b6", "b7", "b8"};

        String expected = "ID |      2      |  3   |  4  |\n" +
        "-------------------------------\n" +
                " 1 |-abac bc   - |-aaa- |  0  |\n" +
                " 2 |-abac bc   - |-ab-  |  1  |\n" +
                " 3 |-abac bc   - |-bc-  |  6  |\n" +
                " 4 |-ab        - |NULL  |NULL |\n" +
                " 5 |-          - |NULL  |NULL |\n" +
                " 6 |-ab        - | --   |  1  |\n" +
                " 7 |    NULL     |-aa-  |NULL |";

        String expected2 = "ID |      2      |  3  | 4 |\n" +
                "----------------------------\n" +
                " 3 |-abac bc   - |-bc- | 6 |";

        for (String col1: col1set) {
            for (String col2 : col2set) {
                try {
                    /* Q1 */
                    String sqlText = format("select id, '-' || a1 || '-', '-' || b2 || '-', locate(%1$s,%2$s) from t1 --splice-properties useSpark=%3$s\n order by id", col2, col1, useSpark);

                    ResultSet rs = methodWatcher.executeQuery(sqlText);
                    Assert.assertEquals("\nlocate(" + col2 + "," + col1 + "):" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                    rs.close();

                    /* Q2 */
                    sqlText = format("select id, '-' || a1 || '-', '-' || b2 || '-', locate(%1$s,%2$s) from t1 --splice-properties useSpark=%3$s\n where locate(%1$s,%2$s)>1 order by id", col2, col1, useSpark);

                    rs = methodWatcher.executeQuery(sqlText);
                    Assert.assertEquals("\nlocate(" + col2 + "," + col1 + "):" + sqlText + "\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
                    rs.close();
                } catch (SQLSyntaxErrorException e) {
                    // it is expected that long varchar and clob are not compared with bit data type
                    if ((col2.equals("b3") || col2.equals("b4")) && col1.equals("a5"))
                        Assert.assertEquals("locate(" + col2 + "," + col1 + "):" + e.getMessage(), SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, e.getSQLState());
                    else
                        Assert.fail("locate(" + col2 + "," + col1 + "):" + e.getMessage());
                }
            }
        }
    }

    @Test
    public void testLocateSearchFromSearchForOfDifferentTypeCombination3() throws Exception {
        /* var type - fix type */
        String[] col1set = {"a2", "a3", "a4", "a6", "a7", "a8"};
        String[] col2set = {"b1", "b5"};

        String expected = "ID |    2     |   3    |  4  |\n" +
                "------------------------------\n" +
                " 1 |-abac bc- |-aaa  - |  0  |\n" +
                " 2 |-abac bc- |-ab   - |  0  |\n" +
                " 3 |-abac bc- |-bc   - |  0  |\n" +
                " 4 |  -ab-    | NULL   |NULL |\n" +
                " 5 |   --     | NULL   |NULL |\n" +
                " 6 |  -ab-    |-     - |  0  |\n" +
                " 7 |  NULL    |-aa   - |NULL |";

        String expected2 = "";

        for (String col1: col1set) {
            for (String col2 : col2set) {
                try {
                    /* Q1 */
                    String sqlText = format("select id, '-' || a2 || '-', '-' || b1 || '-', locate(%1$s,%2$s) from t1 --splice-properties useSpark=%3$s\n order by id", col2, col1, useSpark);

                    ResultSet rs = methodWatcher.executeQuery(sqlText);
                    Assert.assertEquals("\nlocate(" + col2 + "," + col1 + "):" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                    rs.close();

                    /* Q2 */
                    sqlText = format("select id, '-' || a2 || '-', '-' || b1 || '-', locate(%1$s,%2$s) from t1 --splice-properties useSpark=%3$s\n where locate(%1$s,%2$s)>1 order by id", col2, col1, useSpark);

                    rs = methodWatcher.executeQuery(sqlText);
                    Assert.assertEquals("\nlocate(" + col2 + "," + col1 + "):" + sqlText + "\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
                    rs.close();
                } catch (SQLSyntaxErrorException e) {
                    // it is expected that long varchar and clob are not compared with bit data type
                    if ((col1.equals("a3") || col1.equals("a4")) && col2.equals("b5"))
                        Assert.assertEquals("locate(" + col2 + "," + col1 + "):" + e.getMessage(), SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, e.getSQLState());
                    else
                        Assert.fail("locate(" + col2 + "," + col1 + "):" + e.getMessage());
                }
            }
        }
    }

    @Test
    public void testLocateSearchFromSearchForOfDifferentTypeCombination4() throws Exception {
        /* var type - var type */
        String[] col1set = {"a2", "a3", "a4", "a6", "a7", "a8"};
        String[] col2set = {"b2", "b3", "b4", "b6", "b7", "b8"};

        String expected = "ID |    2     |  3   |  4  |\n" +
                "----------------------------\n" +
                " 1 |-abac bc- |-aaa- |  0  |\n" +
                " 2 |-abac bc- |-ab-  |  1  |\n" +
                " 3 |-abac bc- |-bc-  |  6  |\n" +
                " 4 |  -ab-    |NULL  |NULL |\n" +
                " 5 |   --     |NULL  |NULL |\n" +
                " 6 |  -ab-    | --   |  1  |\n" +
                " 7 |  NULL    |-aa-  |NULL |";

        String expected2 = "ID |    2     |  3  | 4 |\n" +
                "-------------------------\n" +
                " 3 |-abac bc- |-bc- | 6 |";

        for (String col1: col1set) {
            for (String col2 : col2set) {
                try {
                    /* Q1 */
                    String sqlText = format("select id, '-' || a2 || '-', '-' || b2 || '-', locate(%1$s,%2$s) from t1 --splice-properties useSpark=%3$s\n order by id", col2, col1, useSpark);

                    ResultSet rs = methodWatcher.executeQuery(sqlText);
                    Assert.assertEquals("\nlocate(" + col2 + "," + col1 + "):" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                    rs.close();

                    /* Q2 */
                    sqlText = format("select id, '-' || a2 || '-', '-' || b2 || '-', locate(%1$s,%2$s) from t1 --splice-properties useSpark=%3$s\n where locate(%1$s,%2$s)>1 order by id", col2, col1, useSpark);

                    rs = methodWatcher.executeQuery(sqlText);
                    Assert.assertEquals("\nlocate(" + col2 + "," + col1 + "):" + sqlText + "\n", expected2, TestUtils.FormattedResult.ResultFactory.toString(rs));
                    rs.close();
                } catch (SQLSyntaxErrorException e) {
                    // it is expected that long varchar and clob are not compared with bit data type
                    if ((col1.equals("a3") || col1.equals("a4")) && (col2.equals("b6") || col2.equals("b7") || col2.equals("b8")) ||
                        (col2.equals("b3") || col2.equals("b4")) && (col1.equals("a6") || col1.equals("a7") || col1.equals("a8")))
                        Assert.assertEquals("locate(" + col2 + "," + col1 + "):" + e.getMessage(), SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, e.getSQLState());
                    else
                        Assert.fail("locate(" + col2 + "," + col1 + "):" + e.getMessage());
                }
            }
        }
    }

    @Test
    public void testLocateParameterizedQuery1() throws Exception {
        ResultSet rs = null;
        String sqlText = format("select id, '-' || a2 || '-', locate(?, a5) from t1 --splice-properties useSpark=%s\n order by id", useSpark);
        try {
            PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
            ps.setBytes(1, "ac".getBytes());
            rs = ps.executeQuery();

            String expected = "ID |    2     |  3  |\n" +
                    "---------------------\n" +
                    " 1 |-abac bc- |  3  |\n" +
                    " 2 |-abac bc- |  3  |\n" +
                    " 3 |-abac bc- |  3  |\n" +
                    " 4 |  -ab-    |  0  |\n" +
                    " 5 |   --     |  0  |\n" +
                    " 6 |  -ab-    |  0  |\n" +
                    " 7 |  NULL    |NULL |";

            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        } finally {
            if (rs != null)
                rs.close();
        }
    }

    @Test
    public void testLocateParameterizedQuery2() throws Exception {
        /* similar to testLocateParameterizedQuery1, but pass in the parameter as a string instead of bytes */
        ResultSet rs = null;
        String sqlText = format("select id, '-' || a2 || '-', locate(?, a5) from t1 --splice-properties useSpark=%s\n order by id", useSpark);
        try {
            PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
            ps.setObject(1, "ac");
            rs = ps.executeQuery();

            String expected = "ID |    2     |  3  |\n" +
                    "---------------------\n" +
                    " 1 |-abac bc- |  3  |\n" +
                    " 2 |-abac bc- |  3  |\n" +
                    " 3 |-abac bc- |  3  |\n" +
                    " 4 |  -ab-    |  0  |\n" +
                    " 5 |   --     |  0  |\n" +
                    " 6 |  -ab-    |  0  |\n" +
                    " 7 |  NULL    |NULL |";

            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        } finally {
            if (rs != null)
                rs.close();
        }
    }

    @Test
    public void testLocateParameterizedQuery3() throws Exception {
        ResultSet rs = null;
        String sqlText = format("select id, '-' || b2 || '-', locate(b6, ?) from t1 --splice-properties useSpark=%s\n order by id", useSpark);
        try {
            PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
            ps.setBytes(1, "abc".getBytes());
            rs = ps.executeQuery();

            String expected = "ID |  2   |  3  |\n" +
                    "-----------------\n" +
                    " 1 |-aaa- |  0  |\n" +
                    " 2 |-ab-  |  1  |\n" +
                    " 3 |-bc-  |  2  |\n" +
                    " 4 |NULL  |NULL |\n" +
                    " 5 |NULL  |NULL |\n" +
                    " 6 | --   |  1  |\n" +
                    " 7 |-aa-  |  0  |";

            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        } finally {
            if (rs != null)
                rs.close();
        }
    }

    @Test
    public void testLocateParameterizedQuery4() throws Exception {
        ResultSet rs = null;
        String sqlText = format("select id, '-' || b2 || '-', locate(?, ?) from t1 --splice-properties useSpark=%s\n order by id", useSpark);
        try {
            PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
            ps.setBytes(1, "abc".getBytes());
            ps.setBytes(2, "cbaabc".getBytes());
            rs = ps.executeQuery();

            String expected = "ID |  2   | 3 |\n" +
                    "---------------\n" +
                    " 1 |-aaa- | 4 |\n" +
                    " 2 |-ab-  | 4 |\n" +
                    " 3 |-bc-  | 4 |\n" +
                    " 4 |NULL  | 4 |\n" +
                    " 5 |NULL  | 4 |\n" +
                    " 6 | --   | 4 |\n" +
                    " 7 |-aa-  | 4 |";

            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        } finally {
            if (rs != null)
                rs.close();
        }
    }
}