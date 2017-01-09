package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 1/10/17.
 */
public class RemoveColumnIT extends SpliceUnitTest {

    public static final String CLASS_NAME = RemoveColumnIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table a (a1 int, a2 varchar(10))")
                .withInsert("insert into a values(?,?)")
                .withRows(rows(
                        row(1, "a1"),
                        row(2, "a2"),
                        row(3, "a3"),
                        row(4, "a4")))
                .create();

        new TableCreator(conn)
                .withCreate("create table b (b1 int, b2 varchar(10), b3 date, b4 decimal, b5 timestamp, b6 double)")
                .withInsert("insert into b values(?,?,?,?,?,?)")
                .withRows(rows(
                        row(1, "b1", "1960-01-01", 11.0, "1960-01-01 23:03:20", 110.0),
                        row(2, "b2", "1960-01-02", 12.0, "1960-01-02 23:03:20", 120.0),
                        row(3, "b3", "1960-01-03", 13.0, "1960-01-03 23:03:20", 130.0),
                        row(4, "b4", "1960-01-04", 14.0, "1960-01-04 23:03:20", 140.0)))
                .create();

        new TableCreator(conn)
                .withCreate("create table c (c1 int, c2 varchar(10), c3 date, c4 decimal, c5 timestamp, c6 double)")
                .withInsert("insert into c values(?,?,?,?,?,?)")
                .withRows(rows(
                        row(1, "c1", "1990-01-01", 21.0, "1990-01-01 23:03:20", 210.0),
                        row(2, "c2", "1990-01-02", 22.0, "1990-01-02 23:03:20", 220.0),
                        row(3, "c3", "1990-01-03", 23.0, "1990-01-03 23:03:20", 230.0),
                        row(4, "c4", "1990-01-04", 24.0, "1990-01-04 23:03:20", 240.0)))
                .create();

        methodWatcher.execute("create view v " +
                "as select b1, b2, b3, b4, b5, b6, c1, c2, c3, c4, c5, c6 " +
                "from b, c where b.b1 = c.c1");

    }


    @Test
    public void testQuery() throws Exception {

        String query = "select b6, c6, b2, c2, b3, c3, b4, c4, b5, c5 from a, v where a.a1 = v.b1 order by 1";
        ResultSet rs =methodWatcher.executeQuery(query);
        String actual = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        String expected =
                "B6   | C6   |B2 |C2 |    B3     |    C3     |B4 |C4 |         B5           |         C5           |\n" +
                "----------------------------------------------------------------------------------------------------\n" +
                "110.0 |210.0 |b1 |c1 |1960-01-01 |1990-01-01 |11 |21 |1960-01-01 23:03:20.0 |1990-01-01 23:03:20.0 |\n" +
                "120.0 |220.0 |b2 |c2 |1960-01-02 |1990-01-02 |12 |22 |1960-01-02 23:03:20.0 |1990-01-02 23:03:20.0 |\n" +
                "130.0 |230.0 |b3 |c3 |1960-01-03 |1990-01-03 |13 |23 |1960-01-03 23:03:20.0 |1990-01-03 23:03:20.0 |\n" +
                "140.0 |240.0 |b4 |c4 |1960-01-04 |1990-01-04 |14 |24 |1960-01-04 23:03:20.0 |1990-01-04 23:03:20.0 |";
        Assert.assertEquals("\n"+query+"\n", expected, actual);
    }

    @Test
    public void testHeapSize() throws Exception {
        // The column that are actually read is a1, b1, c1, so first 3 query use the same heap size
        double s1 = getHeapSize("explain select a1 from a, v where a.a1 = v.b1");
        Assert.assertTrue("s1 = " + s1, s1>0);

        double s2 = getHeapSize("explain select a1,b1 from a, v where a.a1 = v.b1");
        Assert.assertTrue("s2 = " + s2, s2>0);

        double s3 = getHeapSize("explain select a1,b1,c1 from a, v where a.a1 = v.b1");
        Assert.assertTrue("s3 = " + s3, s3>0);

        // This query read column a1,b1,c1, and a2, so it should consume more heap size
        double s4 = getHeapSize("explain select a1,a2 from a, v where a.a1 = v.b1");
        Assert.assertTrue("s4 = " + s4, s4>0);

        Assert.assertTrue(s1==s2 && s2==s3);
        Assert.assertTrue(s3 < s4);
    }

    private double getHeapSize(String query) throws Exception{
        ResultSet rs = methodWatcher.executeQuery(query);
        rs.next();
        rs.next();
        String text = rs.getString(1);
        String[] kvs = text.split(",");
        for (String kv : kvs) {
            if (kv.trim().startsWith("outputHeapSize")) {
                String[] hs = kv.split("=");
                String[] size = hs[1].split(" ");

                if (size[1].trim().startsWith("KB")){
                    return new Double(size[0])*1000;
                }
                else if (size[1].trim().startsWith("MB")){
                    return new Double(size[0])*1000000;
                }

                return new Double(size[0]);
            }
        }
        return -1;
    }
}
