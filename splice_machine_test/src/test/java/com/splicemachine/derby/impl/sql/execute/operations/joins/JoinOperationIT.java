package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.sql.ResultSet;
import java.util.Collection;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

@RunWith(Parameterized.class)
public class JoinOperationIT extends SpliceUnitTest{
    private static final String SCHEMA = JoinOperationIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{"NESTEDLOOP"});
        params.add(new Object[]{"SORTMERGE"});
        params.add(new Object[]{"BROADCAST"});
//        params.add(new Object[]{"MERGE"});
        return params;
    }
    private String joinStrategy;

    public JoinOperationIT(String joinStrategy) {
        this.joinStrategy = joinStrategy;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table FOO (col1 int primary key, col2 int)")
                .withInsert("insert into FOO values(?,?)")
                .withRows(rows(row(1,1), row(2,1), row(3,1), row(4,1), row(5,1))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table FOO2 (col1 int primary key, col2 int)")
                .withInsert("insert into FOO2 values(?,?)")
                .withRows(rows(row(1,5), row(3,7), row(5,9))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table a (v varchar(12))")
                .withInsert("insert into a values(?)")
                .withRows(rows(row("1"), row("2"), row("3 "), row("4 "))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table b (v varchar(12))")
                .withInsert("insert into b values(?)")
                .withRows(rows(row("1"), row("2 "), row("3"), row("4 "))).create();


    }
    @Test
    public void testInnerJoinNoRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*), max(foo.col1) from --Splice-properties joinOrder=FIXED\n " +
                        "foo, foo2 --Splice-properties joinStrategy=%s\n" +
                "where foo.col1 = foo2.col1",this.joinStrategy
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 3, rs.getInt(1));
        Assert.assertEquals(String.format("Wrong max for %s", joinStrategy), 5, rs.getInt(2));
    }
    @Test
    public void testInnerJoinRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*), min(foo.col1) from --Splice-properties joinOrder=FIXED\n" +
                        " foo inner join foo2 --Splice-properties joinStrategy=%s\n" +
                        "on foo.col1 = foo2.col1 and foo.col1+foo2.col2>6",this.joinStrategy
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy),2,rs.getInt(1));
        Assert.assertEquals(String.format("Wrong min for %s", joinStrategy),3,rs.getInt(2));
    }
    @Test
    @Ignore("DB-4272 no valid plan execution for sortmerge, bcast and merge")
    public void testInnerAntiJoinNoRestriction() throws Exception {
        System.out.println(joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*) from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                        " foo where not exists (select * from foo2 --SPLICE-PROPERTIES joinStrategy=%s\n" +
                        "where foo.col1 = foo2.col1)",this.joinStrategy
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy),2,rs.getInt(1));
    }
    @Test
    public void testInnerAntiJoinRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*) from --Splice-properties joinOrder=FIXED\n" +
                        " foo where not exists (select * from foo2 --Splice-properties joinStrategy=%s\n" +
                        "where foo.col1 = foo2.col1 and foo.col1+foo2.col2>6)",this.joinStrategy
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy),3,rs.getInt(1));
    }
    @Test
    public void testOuterJoinNoRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*), sum(CASE WHEN foo2.col1 is null THEN 0 ELSE 1 END) from --Splice-properties joinOrder=FIXED\n" +
                        " foo left outer join foo2 --Splice-properties joinStrategy=%s\n" +
                        "on foo.col1 = foo2.col1",this.joinStrategy
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 5, rs.getInt(1));
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy),3,rs.getInt(2));
    }
    @Test
    public void testOuterJoinRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*), sum(CASE WHEN foo2.col2 is null THEN 0 ELSE 1 END) from --Splice-properties joinOrder=FIXED\n" +
                        " foo left outer join foo2 --Splice-properties joinStrategy=%s\n" +
                        "on foo.col1 = foo2.col1 and foo.col1+foo2.col2>6",this.joinStrategy
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy),5,rs.getInt(1));
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy),2,rs.getInt(2));
    }
    @Test
    public void testTrimJoinFunctionCall() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select * from --Splice-properties joinOrder=FIXED\n" +
                        " a, b --Splice-properties joinStrategy=%s\n" +
                        "where rtrim(a.v) = b.v order by a.v",this.joinStrategy
        ));
        Assert.assertTrue("First Row Not Returned",rs.next());
        Assert.assertEquals("1",rs.getString(1));
        Assert.assertEquals("1",rs.getString(2));
        Assert.assertTrue("Second Row Not Returned",rs.next());
        Assert.assertEquals("3 ",rs.getString(1));
        Assert.assertEquals("3",rs.getString(2));
        Assert.assertFalse("Third Row Returned",rs.next());
    }

}
