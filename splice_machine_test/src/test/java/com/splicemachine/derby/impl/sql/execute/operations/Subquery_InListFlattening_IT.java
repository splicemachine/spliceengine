package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;

@Ignore("fails, see DB-3628")
public class Subquery_InListFlattening_IT {

    private static final String SCHEMA = Subquery_InListFlattening_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table A(a1 int, a2 int)");
        classWatcher.executeUpdate("create table B(b1 int, b2 int)");
        classWatcher.executeUpdate("insert into A values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        classWatcher.executeUpdate("insert into B values(0,0),(0,0),(1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50)");
    }

    @Test
    public void simple() throws Exception {
        // subquery reads different table
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 in (select b1 from B where b2 > 20)", 0, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |"
        );
        // subquery reads same table
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 in (select a1 from A ai where ai.a2 > 20)", 0, "" +
                "A1 |A2 |\n" +
                "--------\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |"
        );
    }


}