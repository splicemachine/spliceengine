package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class Subquery_Flattening_InList_IT {

    private static final String SCHEMA = Subquery_Flattening_InList_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private static final int ZERO_SUBQUERY_NODES_IN_PLAN = 0;

    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table A(a1 int, a2 int)");
        classWatcher.executeUpdate("create table B(b1 int, b2 int)");
        classWatcher.executeUpdate("create table C(name varchar(20), surname varchar(20))");
        classWatcher.executeUpdate("insert into A values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        classWatcher.executeUpdate("insert into B values(0,0),(0,0),(1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50)");
        classWatcher.executeUpdate("insert into C values('Jon', 'Snow'),('Eddard', 'Stark'),('Robb', 'Stark'), ('Jon', 'Arryn')");
    }

    @Test
    public void simple() throws Exception {
        // subquery reads different table
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 in (select b1 from B where b2 > 20)", ZERO_SUBQUERY_NODES_IN_PLAN, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
        // subquery reads same table
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 in (select a1 from A ai where ai.a2 > 20)", ZERO_SUBQUERY_NODES_IN_PLAN, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testBetweenFlattenAndNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 between 0 and 10 and a1 in (select a1 from A ai where ai.a2 > 20)", ZERO_SUBQUERY_NODES_IN_PLAN, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testBetweenNotFlattenOrNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 between 0 and 4 or a1 in (select a1 from A ai where ai.a2 > 20)", 1, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testLikeFlattenAndNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C where surname like 'S%' and surname in (select surname from C where name = 'Jon')", ZERO_SUBQUERY_NODES_IN_PLAN, "" +
                        "NAME | SURNAME |\n" +
                        "----------------\n" +
                        " Jon |  Snow   |"
        );
    }

    @Test
    public void testLikeNotFlattenOrNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C where surname like 'A%' or surname in (select surname from C where name = 'Robb')", 1, "" +
                        "NAME  | SURNAME |\n" +
                        "------------------\n" +
                        "Eddard |  Stark  |\n" +
                        "  Jon  |  Arryn  |\n" +
                        " Robb  |  Stark  |"
        );
    }


}