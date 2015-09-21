package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;

public class SubqueryExistsFlatteningIT {

    private static final String SCHEMA = SubqueryExistsFlatteningIT.class.getSimpleName();

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
        classWatcher.executeUpdate("create table C(name varchar(20), surname varchar(20))");
        classWatcher.executeUpdate("insert into A values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        classWatcher.executeUpdate("insert into C values('Jon', 'Snow'),('Eddard', 'Stark'),('Robb', 'Stark'), ('Jon', 'Arryn')");
    }

    @Test
    @Ignore
    public void testBetweenFlattenAndNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 between 1 and 10 and exists (select a1 from A ai where ai.a2 > 20)", ZERO_SUBQUERY_NODES_IN_PLAN, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    @Ignore
    public void testBetweenNotFlattenOrNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 between 1 and 4 or exists (select a1 from A ai where ai.a2 > 20)", 1, "" +
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
    @Ignore
    public void testLikeFlattenAndNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C where surname like 'S%' and exists (select surname from C where name = 'Jon')", ZERO_SUBQUERY_NODES_IN_PLAN, "" +
                        "NAME  | SURNAME |\n" +
                        "------------------\n" +
                        "Eddard |  Stark  |\n" +
                        "  Jon  |  Snow   |\n" +
                        " Robb  |  Stark  |"
        );
    }

    @Test
    @Ignore
    public void testLikeNotFlattenOrNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C where surname like 'A%' or exists (select surname from C where name = 'Robb')", 1, "" +
                        "NAME  | SURNAME |\n" +
                        "------------------\n" +
                        "Eddard |  Stark  |\n" +
                        "  Jon  |  Arryn  |\n" +
                        "  Jon  |  Snow   |\n" +
                        " Robb  |  Stark  |"
        );
    }

    @Test
    @Ignore
    public void testSimplePredicateFlattenAndNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 > 0 and a1 <= 10 and exists (select a1 from A ai where ai.a2 > 20)", ZERO_SUBQUERY_NODES_IN_PLAN, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    @Ignore
    public void testSimplePredicateNotFlattenOrNode() throws Exception {
        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 >= 1 and a1 <= 4 or exists (select a1 from A ai where ai.a2 > 20)", 1, "" +
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
}
