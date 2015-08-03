package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 *
 *
 */
public class JoinSelectivityIT extends SpliceUnitTest {
    public static final String CLASS_NAME = JoinSelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table ts_10_spk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into ts_10_spk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(6, "6", "1995-01-01 23:03:20", false),
                        row(7, "7", "1995-01-01 23:03:20", false),
                        row(8, "8", "1995-01-01 23:03:20", false),
                        row(9, "9", "1995-01-01 23:03:20", false),
                        row(10, "10", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_10_mpk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into ts_10_mpk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(6, "6", "1995-01-01 23:03:20", false),
                        row(7, "7", "1995-01-01 23:03:20", false),
                        row(8, "8", "1995-01-01 23:03:20", false),
                        row(9, "9", "1995-01-01 23:03:20", false),
                        row(10, "10", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_10_npk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into ts_10_npk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(6, "6", "1995-01-01 23:03:20", false),
                        row(7, "7", "1995-01-01 23:03:20", false),
                        row(8, "8", "1995-01-01 23:03:20", false),
                        row(9, "9", "1995-01-01 23:03:20", false),
                        row(10, "10", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_5_spk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into ts_5_spk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_5_mpk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into ts_5_mpk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false) )).create();

        new TableCreator(conn)
                .withCreate("create table ts_5_npk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into ts_5_npk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false) )).create();


        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                spliceSchemaWatcher));
        conn.commit();

    }

    @Test
    public void leftInnerJoinNoRelationships() throws Exception {
        firstRowContainsQuery("explain select * from ts_10_npk, ts_5_npk where ts_10_npk.c1 = ts_5_npk.c1","outputRows=10",methodWatcher);
    }

    @Test
    public void leftOuterJoinNoRelationships() throws Exception {
        firstRowContainsQuery("explain select * from ts_10_npk left outer join ts_5_npk on ts_10_npk.c1 = ts_5_npk.c1","outputRows=10",methodWatcher);
    }

    @Test
    public void leftAntiJoinNoRelationships() throws Exception {
        firstRowContainsQuery("explain select * from ts_10_npk where not exists (select * from ts_5_npk where ts_10_npk.c1 = ts_5_npk.c1)","outputRows=10",methodWatcher);
    }



}