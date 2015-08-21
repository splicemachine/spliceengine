package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.Test;
import java.sql.Connection;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 8/19/15.
 */
public class ScanSelectivityIT extends SpliceUnitTest {

    public static final String CLASS_NAME = ScanSelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table ts_bool (i int, b boolean)")
                .withInsert("insert into ts_bool values(?, ?)")
                .withRows(rows(
                        row(1, false),
                        row(2, false),
                        row(3, false),
                        row(4, true),
                        row(5, true),
                        row(6, null),
                        row(7, null),
                        row(8, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_int (s smallint, i int, l bigint)")
                .withInsert("insert into ts_int values(?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(null, null, null),
                        row(null, null, null),
                        row(null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_float (f float, d double, n numeric(10, 1), r real, c decimal(4, 3))")
                .withInsert("insert into ts_float values(?, ?, ?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1, 1, 1),
                        row(2, 2, 2, 2, 2),
                        row(3, 3, 3, 3, 3),
                        row(4, 4, 4, 4, 4),
                        row(5, 5, 5, 5, 5),
                        row(null, null, null, null, null),
                        row(null, null, null, null, null),
                        row(null, null, null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_char (c char(10), v varchar(20), l long varchar, b clob)")
                .withInsert("insert into ts_char values(?,?,?,?)")
                .withRows(rows(
                        row("a", "aaaa", "aaaa", "aaaa"),
                        row("b", "bbbbb", "bbbbb", "bbbbb"),
                        row("c", "cc", "cc", "cc"),
                        row("d", "ddddd", "ddddd", "ddddd"),
                        row("e", "eee", "eee", "eee"),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_datetime(d date, t time, ts timestamp)")
                .withInsert("insert into ts_datetime values (?, ?, ?)")
                .withRows(rows(
                        row("1994-02-23", "15:09:02", "1962-09-23 03:23:34.234"),
                        row("1995-02-23", "16:09:02", "1962-09-24 03:23:34.234"),
                        row("1996-02-23", "17:09:02", "1962-09-25 03:23:34.234"),
                        row("1997-02-23", "18:09:02", "1962-09-26 03:23:34.234"),
                        row("1998-02-23", "19:09:02", "1962-09-27 03:23:34.234"),
                        row(null, null, null),
                        row(null, null, null),
                        row(null, null, null)))
                .create();

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                schemaName));

        conn.commit();

        new TableCreator(conn)
                .withCreate("create table tns_bool (i int, b boolean)")
                .withInsert("insert into tns_bool values(?, ?)")
                .withRows(rows(
                        row(1, false),
                        row(2, false),
                        row(3, false),
                        row(4, true),
                        row(5, true),
                        row(6, null),
                        row(7, null),
                        row(8, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tns_int (s smallint, i int, l bigint)")
                .withInsert("insert into tns_int values(?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(null, null, null),
                        row(null, null, null),
                        row(null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tns_float (f float, d double, n numeric(10, 1), r real, c decimal(4, 3))")
                .withInsert("insert into tns_float values(?, ?, ?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1, 1, 1),
                        row(2, 2, 2, 2, 2),
                        row(3, 3, 3, 3, 3),
                        row(4, 4, 4, 4, 4),
                        row(5, 5, 5, 5, 5),
                        row(null, null, null, null, null),
                        row(null, null, null, null, null),
                        row(null, null, null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tns_char (c char(10), v varchar(20), l long varchar, b clob)")
                .withInsert("insert into tns_char values(?,?,?,?)")
                .withRows(rows(
                        row("a", "aaaa", "aaaa", "aaaa"),
                        row("b", "bbbbb", "bbbbb", "bbbbb"),
                        row("c", "cc", "cc", "cc"),
                        row("d", "ddddd", "ddddd", "ddddd"),
                        row("e", "eee", "eee", "eee"),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tns_datetime(d date, t time, ts timestamp)")
                .withInsert("insert into tns_datetime values (?, ?, ?)")
                .withRows(rows(
                        row("1994-02-23", "15:09:02", "1962-09-23 03:23:34.234"),
                        row("1995-02-23", "16:09:02", "1962-09-24 03:23:34.234"),
                        row("1996-02-23", "17:09:02", "1962-09-25 03:23:34.234"),
                        row("1997-02-23", "18:09:02", "1962-09-26 03:23:34.234"),
                        row("1998-02-23", "19:09:02", "1962-09-27 03:23:34.234"),
                        row(null, null, null),
                        row(null, null, null),
                        row(null, null, null)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testBoolSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_bool where b=true","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b=false","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b<>true","outputRows=6",methodWatcher);
        firstRowContainsQuery("explain select * from ts_bool where b<>false","outputRows=5",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_bool where b=true","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b=false","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b is not null", "outputRows=18", methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b<>true","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_bool where b<>false","outputRows=2",methodWatcher);
    }

    @Test
    public void testSmallIntSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_int where s=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<2","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<=2","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s>1","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s>1 and s<5","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s is not null and s>1","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where s<>1","outputRows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_int where s=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<=2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s>1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s>1 and s<5","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s is not null and s>1","outputRows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where s<>1","outputRows=2",methodWatcher);
    }

    @Test
    public void testIntSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_int where i=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<2","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<=2","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i>1","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i>1 and i<5","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i is not null and i>1","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i>3 or i<2","outputRows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where i<>2","outputRows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_int where i=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<=2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i>1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i>1 and i<5","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i is not null and i>1","outputRows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i>3 or i<2","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where i<>2","outputRows=2",methodWatcher);
    }

    @Test
    public void testLongIntSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_int where l=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<2","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<=2","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l>1","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l>1 and l<5","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l is not null and l>1","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_int where l<>1","outputRows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_int where l=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<=2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l>1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l>1 and l<5","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l is not null and l>1","outputRows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_int where l<>1","outputRows=2",methodWatcher);
    }

    @Test
    public void testFloatSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where f=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<2","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<=2","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f>1","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f>1 and f<5","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f is not null and f>1","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where f<>1","outputRows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_float where f=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<=2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f>1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f>1 and f<5","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f is not null and f>1","outputRows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where f<>1","outputRows=2",methodWatcher);
    }

    @Test
    public void testDoubleSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where d=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<=1","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<2","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<=2","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d>1","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d>1 and d<5","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d is not null and d>1","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where d<>1","outputRows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_float where d=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<=1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<=2","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d>1","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d>1 and d<5","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d is not null and d>1","outputRows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where d<>1","outputRows=2",methodWatcher);
    }

    @Test
    public void testNumericSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where n=1.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<1.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<=1.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<2.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<=2.0","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n>1.0","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n>1.0 and n<5.0","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n is not null and n>1.0","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where n<>1.0","outputRows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_float where n=1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<=1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<2.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<=2.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n>1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n>1.0 and n<5.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n is not null and n>1.0","outputRows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where n<>1.0","outputRows=2",methodWatcher);
    }

    @Test
    public void testRealSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where r=1.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<1.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<=1.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<2.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<=2.0","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r>1.0","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r>1.0 and r<5.0","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r is not null and r>1.0","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where r<>1.0","outputRows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_float where r=1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<=1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<2.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<=2.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r>1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r>1.0 and r<5.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r is not null and r>1.0","outputRows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where r<>1.0","outputRows=2",methodWatcher);
    }

    @Test
    public void testDecimalSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_float where c=1.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<1.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<=1.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<2.0","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<=2.0","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c>1.0","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c>1.0 and c<5.0","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c is not null and c>1.0","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_float where c<>1.0","outputRows=7",methodWatcher);

        // no statistics
        firstRowContainsQuery("explain select * from tns_float where c=1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<=1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<2.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<=2.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c>1.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c>1.0 and c<5.0","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c is not null and c>1.0","outputRows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_float where c<>1.0","outputRows=2",methodWatcher);
    }

    @Test
    public void testCharSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_char where c='a'","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c>'a'","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c>='a'","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c<'e'","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c<='e'","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c>='a' and c<='e'","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c is null", "outputRows=3", methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c is not null", "outputRows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c>'a' and c is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c<'e' and c is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where c<>'a'","outputRows=7",methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_char where c='a'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>'a'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>='a'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<'e'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<='e'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>='a' and c<='e'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>'a' and c is not null", "outputRows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<'e' and c is not null", "outputRows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<>'a'","outputRows=2",methodWatcher);
    }

    @Test
    public void testVarcharSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_char where v='aaaa'","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'aaaa'","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='aaaa'","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'eee'","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<='eee'","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>='aaaa' and v<='eee'","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v>'aaaa' and c is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<'eee' and c is not null","outputRows=5",methodWatcher);
        secondRowContainsQuery("explain select * from ts_char where v like '%aa%'", "outputRows=4", methodWatcher);
        secondRowContainsQuery("explain select * from ts_char where v not like '%aa%'", "outputRows=1", methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where v<>'aaaa'", "outputRows=7", methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_char where c='a'", "outputRows=18", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>'a'", "outputRows=18", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>='a'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<'e'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<='e'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>='a' and c<='e'","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c>'a' and c is not null","outputRows=17",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where c<'e' and c is not null","outputRows=17",methodWatcher);
        secondRowContainsQuery("explain select * from tns_char where v like '%aa%'", "outputRows=10", methodWatcher);
        secondRowContainsQuery("explain select * from tns_char where v not like '%aa%'", "outputRows=2", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where v<>'aaaa'", "outputRows=2", methodWatcher);
    }

    @Test
    public void testLongVarcharSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_char where l like '%a%'","outputRows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where l not like '%a%'","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where l is null", "outputRows=3", methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where l is not null", "outputRows=5", methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_char where l like '%a%'","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where l not like '%a%'","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where l is null", "outputRows=2", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where l is not null", "outputRows=18", methodWatcher);
    }

    @Test
    public void testClobSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_char where b like '%a%'","outputRows=4",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where b not like '%a%'","outputRows=1",methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where b is null", "outputRows=1", methodWatcher);
        firstRowContainsQuery("explain select * from ts_char where b is not null", "outputRows=7", methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_char where b like '%a%'","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where b not like '%a%'","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where b is null", "outputRows=2", methodWatcher);
        firstRowContainsQuery("explain select * from tns_char where b is not null", "outputRows=18", methodWatcher);
    }

    @Test
    public void testDateSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_datetime where d>date('1994-02-23')","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d>=date('1994-02-23')","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d<date('1998-02-23')", "outputRows=7", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d<=date('1998-02-23')", "outputRows=8", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d>date('1994-02-23') and d is not null", "outputRows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d<date('1998-02-23') and d is not null", "outputRows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where d<>date('1994-02-23')","outputRows=7",methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_datetime where d>date('1994-02-23')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d>=date('1994-02-23')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d<date('1998-02-23')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d<=date('1998-02-23')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d>date('1994-02-23') and d is not null", "outputRows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d<date('1998-02-23') and d is not null", "outputRows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where d<>date('1994-02-23')","outputRows=2",methodWatcher);
    }

    @Test
    public void testTimeSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_datetime where t>time('15:09:02')","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t>=time('15:09:02')","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t<time('19:09:02')","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t<=time('19:09:02')","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t>time('15:09:02') and t is not null", "outputRows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t<time('19:09:02') and t is not null", "outputRows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where t<>time('15:09:02')","outputRows=7",methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_datetime where t>time('15:09:02')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t>=time('15:09:02')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t<time('19:09:02')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t<=time('19:09:02')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t>time('15:09:02') and t is not null", "outputRows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t<time('19:09:02') and t is not null", "outputRows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where t<>time('15:09:02')","outputRows=2",methodWatcher);
    }


    @Test
    public void testTimestampSelectivity() throws Exception {
        firstRowContainsQuery("explain select * from ts_datetime where ts>timestamp('1962-09-23 03:23:34.234')","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts>=timestamp('1962-09-23 03:23:34.234')","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts<timestamp('1962-09-27 03:23:34.234')","outputRows=7",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts<=timestamp('1962-09-27 03:23:34.234')","outputRows=8",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts is null","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts is not null","outputRows=5",methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts>=timestamp('1962-09-23 03:23:34.234') and ts is not null", "outputRows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts<timestamp('1962-09-27 03:23:34.234') and ts is not null", "outputRows=5", methodWatcher);
        firstRowContainsQuery("explain select * from ts_datetime where ts<>timestamp('1962-09-23 03:23:34.234')","outputRows=7",methodWatcher);

        // No statistics
        firstRowContainsQuery("explain select * from tns_datetime where ts>timestamp('1962-09-23 03:23:34.234')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts>=timestamp('1962-09-23 03:23:34.234')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts<timestamp('1962-09-27 03:23:34.234')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts<=timestamp('1962-09-27 03:23:34.234')","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts is null","outputRows=2",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts is not null","outputRows=18",methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts>=timestamp('1962-09-23 03:23:34.234') and ts is not null", "outputRows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts<timestamp('1962-09-27 03:23:34.234') and ts is not null", "outputRows=17", methodWatcher);
        firstRowContainsQuery("explain select * from tns_datetime where ts<>timestamp('1962-09-23 03:23:34.234')","outputRows=2",methodWatcher);
    }
}
