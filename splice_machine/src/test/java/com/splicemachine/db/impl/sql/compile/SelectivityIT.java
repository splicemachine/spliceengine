/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.*;
import java.util.Properties;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 * 
 *
 */
public class SelectivityIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(SelectivityIT.class);
    public static final String CLASS_NAME = SelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private static String BADDIR;

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table ts_low_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into ts_low_cardinality values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .create();
        for (int i = 0; i < 10; i++) {
            spliceClassWatcher.executeUpdate("insert into ts_low_cardinality select * from ts_low_cardinality");
        }

        new TableCreator(conn)
                .withCreate("create table ts_high_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .create();

        PreparedStatement insert = spliceClassWatcher.prepareStatement("insert into ts_high_cardinality values (?,?,?,?)");

        long time = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            insert.setInt(1,i);
            insert.setString(2, "" + i);
            insert.setTimestamp(3,new Timestamp(time-i));
            insert.setBoolean(4,false);
            insert.addBatch();
            if (1%100==0)
                insert.executeBatch();
        }
        insert.executeBatch();

        new TableCreator(conn)
                .withCreate("create table ts_nulls (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into ts_nulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                        .create();
        new TableCreator(conn)
                .withCreate("create table ts_nonulls (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into ts_nonulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_notnulls (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into ts_notnulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_singlepk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into ts_singlepk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_multiplepk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into ts_multiplepk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_allnulls(i int, j int)")
                .withInsert("insert into ts_allnulls values (?, ?)")
                .withRows(rows(
                        row(null, null),
                        row(null, null),
                        row(null, null))).create();

        new TableCreator(conn)
                .withCreate("create table t_range_test (a1 int, b1 int, c1 int, d1 char(4), primary key (a1,b1))")
                .withIndex("create index ix_t_range_test on t_range_test (d1, a1)")
                .withInsert("insert into t_range_test values (?,?,?,?)")
                .withRows(rows(
                        row(1,1,1, "aaaa"),
                        row(1,2,1,"bbbb"),
                        row(1,3,1,"cccc"),
                        row(1,4,1,"dddd"),
                        row(1,5,1,"eeee"),
                        row(1,6,1,"ffff"),
                        row(1,7,1,"gggg"),
                        row(1,8,1,"hhhh"),
                        row(1,9,1,"iiii"),
                        row(1,10,1,"jjjj")))
                .create();

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                schemaName));

        conn.commit();
        new TableCreator(conn)
                .withCreate("create table tns_nulls (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into tns_nulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null))).create();
        new TableCreator(conn)
                .withCreate("create table tns_nonulls (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into tns_nonulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table tns_notnulls (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into tns_notnulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();
        new TableCreator(conn)
                .withCreate("create table tns_singlepk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into tns_singlepk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table tns_multiplepk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into tns_multiplepk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table t1 (a1 char(10), b1 int, c1 int, primary key(a1, b1))").create();

        new TableCreator(conn)
                .withCreate("create table t1s (a1 char(10), b1 int, c1 int, primary key(a1, b1))").create();

        new TableCreator(conn)
                .withCreate("create table t1a (a1 int)").create();

        new TableCreator(conn)
                .withCreate("create table t1b (a1 int)").create();

        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "null," +  // insert column list
                        "'%s'," +  // file path
                        "','," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "%d," +    // max bad records
                        "'%s'," +  // bad record dir
                        "true," +  // has one line records
                        "null)",   // char set
                spliceSchemaWatcher.schemaName, "T1",
                getResourceDirectory()+"even_distribution_t1.csv",
                0,
                BADDIR));
        ps.execute();

        ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "null," +  // insert column list
                        "'%s'," +  // file path
                        "','," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "%d," +    // max bad records
                        "'%s'," +  // bad record dir
                        "true," +  // has one line records
                        "null)",   // char set
                spliceSchemaWatcher.schemaName, "T1S",
                getResourceDirectory()+"skewed_distribution_t1s.csv",
                0,
                BADDIR));
        ps.execute();

        spliceClassWatcher.executeQuery(format(
                "call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','T1', false)",
                spliceSchemaWatcher.schemaName));

        spliceClassWatcher.executeQuery(format(
                "call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','T1S', false)",
                spliceSchemaWatcher.schemaName));

        spliceClassWatcher.executeQuery(format(
                "call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','T1A', false)",
                spliceSchemaWatcher.schemaName));

        spliceClassWatcher.executeQuery(format(
                "call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','T1B', false)",
                spliceSchemaWatcher.schemaName));

        new TableCreator(conn)
                .withCreate("create table t5 (a5 int, b5 decimal(10,2), c5 date, d5 timestamp, e5 varchar(10), a55 int, c55 date)")
                .withInsert("insert into t5 values(?,?,?,?,?,?,?)")
                .withRows(rows(
                        row(1,1.0,"2018-01-01","2018-01-01 09:45:00","A1",1,"2018-12-01"),
                        row(2,2.0,"2018-01-02","2018-01-02 09:45:00","A2",2,"2018-12-02"),
                        row(3,3.0,"2018-01-03","2018-01-03 09:45:00","A3",3,"2018-12-03"),
                        row(4,4.0,"2018-01-04","2018-01-04 09:45:00","A4",4,"2018-12-04"),
                        row(5,5.0,"2018-01-05","2018-01-05 09:45:00","A5",5,"2018-12-05"),
                        row(6,6.0,"2018-01-06","2018-01-06 09:45:00","A6",6,"2018-12-06"),
                        row(7,7.0,"2018-01-07","2018-01-07 09:45:00","A7",7,"2018-12-07"),
                        row(8,8.0,"2018-01-08","2018-01-08 09:45:00","A8",8,"2018-12-08"),
                        row(9,9.0,"2018-01-09","2018-01-09 09:45:00","A9",9,"2018-12-09"),
                        row(10,10.0,"2018-01-10","2018-01-10 09:45:00","A10",10,"2018-12-10")))
                .create();

        int increase = 10;
        for (int i = 0; i < 5; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t5 select a5+%1$d, b5+%1$d, c5+%1$d,d5+%1$d, 'A'||char(a5+%1$d), a5+%1$d, c5+%1$d from t5", increase));
            increase = increase*2;
        }
        for (int i=0; i<4; i++)
            spliceClassWatcher.executeUpdate(format("insert into t5 select * from t5"));


        /* enable extrapolation for a5, c5, e5, g5, h5, i5 */
        spliceClassWatcher.execute(format("call syscs_util.set_stats_extrapolation_for_column('%s', '%s', '%s', 1)", spliceSchemaWatcher.schemaName, "T5", "A5"));
        spliceClassWatcher.execute(format("call syscs_util.set_stats_extrapolation_for_column('%s', '%s', '%s', 1)", spliceSchemaWatcher.schemaName, "T5", "B5"));
        spliceClassWatcher.execute(format("call syscs_util.set_stats_extrapolation_for_column('%s', '%s', '%s', 1)", spliceSchemaWatcher.schemaName, "T5", "C5"));
        spliceClassWatcher.execute(format("call syscs_util.set_stats_extrapolation_for_column('%s', '%s', '%s', 1)", spliceSchemaWatcher.schemaName, "T5", "D5"));

        spliceClassWatcher.executeQuery(format(
                "call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','T5', false)",
                spliceSchemaWatcher.schemaName));
        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        BADDIR = SpliceUnitTest.createBadLogDirectory(spliceSchemaWatcher.schemaName).getCanonicalPath();
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }


    @Test
    public void testNullSelectivity() throws Exception {
        // with stats
        rowContainsQuery(3,"explain select * from ts_nulls where c1 is null","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_nonulls where c1 is null","outputRows=1,",methodWatcher); // clamps to 1
        rowContainsQuery(3,"explain select * from ts_notnulls where c1 is null","outputRows=1,",methodWatcher); // clamps to 1
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 is null","outputRows=1,",methodWatcher); // clamps to 1
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 is null","outputRows=1,",methodWatcher); // clamps to 1
        // without stats
        rowContainsQuery(3,"explain select * from tns_nulls where c1 is null","outputRows=2,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_nonulls where c1 is null","outputRows=2,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_notnulls where c1 is null","outputRows=2,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_singlepk where c1 is null","outputRows=1,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_multiplepk where c1 is null","outputRows=2,",methodWatcher);
    }

    @Test
    public void testNotNullSelectivity() throws Exception {
        // with stats
        rowContainsQuery(3,"explain select * from ts_nulls where c1 is not null","outputRows=5,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_nonulls where c1 is not null","outputRows=5,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_notnulls where c1 is not null","outputRows=5,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 is not null","outputRows=5,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 is not null","outputRows=5,",methodWatcher);
        // without stats
        rowContainsQuery(3,"explain select * from tns_nulls where c1 is not null","outputRows=18,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_nonulls where c1 is not null","outputRows=18,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_notnulls where c1 is not null","outputRows=18,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_singlepk where c1 is not null","outputRows=18,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_multiplepk where c1 is not null","outputRows=18,",methodWatcher);
    }

    @Test
    @Ignore("reason unknown - DB-3629 is the supposed cause but that has been resolved")
    public void testInSelectivity() throws Exception {
        // with stats
        secondRowContainsQuery("explain select * from ts_nulls where c1 in (1,2,3)", "outputRows=3", methodWatcher);
        secondRowContainsQuery("explain select * from ts_nonulls where c1 in (1,2,3)", "outputRows=3", methodWatcher);
        secondRowContainsQuery("explain select * from ts_notnulls where c1 in (1,2,3)", "outputRows=3", methodWatcher);
        firstRowContainsQuery("explain select * from ts_singlepk where c1 in (1,2,3)", "outputRows=3", methodWatcher);
        firstRowContainsQuery("explain select * from ts_multiplepk where c1 in (1,2,3)", "outputRows=3", methodWatcher);
        // without stats
        secondRowContainsQuery("explain select * from tns_nulls where c1 in (1,2,3)", "outputRows=2", methodWatcher);
        secondRowContainsQuery("explain select * from tns_nonulls where c1 in (1,2,3)", "outputRows=3", methodWatcher);
        secondRowContainsQuery("explain select * from tns_notnulls where c1 in (1,2,3)", "outputRows=0", methodWatcher);
        secondRowContainsQuery("explain select * from tns_singlepk where c1 in (1,2,3)", "outputRows=3", methodWatcher);
        secondRowContainsQuery("explain select * from tns_multiplepk where c1 in (1,2,3)", "outputRows=3", methodWatcher);

    }

    @Test
    public void testWildcardLikeSelectivity() throws Exception {
        // with stats
        rowContainsQuery(3, "explain select * from ts_nulls where c2 like '%1'", "outputRows=4,", methodWatcher); // ?JL
        rowContainsQuery(3, "explain select * from ts_nonulls where c2 like '%1'", "outputRows=3,", methodWatcher);
        rowContainsQuery(3, "explain select * from ts_notnulls where c2 like '%1'", "outputRows=3,", methodWatcher);
        rowContainsQuery(3, "explain select * from ts_singlepk where c2 like '%1'", "outputRows=3,", methodWatcher);
        rowContainsQuery(3, "explain select * from ts_multiplepk where c2 like '%1'", "outputRows=3,", methodWatcher);
        // without stats
        rowContainsQuery(3, "explain select * from tns_nulls where c2 like '%1'", "outputRows=10,", methodWatcher);
        rowContainsQuery(3, "explain select * from tns_nonulls where c2 like '%1'", "outputRows=10,", methodWatcher);
        rowContainsQuery(3, "explain select * from tns_notnulls where c2 like '%1'", "outputRows=10,", methodWatcher);
        rowContainsQuery(3, "explain select * from tns_singlepk where c2 like '%1'", "outputRows=10,", methodWatcher);
        rowContainsQuery(3, "explain select * from tns_multiplepk where c2 like '%1'", "outputRows=10,", methodWatcher);

    }

    @Test
    public void testAndSelectivity() throws Exception {
    }

    @Test
    public void testOrSelectivity() throws Exception {
    }

    @Test
    public void testNotSelectivity() throws Exception {

    }

    @Test
    public void testDB3635Between() throws Exception {
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 < 3 and c1< 6 and c1>4 and c1>5","outputRows=1,",methodWatcher);
    }

    @Test
    // 6 is out of bounds, 3 valid
    public void testSinglePKMultiprobeScan() throws Exception {
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 in (3,6,4,5)","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 in (3,6,4,5)","MultiProbeTableScan",methodWatcher);
        rowContainsQuery(4,"explain select * from ts_singlepk where c1 in (3,6,4,5) and c1 = 3","outputRows=1,",methodWatcher);
        // we will apply c1=3 first on the base table, and push the inlist to the PR Node
        rowContainsQuery(4,"explain select * from ts_singlepk where c1 in (3,6,4,5) and c1 = 3","TableScan",methodWatcher);
    }

    @Test
    public void testMultiplePKMultiprobeScan() throws Exception {
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 in (3,6,4,5) and c2 = '3'","outputRows=1",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 in (3,6,4,5) and c2 = '3'","MultiProbeTableScan",methodWatcher);
    }

    @Test
    public void testLTSelectivity() throws Exception {

        // with stats
        rowContainsQuery(3,"explain select * from ts_nulls where c1 < 3", "outputRows=2,", methodWatcher);
        rowContainsQuery(3,"explain select * from ts_nonulls where c1 < 3", "outputRows=2,", methodWatcher);
        rowContainsQuery(3,"explain select * from ts_notnulls where c1 < 3", "outputRows=2,", methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 < 3","outputRows=2,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 < 3","outputRows=2,",methodWatcher);
        // without stats
        rowContainsQuery(3,"explain select * from tns_nulls where c1 < 3","outputRows=18",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_nonulls where c1 < 3","outputRows=18",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_notnulls where c1 < 3","outputRows=18",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_singlepk where c1 < 3","outputRows=18",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_multiplepk where c1 < 3","outputRows=18",methodWatcher);

    }

    @Test
    public void testLTESelectivity() throws Exception {
        // with stats
        rowContainsQuery(3,"explain select * from ts_nulls where c1 <= 3","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_nonulls where c1 <= 3","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_notnulls where c1 <= 3","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 <= 3","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 <= 3","outputRows=3,",methodWatcher);
        Assert.assertEquals(500.0d,parseOutputRows(getExplainMessage(3,"explain select * from ts_high_cardinality where c1 <= 500",methodWatcher)),100.0d);
        Assert.assertEquals(1000.0d,parseOutputRows(getExplainMessage(3,"explain select * from ts_high_cardinality where c1 <= 1000",methodWatcher)),100.0d);
        Assert.assertEquals(2000.0d,parseOutputRows(getExplainMessage(3,"explain select * from ts_high_cardinality where c1 <= 2000",methodWatcher)),100.0d);
        Assert.assertEquals(8000.0d,parseOutputRows(getExplainMessage(3,"explain select * from ts_high_cardinality where c1 <= 8000",methodWatcher)),100.0d);


        // without stats
/*        firstRowContainsQuery("explain select * from tns_nulls where c1 <= 3","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_nonulls where c1 <= 3","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_notnulls where c1 <= 3","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_singlepk where c1 <= 3","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_multiplepk where c1 <= 3","outputRows=10",methodWatcher);
        */
    }


    @Test
    public void testGTSelectivity() throws Exception {

    }

    @Test
    public void testGTESelectivity() throws Exception {

    }

    @Test
    public void testBetweenSelectivity() throws Exception {
        Assert.assertEquals(500.0d,parseOutputRows(getExplainMessage(3,"explain select * from ts_high_cardinality where c1 <= 500 and c1 >= 0",methodWatcher)),100.0d);
        Assert.assertEquals(500.0d,parseOutputRows(getExplainMessage(3,"explain select * from ts_high_cardinality where c1 <= 1000 and c1 >= 500",methodWatcher)),100.0d);
        Assert.assertEquals(1501.0d,parseOutputRows(getExplainMessage(3,"explain select * from ts_high_cardinality where c1 <= 2000 and c1 >= 500",methodWatcher)),100.0d);
        Assert.assertEquals(7001.0d,parseOutputRows(getExplainMessage(3,"explain select * from ts_high_cardinality where c1 <= 8000 and c1 >= 1000",methodWatcher)),100.0d);

    }


    @Test
    @Ignore("reason unknown")
    public void testOutOfBoundsPredicates() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from ts_nulls where date(c3)='0000-01-01'");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1), rs.getString(1).contains("outputRows=5"));
    }


    @Test
    @Ignore("10% Selectivity")
    public void testProjectionSelectivity() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from ts_nulls where c2 like '%1%'");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1), rs.getString(1).contains("outputRows=1"));
    }

    @Test
    @Ignore("reason unknown")
    public void testExtractOperatorNodeSelectivity() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from ts_low_cardinality where month(c3) = 1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1), rs.getString(1).contains("outputRows=1"));
    }

    @Test
    public void testLimitCosting() throws Exception {
        rowsContainsQuery("explain select * from ts_low_cardinality {limit 10}", new Contains().add(1, "rows=10,"), methodWatcher);
    }


    @Test
    public void testTernaryOperator() throws Exception {
        rowContainsQuery(2, "explain select distinct trim(cast(c1 as char(5))) as j from ts_nulls", "outputRows=5,", methodWatcher);
    }

    @Test
    public void testAllNullSelectivity() throws Exception {
        rowContainsQuery(3, "explain select * from ts_allnulls t1, ts_allnulls t2 where t1.i = t2.i", "outputRows=1", methodWatcher);
    }
    
    @Test
    @Ignore
    public void testSelectColumnStatistics() throws Exception {
        String url = "jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin;useSpark=true";
        Connection connection = DriverManager.getConnection(url, new Properties());
        PreparedStatement ps = connection.prepareStatement("select * from sysvw.syscolumnstatistics");
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while(rs.next()){
            count++;
        }
        Assert.assertTrue(count>0);
    }

    @Test
    public void testEvenDistributionSelectivity() throws Exception {
        /* T1's data demographics are as follows:
        a1: char, 100,000 rows, 1000 distinct values, rows per value(rpv) is 100
        b1: int, unique
        c1: int, 100,000 rows, 100 distinct values, rows per value(rpv) is 1000
         */
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1 where a1='CCC'", methodWatcher));
        Assert.assertEquals(99, rowCount, 10);
        double rangeRowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1 where a1>='CCC' and a1<'CCD'", methodWatcher));
        Assert.assertTrue(rangeRowCount>=rowCount);

        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1 where c1=50", methodWatcher));
        Assert.assertEquals(1000, rowCount, 100);
        rangeRowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1 where c1>=50 and c1<51", methodWatcher));
        Assert.assertTrue(rangeRowCount >= rowCount);
    }

    @Test
    public void testSkewedDistributionSelectivity() throws Exception {
        /* T1S's data demographics are as follows:
        a1: char, 100,000 rows, 1001 distinct values, rows per value(rpv) is 50 for all values except for one skewed value 'KKK' which has rpv 50,000
        b1: int, unique
        c1: int, 100,000 rows, 51 distinct values, rows per value(rpv) is rows per value(rpv) is 1000 except for one skewed value 50 which has rpv 50,000
         */
        /* test skewed value */
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1s where a1='KKK'", methodWatcher));
        Assert.assertEquals(50000, rowCount, 1000);
        double rangeRowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1s where a1>='KKK' and a1<'KKL'", methodWatcher));
        Assert.assertTrue(rangeRowCount >= rowCount);

        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1s where c1=50", methodWatcher));
        Assert.assertEquals(50000, rowCount, 1000);
        rangeRowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1s where c1>=50 and c1<51", methodWatcher));
        Assert.assertTrue(rangeRowCount >= rowCount);

        /* test non-skewed value */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1s where a1='CCC'", methodWatcher));
        Assert.assertEquals(49, rowCount, 5);
        rangeRowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1s where a1>='CCC' and a1<'CCD'", methodWatcher));
        Assert.assertTrue(rangeRowCount >= rowCount);

        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1s where c1=31", methodWatcher));
        Assert.assertEquals(1000, rowCount, 100);
        rangeRowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1s where c1>30 and c1<=31", methodWatcher));
        Assert.assertTrue(rangeRowCount >= rowCount);
    }

    @Test
    public void testEqualityPredicateWithUnknownConstantExpression() throws Exception {
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1 where a1=CAST (new java.lang.Object() AS CHAR(10))", methodWatcher));
        Assert.assertEquals(100, rowCount, 10);
    }

    @Test
    public void testNoteEqualPredicateWithUnknownConstantExpression() throws Exception {
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t1 where a1 != CAST (new java.lang.Object() AS CHAR(10))", methodWatcher));
        Assert.assertEquals(99900, rowCount, 10);
    }

    @Test
    public void testRangeSelectivityWithDifferentPredicateOrder() throws Exception {
        double rowCount = parseOutputRows(getExplainMessage(4,
                "explain select * from t_range_test --splice-properties index=ix_t_range_test\n where a1=1 and d1 < 'cccc' and d1 >='bbbb'", methodWatcher));
        Assert.assertEquals(1, rowCount, 0);
        double rowCount1 = parseOutputRows(getExplainMessage(4,
                "explain select * from t_range_test --splice-properties index=ix_t_range_test\n where a1=1 and d1 >='bbbb' and d1 < 'cccc'", methodWatcher));
        Assert.assertTrue(rowCount == rowCount);
    }

    @Test
    public void testZeroRowAntiJoinSelectivity() throws  Exception {
        rowContainsQuery(8,"explain select t1a.a1 from t1a,t1b where not exists (select 1 from t1b where t1a.a1 = t1b.a1)","outputRows=1,",methodWatcher);
    }

    @Test
    public void testIntPointRangeWithExtrapolation() throws Exception {
        /* column with extrapolation enabled, a5 is between [1,320] */
        /* value inside min-max range */
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where a5=10", methodWatcher));
        /* accurate number is 16, but due to inaccuracy in stats, it could be somewhat off, but it should not be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount >=8);
        /* value outside min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where a5=1000", methodWatcher));
        /* accurate number is 16, but due to inaccuracy in stats, it could be somewhat off, but it should not be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount >=8);

        /* column with extrapolation disabled a55 has the same value as a5 */
        /* value inside min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where a55=10", methodWatcher));
        /* accurate number is 16, but due to inaccuracy in stats, it could be somewhat off, but it should not be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount >=8);
        /* value outside min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where a55=1000", methodWatcher));
        /* estimation should be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount ==1);
    }

    @Test
    public void testDatePointRangeWithExtrapolation() throws Exception {
        /* column with extrapolation enabled, c5 is between ['2018-01-01,2018-11-16] */
        /* value inside min-max range */
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c5='2018-01-30'", methodWatcher));
        /* accurate number is 16, but due to inaccuracy in stats, it could be somewhat off, but it should not be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount >=8);
        /* value outside min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c5='2019-01-01'", methodWatcher));
        /* accurate number is 16, but due to inaccuracy in stats, it could be somewhat off, but it should not be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount >=8);

        /* column with extrapolation disabled a55 has the same value as c55 */
        /* value inside min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c5='2018-01-30'", methodWatcher));
        /* accurate number is 16, but due to inaccuracy in stats, it could be somewhat off, but it should not be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount >=8);
        /* value outside min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c55='2019-01-01'", methodWatcher));
                /* estimation should be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount == 1);
    }

    @Test
    public void testTimestampPointRangeWithExtrapolation() throws Exception {
        /* column with extrapolation enabled, d5 is between ['2018-01-01 09:45:00.0,2018-11-16 09:45:00.0] */
        /* value inside min-max range */
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where d5='2018-01-30 09:45:00.0'", methodWatcher));
        /* accurate number is 16, but due to inaccuracy in stats, it could be somewhat off, but it should not be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount >=8);
        /* value outside min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where d5='2019-01-01 09:45:00.0'", methodWatcher));
        /* accurate number is 16, but due to inaccuracy in stats, it could be somewhat off, but it should not be 1 */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount >=8);
    }

    @Test
    public void testNumberRangeWithExtrapolation() throws Exception {
        /* column with extrapolation enabled, b5 is between [1.0, 320.0] */
        /* start < min */
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where b5 > -5.0", methodWatcher));
        /* estimation is bound by the total not-null rows */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount ==5120);

        /* stop > max */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where b5 < 1000.0", methodWatcher));
        /* estimation is bound by the total not-null rows */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount ==5120);

        /* range partially fall-out of range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where b5 between 300.0 and 480.0", methodWatcher));
        /* this should be projected to around half of the total rows */
        Assert.assertEquals("Estimation wrong, actual rowCount="+rowCount, 2560, rowCount, 1000);

        /* range completely fall out of range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where b5 between 400.0 and 560.0", methodWatcher));
        /* this should be projected to around half of the total rows */
        Assert.assertEquals("Estimation wrong, actual rowCount="+rowCount, 2560, rowCount, 1000);
    }

    @Test
    public void testDateRangeWithExtrapolation() throws Exception {
        /* column with extrapolation enabled, c5 between ['2018-01-01','2018-11-16'] */
        /* start < min */
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c5 > '2017-12-01'", methodWatcher));
        /* estimation is bound by the total not-null rows */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount ==5120);

        /* stop > max */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c5 < '2018-12-31'", methodWatcher));
        /* estimation is bound by the total not-null rows */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount ==5120);

        /* range partially fall-out of range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c5 between '2018-11-01' and '2019-05-30'", methodWatcher));
        /* this should be projected to around half of the total rows */
        Assert.assertEquals("Estimation wrong, actual rowCount="+rowCount, 2560, rowCount, 1000);

        /* range completely fall out of range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c5 between '2015-01-01' and '2015-06-15'", methodWatcher));
        /* this should be projected to around half of the total rows */
        Assert.assertEquals("Estimation wrong, actual rowCount="+rowCount, 2560, rowCount, 1000);

        /******************************************************************/
        /*compare to column c55 with same data but extrapolation disabled */
        /******************************************************************/
        /* start < min */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c55 > '2017-12-01'", methodWatcher));
        /* estimation is bound by the total not-null rows */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount ==5120);

        /* stop > max */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c55 < '2018-12-31'", methodWatcher));
        /* estimation is bound by the total not-null rows */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount ==5120);

        /* range partially fall-out of range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c55 between '2018-11-01' and '2019-05-30'", methodWatcher));
        /* without extrapolation, only the range ['2018-11-01', '2018-11-16'] contribute to the estimation */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount<1000);

        /* range completely fall out of range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c55 between '2015-01-01' and '2015-06-15'", methodWatcher));
        /* this should be projected to around half of the total rows */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount ==1);
    }

    @Test
    public void testInlistWithExtrapolation() throws Exception {
        /* element fall out of the min-max range */
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c5 in ('2017-12-01', '2017-12-02')", methodWatcher));
        /* we use average rows per value to extrapolate for each element in the inlist even if it falls out of the min-max range */
        Assert.assertEquals("Estimation wrong, actual rowCount="+rowCount, rowCount, 32, 16);

        /******************************************************************/
        /*compare to column c55 with same data but extrapolation disabled */
        /******************************************************************/
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where c55 in ('2017-12-01', '2017-12-02')", methodWatcher));
        /* no row will qualify with elements out side the min-max range */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount==1);
    }

    @Test
    public void testNotEqualsWithExtrapolation() throws Exception {
        /********************************************/
        /*test column a5 with extrapolation enabled */
        /********************************************/
        /* element fall in the min-max range */
        double rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where a5 != 30", methodWatcher));
        /* estimation should be less than the total rows */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount<5120);

        /* element fall out of the min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where a5 != 1000", methodWatcher));
        /* with extrapolation, estimation should be less than the total rows */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount<5120);
        /******************************************************************/
        /*compare to column a55 with same data but extrapolation disabled */
        /******************************************************************/
        /* element within the min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where a55 != 30", methodWatcher));
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount<5120);

        /* element fall out of the min-max range */
        rowCount = parseOutputRows(getExplainMessage(3, "explain select * from t5 where a55 != 1000", methodWatcher));
        /* estimation should be the same as the total rows, as no rows should be excluded */
        Assert.assertTrue("Estimation wrong, actual rowCount="+rowCount, rowCount==5120);
    }

}
