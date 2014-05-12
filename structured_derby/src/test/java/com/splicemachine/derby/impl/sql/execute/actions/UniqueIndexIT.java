package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Created on: 3/7/13
 */
public class UniqueIndexIT extends SpliceUnitTest {
    private static final String SCHEMA_NAME = UniqueIndexIT.class.getSimpleName().toUpperCase();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME_1 = "A";
    public static final String INDEX_11 = "IDX_A1";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1, SCHEMA_NAME,
                                                                                     "(name varchar(40), val int)");
    public static final String TABLE_NAME_2 = "B";
    public static final String INDEX_2 = "IDX_B";
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2, SCHEMA_NAME,
                                                                                     "(name varchar(40), val int)");
    public static final String TABLE_NAME_3 = "C";
    public static final String INDEX_3 = "IDX_C";
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3, SCHEMA_NAME,
                                                                                     "(name varchar(40), val int)");
    public static final String TABLE_NAME_4 = "D";
    public static final String INDEX_4 = "IDX_D";
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4, SCHEMA_NAME,
                                                                                     "(name varchar(40), val int)");
    public static final String TABLE_NAME_5 = "E";
    public static final String INDEX_5 = "IDX_E";
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5, SCHEMA_NAME,
                                                                                     "(name varchar(40), val int)");
    public static final String TABLE_NAME_6 = "F";
    public static final String INDEX_6 = "IDX_F";
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6, SCHEMA_NAME,
                                                                                     "(name varchar(40), val int)");
    public static final String TABLE_NAME_7 = "G";
    public static final String INDEX_7 = "IDX_G";
    protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_NAME_7, SCHEMA_NAME,
                                                                                     "(name varchar(40), val int)");
    public static final String TABLE_NAME_8 = "H";
    protected static SpliceTableWatcher uniqueTableWatcher8 = new SpliceTableWatcher(TABLE_NAME_8, SCHEMA_NAME,
                                                                                     "(name varchar(40), val int, " +
                                                                                         "constraint FOO unique(val))");
    public static final String TABLE_NAME_9 = "I";
    public static final String INDEX_9 = "IDX_I";
    protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher(TABLE_NAME_9, SCHEMA_NAME,
                                                                                     "(name varchar(40), val int)");
    public static final String TABLE_NAME_10 = "J";
    public static final String INDEX_10 = "IDX_J";
    protected static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher(TABLE_NAME_10, SCHEMA_NAME,
                                                                                      "(name varchar(40), val int)");
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceSchemaWatcher)
                                            .around(spliceClassWatcher)
                                            .around(spliceTableWatcher1)
                                            .around(spliceTableWatcher2)
                                            .around(spliceTableWatcher3)
                                            .around(spliceTableWatcher4)
                                            .around(spliceTableWatcher5)
                                            .around(spliceTableWatcher6)
                                            .around(spliceTableWatcher7)
                                            .around(uniqueTableWatcher8)
                                            .around(spliceTableWatcher9)
                                            .around(spliceTableWatcher10);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Override
    public String getSchemaName() {
        return SCHEMA_NAME;
    }

    private static void verifyResult(ResultSet rs) throws SQLException {
        List<String> results = Lists.newArrayListWithExpectedSize(0);
        Assert.assertFalse("Rows were returned incorrectly!", rs.next());
        while (rs.next()) {
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned! " + results, 0, results.size());
    }

    /**
     * Basic test to ensure that a Unique Index can be used
     * to perform lookups.
     * <p/>
     * We create the Index BEFORE we add data, to ensure that
     * we don't deal with any kind of situation which might
     * arise from adding the index after data exists
     * <p/>
     * Basically, create an index, then add some data to the table,
     * then scan for data through the index and make sure that the
     * correct data returns.
     */
    @Test(timeout = 10000)
    public void testCanUseUniqueIndex() throws Exception {
        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(TABLE_NAME_1, SCHEMA_NAME, INDEX_11, SCHEMA_NAME,
                                                                 "(name)", true);
        indexWatcher.starting(null);
        try {
            //now add some data
            String name = "sfines";
            int value = 2;
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(TABLE_NAME_1), name, value));

            //now check that we can get data out for the proper key
            ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                                    this.getTableReference(TABLE_NAME_1), name));
            List<String> results = Lists.newArrayListWithExpectedSize(1);
            while (resultSet.next()) {
                String retName = resultSet.getString(1);
                int val = resultSet.getInt(2);
                Assert.assertEquals("Incorrect name returned!", name, retName);
                Assert.assertEquals("Incorrect value returned!", value, val);
                results.add(String.format("name:%s,value:%d", retName, val));
            }
            Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
        } finally {
            indexWatcher.drop();
        }
    }

    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index
     * <p/>
     * Basically, add some data to the table, then create the index,
     * then perform a lookup on that same data via the index to ensure
     * that the index will find those values.
     */
    @Test(timeout = 10000)
    public void testCanCreateIndexFromExistingData() throws Exception {
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(TABLE_NAME_2), name, value));
        //create the index
        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(TABLE_NAME_2, SCHEMA_NAME, INDEX_2, SCHEMA_NAME,
                                                                 "(name)", true);
        indexWatcher.starting(null);

        try {
            //now check that we can get data out for the proper key
            ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                                    this.getTableReference(TABLE_NAME_2), name));
            List<String> results = Lists.newArrayListWithExpectedSize(1);
            while (resultSet.next()) {
                String retName = resultSet.getString(1);
                int val = resultSet.getInt(2);
                Assert.assertEquals("Incorrect name returned!", name, retName);
                Assert.assertEquals("Incorrect value returned!", value, val);
                results.add(String.format("name:%s,value:%d", retName, val));
            }
            Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
        } finally {
            indexWatcher.drop();
        }
    }

    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index, that we can safely add data to
     * <p/>
     * Basically, add some data, create an index off of that, and then
     * add some more data, and check to make sure that the new data shows up as well
     */
    @Test(timeout = 10000)
    public void testCanCreateIndexFromExistingDataAndThenAddData() throws Exception {
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(TABLE_NAME_3), "sfines", 2));
        //create the index
        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(TABLE_NAME_3, SCHEMA_NAME, INDEX_3, SCHEMA_NAME,
                                                                 "(name)", true);
        indexWatcher.starting(null);

        try {
            //add some more data
            String name = "jzhang";
            int value = 2;
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(TABLE_NAME_3), name, value));

            //now check that we can get data out for the proper key
            ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                                    this.getTableReference(TABLE_NAME_3), name));
            List<String> results = Lists.newArrayListWithExpectedSize(1);
            while (resultSet.next()) {
                String retName = resultSet.getString(1);
                int val = resultSet.getInt(2);
                Assert.assertEquals("Incorrect name returned!", name, retName);
                Assert.assertEquals("Incorrect value returned!", value, val);
                results.add(String.format("name:%s,value:%d", retName, val));
            }
            Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
        } finally {
            indexWatcher.drop();
        }
    }

    /**
     * Tests that the uniqueness constraint is correctly managed.
     * <p/>
     * Basically, create an index, add some data, then try and
     * add some duplicate data, and validate that the duplicate
     * data cannot succeed.
     */
    @Test(expected = SQLException.class, timeout = 10000)
    public void testViolateUniqueConstraint() throws Exception {
        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(TABLE_NAME_4, SCHEMA_NAME, INDEX_4, SCHEMA_NAME,
                                                                 "(name)", true);
        indexWatcher.starting(null);

        try {
            String name = "sfines";
            int value = 2;
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(TABLE_NAME_4), name, value));
            try {
                methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                            this.getTableReference(TABLE_NAME_4), name, value));
            } catch (SQLException se) {
                SpliceLogUtils.error(Logger.getLogger(UniqueIndexIT.class), se);
                if (se.getMessage().contains("unique"))
                    throw se;
            }
            Assert.fail("Did not report a duplicate key violation!");
        } finally {
            indexWatcher.drop();
        }
    }

    /**
     * Tests that we can safely drop the index, and constraints
     * will also be dropped.
     * <p/>
     * Basically, create an index, add some data, validate
     * that the uniqueness constraint holds, then drop the index
     * and validate that A) the uniqueness constraint doesn't hold
     * and B) the index is no longer used in the lookup.
     */
    @Test(timeout = 10000)
    public void testCanDropIndex() throws Exception {

        //ensure that the uniqueness constraint holds
        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(TABLE_NAME_5, SCHEMA_NAME, INDEX_5, SCHEMA_NAME,
                                                                 "(name)", true);
        indexWatcher.starting(null);
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(TABLE_NAME_5), name, value));
        try {
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(TABLE_NAME_5), name, value));
            Assert.fail("Uniqueness constraint violated");
        } catch (SQLException se) {
            Assert.assertEquals(ErrorState.LANG_DUPLICATE_KEY_CONSTRAINT.getSqlState(), se.getSQLState());
        }
        indexWatcher.finished(null);

        //validate that we can add duplicates now
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(TABLE_NAME_5), name, value));

        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                                this.getTableReference(TABLE_NAME_5), name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while (resultSet.next()) {
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!", name, retName);
            Assert.assertEquals("Incorrect value returned!", value, val);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned!", 2, results.size());
    }

    /**
     * Tests that we can safely delete a record, and have it
     * percolate through to the index.
     * <p/>
     * Basically, create an index, add some data, check if its
     * present, then delete some data and check that it's not
     * there anymore
     */
    @Test(timeout = 10000)
    public void testCanDeleteEntry() throws Exception {
        String tableName = "testCanDeleteEntry";
        String indexName = "IDX_"+tableName;
        new MyWatcher(tableName, SCHEMA_NAME, "(name varchar(40), val int)").create(null);
        SpliceIndexWatcher spliceIndexWatcher = new SpliceIndexWatcher(tableName, SCHEMA_NAME, indexName,
                                                                       SCHEMA_NAME, "(name)", true);
        spliceIndexWatcher.starting(null);
        try {
            String name = "sfines";
            int value = 2;
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), name, value));
            methodWatcher.getStatement().execute(format("delete from %s where name = '%s'",
                                                        this.getTableReference(tableName), name));
            ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                             this.getTableReference(tableName), name));
            Assert.assertTrue("Rows are returned incorrectly", !rs.next());
        } finally {
            spliceIndexWatcher.drop();
            MyWatcher.executeDrop(SCHEMA_NAME,tableName);
        }
    }

    /**
     * DB-1020
     * Tests that we can safely delete a record, and have it
     * percolate through to the index.
     * <p/>
     * NULL values should NOT be deleted.
     */
    @Test
    public void testCanDeleteIndexWithNulls() throws Exception {
        SpliceIndexWatcher spliceIndexWatcher = new SpliceIndexWatcher(TABLE_NAME_9, SCHEMA_NAME, INDEX_9,
                                                                       SCHEMA_NAME, "(val)", false);
        spliceIndexWatcher.starting(null);
        try {
            PreparedStatement ps = methodWatcher.prepareStatement(String.format("insert into %s (name,val) values (?," +
                                                                                    "?)", spliceTableWatcher9));
            ps.setString(1, "sfines");
            ps.setInt(2, 2);
            ps.addBatch();
            ps.setString(1, "lfines");
            ps.setInt(2, -2);
            ps.addBatch();
            ps.setNull(1, Types.VARCHAR);
            ps.setNull(2, Types.INTEGER);
            ps.addBatch();
            ps.setString(1, "0");
            ps.setInt(2, 0);
            ps.addBatch();
            int[] updated = ps.executeBatch();
            Assert.assertEquals("Incorrect update number!", 4, updated.length);
            System.out.println(Arrays.toString(updated));

            String query = format("select * from %s", this.getTableReference(TABLE_NAME_9));
            ResultSet rs = methodWatcher.executeQuery(query);
            TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 4, fr.size());

            methodWatcher.getStatement().execute(format("delete from %s where val > 0",
                                                        this.getTableReference(TABLE_NAME_9)));
            rs = methodWatcher.executeQuery(query);

            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 3, fr.size());

            methodWatcher.getStatement().execute(format("delete from %s where val < 0", this.getTableReference
                (TABLE_NAME_9)));
            rs = methodWatcher.executeQuery(query);

            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 2, fr.size());
        } finally {
            spliceIndexWatcher.drop();
        }
    }

    /**
     * DB-1020
     * Tests that we can safely delete a record, and have it
     * percolate through to the unique index.
     * <p/>
     * NULL values should NOT be deleted.
     */
    @Test
    public void testCanDeleteUniqueIndexWithoutNulls() throws Exception {
        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(TABLE_NAME_10, SCHEMA_NAME, INDEX_10, SCHEMA_NAME, "(val)", true);
        indexWatcher.starting(null);
        try {
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(TABLE_NAME_10), "sfines", -2));
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(TABLE_NAME_10), "lfines", 2));
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values (null,null)",
                                                        this.getTableReference(TABLE_NAME_10)));
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('0',0)",
                                                        this.getTableReference(TABLE_NAME_10)));

            String query = format("select * from %s", this.getTableReference(TABLE_NAME_10));
            ResultSet rs = methodWatcher.executeQuery(query);
            TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println("1:");
//        System.out.println(fr.toString());
            Assert.assertEquals(fr.toString(), 4, fr.size());

            methodWatcher.getStatement().execute(format("delete from %s where val > 0",
                                                        this.getTableReference(TABLE_NAME_10)));
            rs = methodWatcher.executeQuery(query);

            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println("2:");
//        System.out.println(fr.toString());
            Assert.assertEquals(fr.toString(), 3, fr.size());

            methodWatcher.getStatement().execute(format("delete from %s where val < 0",
                                                        this.getTableReference(TABLE_NAME_10)));
            rs = methodWatcher.executeQuery(query);

            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println("3:");
//        System.out.println(fr.toString());
            Assert.assertEquals(fr.toString(), 2, fr.size());
        } finally {
            indexWatcher.drop();
        }
    }

    /**
     * DB-1092
     * Tests that we can delete a row with a non-null value in unique index column then insert same row.
     */
    @Test
    public void testCanDeleteUniqueIndexWithoutNulls2() throws Exception {
        String indexName = "IDX2";
        String tableName = "T2";
        new MyWatcher(tableName, SCHEMA_NAME, "(name varchar(40), val int)").create(null);
        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(tableName, SCHEMA_NAME, indexName, SCHEMA_NAME,
                                                                 "(val)", true);
        indexWatcher.starting(null);

        try {
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), "sfines", -2));
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), "lfines", 2));
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), "MyNull", null));
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('0',0)",
                                                        this.getTableReference(tableName)));

            String query = format("select * from %s", this.getTableReference(tableName));
            ResultSet rs = methodWatcher.executeQuery(query);
            TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 4, fr.size());
//        System.out.println("1:");
//        System.out.println(fr.toString());
//
//        System.out.println(TestUtils.indexQuery(methodWatcher.getOrCreateConnection(), SCHEMA_NAME, tableName));

            methodWatcher.getStatement().execute(format("delete from %s", this.getTableReference(tableName)));
            rs = methodWatcher.executeQuery(query);
            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 0, fr.size());
//        System.out.println("2:");
//        System.out.println(fr.toString());

            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), "0", 0));
            rs = methodWatcher.executeQuery(query);
            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 1, fr.size());
//        System.out.println("3:");
//        System.out.println(fr.toString());

            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), "sfines", -2));
            rs = methodWatcher.executeQuery(query);
            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 2, fr.size());
//        System.out.println("4:");
//        System.out.println(fr.toString());
        } finally {
            indexWatcher.drop();
            MyWatcher.executeDrop(SCHEMA_NAME,tableName);
        }
    }

    /**
     * DB-1092
     * Tests that we can delete a row with null in unique index column then insert same row.
     */
    @Test
    public void testCanDeleteUniqueIndexWithNulls() throws Exception {
        String indexName = "IDX1";
        String tableName = "T1";
        new MyWatcher(tableName, SCHEMA_NAME, "(name varchar(40), val int)").create(null);
        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(tableName, SCHEMA_NAME, indexName, SCHEMA_NAME, "(val)", true);
        indexWatcher.starting(null);

        try {
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), "sfines", -2));
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), "lfines", 2));
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), "MyNull", null));
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('0',0)",
                                                        this.getTableReference(tableName)));

            String query = format("select * from %s", this.getTableReference(tableName));
            ResultSet rs = methodWatcher.executeQuery(query);
            TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 4, fr.size());
//        System.out.println("1:");
//        System.out.println(fr.toString());
//
//        System.out.println(TestUtils.indexQuery(methodWatcher.getOrCreateConnection(), SCHEMA_NAME, tableName));

            methodWatcher.getStatement().execute(format("delete from %s where val is null",
                                                        this.getTableReference(tableName)));
            rs = methodWatcher.executeQuery(query);
            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 3, fr.size());
//        System.out.println("2:");
//        System.out.println(fr.toString());

            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        this.getTableReference(tableName), "MyNull", null));
            rs = methodWatcher.executeQuery(query);
            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 4, fr.size());
//        System.out.println("3:");
//        System.out.println(fr.toString());
        } finally {
            indexWatcher.drop();
            MyWatcher.executeDrop(SCHEMA_NAME, tableName);
        }
    }

    /**
     * DB-1110
     * Tests create a descending index.
     */
    @Test
    public void testCanCreateDescendingIndex() throws Exception {
        String indexName = "descinx";
        String tableName = "T";
        new MyWatcher(tableName, SCHEMA_NAME, "(c1 int, c2 smallint)").create(null);
        SpliceIndexWatcher indexWatcher = null;
        try {
            methodWatcher.getStatement().execute(format("insert into %s (c1,c2) values (%s,%s)",
                                                        this.getTableReference(tableName), 8, 12));
            methodWatcher.getStatement().execute(format("insert into %s (c1,c2) values (%s,%s)",
                                                        this.getTableReference(tableName), 56, -3));

            String query = format("select min(c2) from %s", this.getTableReference(tableName));
            ResultSet rs = methodWatcher.executeQuery(query);
            TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 1, fr.size());
            System.out.println("1:");
            System.out.println(fr.toString());


            indexWatcher = new SpliceIndexWatcher(tableName, SCHEMA_NAME, indexName, SCHEMA_NAME, "(c2 desc, c1)", false);
            indexWatcher.starting(null);
            System.out.println(TestUtils.indexQuery(methodWatcher.getOrCreateConnection(), SCHEMA_NAME, tableName));

            rs = methodWatcher.executeQuery(query);
            fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
            Assert.assertEquals(fr.toString(), 1, fr.size());
            System.out.println("2:");
            System.out.println(fr.toString());
        } finally {
            if (indexWatcher != null) {
                indexWatcher.drop();
            }
            MyWatcher.executeDrop(SCHEMA_NAME,tableName);
        }
    }

    @Test(timeout = 10000)
    public void testCanDeleteThenInsertEntryInTransaction() throws Exception {
        String tableName = "testCanDeleteThenInsertEntryInTransaction";
        String indexName = "IDX_"+tableName;
        new MyWatcher(tableName, SCHEMA_NAME, "(name varchar(40), val int)").create(null);
        SpliceIndexWatcher spliceIndexWatcher = new SpliceIndexWatcher(tableName, SCHEMA_NAME, indexName,
                                                                       SCHEMA_NAME, "(name)", true);
        spliceIndexWatcher.starting(null);
        String name = "sfines";
        int value = 2;
        try {
            methodWatcher.getOrCreateConnection().setAutoCommit(false);
            methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)",
                                                        this.getTableReference(tableName), name, value));

            methodWatcher.getStatement().execute(format("delete from %s", this.getTableReference(tableName), name));
            methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)",
                                                        this.getTableReference(tableName), name, value));
            ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                             this.getTableReference(tableName), name));
            List<String> results = Lists.newArrayListWithExpectedSize(1);
            while (rs.next()) {
                String retName = rs.getString(1);
                int val = rs.getInt(2);
                results.add(String.format("name:%s,value:%d", retName, val));
            }
            Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
            methodWatcher.getOrCreateConnection().commit();
        } finally {
            spliceIndexWatcher.drop();
            MyWatcher.executeDrop(SCHEMA_NAME,tableName);
        }
    }

    @Test(timeout = 10000)
    public void testCanInsertThenDeleteEntryInTransaction() throws Exception {
        String tableName = "testCanInsertThenDeleteEntryInTransaction";
        String indexName = "IDX_"+tableName;
        new MyWatcher(tableName, SCHEMA_NAME, "(name varchar(40), val int)").create(null);
        SpliceIndexWatcher spliceIndexWatcher = new SpliceIndexWatcher(tableName, SCHEMA_NAME, indexName,
                                                                       SCHEMA_NAME, "(name)", true);
        try {
            Connection connection = methodWatcher.getOrCreateConnection();
            connection.setAutoCommit(false);
            spliceIndexWatcher.starting(null);
            String name = "sfines3";
            int value = 2;
            methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)",
                                                        this.getTableReference(tableName), name, value));
            methodWatcher.getStatement().execute(format("delete from %s where name = '%s'",
                                                        this.getTableReference(tableName), name));
            connection.commit();
            ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                             this.getTableReference(tableName), name));
            verifyResult(rs);
        } finally {
            spliceIndexWatcher.drop();
            MyWatcher.executeDrop(SCHEMA_NAME,tableName);
        }
    }

    @Test(timeout = 10000)
    public void testCanInsertThenDeleteThenInsertAgainInTransaction() throws Exception {
        SpliceIndexWatcher spliceIndexWatcher = new SpliceIndexWatcher(TABLE_NAME_6, SCHEMA_NAME, INDEX_6,
                                                                       SCHEMA_NAME, "(name)", true);
        try {
            Connection connection = methodWatcher.getOrCreateConnection();
            connection.setAutoCommit(false);
            spliceIndexWatcher.starting(null);
            String name = "sfines3";
            int value = 2;
            methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)",
                                                        this.getTableReference(TABLE_NAME_6), name, value));
            methodWatcher.getStatement().execute(format("delete from %s where name = '%s'",
                                                        this.getTableReference(TABLE_NAME_6), name));
            connection.commit();
            ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                             this.getTableReference(TABLE_NAME_6), name));
            verifyResult(rs);

            connection = methodWatcher.getOrCreateConnection();
            connection.setAutoCommit(false);
            methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)",
                                                        this.getTableReference(TABLE_NAME_6), name, value));
            methodWatcher.getStatement().execute(format("delete from %s where name = '%s'",
                                                        this.getTableReference(TABLE_NAME_6), name));
            connection.commit();
            rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                             this.getTableReference(TABLE_NAME_6), name));
            verifyResult(rs);
        } finally {
            spliceIndexWatcher.drop();
        }
    }

    //    @Test(timeout= 10000)
    @Test
    public void testCanUpdateEntryIndexChanges() throws Exception {
        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(TABLE_NAME_7, SCHEMA_NAME, INDEX_7, SCHEMA_NAME,
                                                                 "(name)", true);
        indexWatcher.starting(null);

        try {
            String name = "sfines";
            int value = 2;
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                        spliceTableWatcher7, name, value));

            String newName = "jzhang";
            methodWatcher.getStatement().execute(format("update %s set name = '%s' where name = '%s'",
                                                        spliceTableWatcher7, newName, name));

            ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",
                                                             spliceTableWatcher7, name));
            Assert.assertTrue("Rows are returned incorrectly", !rs.next());

            rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'", spliceTableWatcher7, newName));
            List<String> results = Lists.newArrayListWithExpectedSize(1);
            while (rs.next()) {
                String retName = rs.getString(1);
                int val = rs.getInt(2);
                Assert.assertEquals("Incorrect name returned!", newName, retName);
                results.add(String.format("name:%s,value:%d", retName, val));
            }
            Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
        } finally {
            indexWatcher.drop();
        }

    }

    @Test(timeout = 10000, expected = SQLException.class)
    public void testUniqueInTableCreationViolatesPrimaryKey() throws Exception {
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)", this.getTableReference(TABLE_NAME_8), name, value));
        try {
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)", this.getTableReference(TABLE_NAME_8), name, value));
        } catch (SQLException se) {
            Assert.assertEquals("Incorrect SQL State returned", SQLState.LANG_DUPLICATE_KEY_CONSTRAINT, se.getSQLState());
            Assert.assertTrue(se.getMessage().contains("identified by 'FOO' defined on 'H'"));
            throw se;
        }
        Assert.fail("Did not report a duplicate key violation!");
    }

}
