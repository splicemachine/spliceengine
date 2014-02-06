package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;

/**
 * @author Jeff Cunningham
 *         Date: 1/13/14
 */
public class UniqueConstraintIT extends SpliceUnitTest {
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = UniqueConstraintIT.class.getSimpleName().toUpperCase();

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static List<String> tableNames =
            Arrays.asList("ZONING1", "ZONING2", "ZONING3", "ZONING4", "ZONING5", "ZONING6", "ZONING7");
    @Before
    public void beforeTests() throws Exception {
        for (String tableName : tableNames) {
            SpliceTableWatcher.executeDrop(CLASS_NAME, tableName);
        }
    }

    /**
     * Bug DB-552
     * Should not be able to alter table to create a unique constraint with non-unique values already in column
     * @throws Exception
     */
    @Test
    public void testNotNullAlterTableCreateUniqueConstraintWithDuplicate() throws Exception {
        // PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(0);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testNotNullAlterTableCreateUniqueConstraintWithDuplicate"));

        methodWatcher.getStatement().execute(format("CREATE UNIQUE INDEX %s ON %s (PARCELID, HEARDATE)",
                this.getTableReference(tableName), this.getTableReference(tableName)));

        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')",
                this.getTableReference(tableName)));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')",
                this.getTableReference(tableName)));

        // expect exception because there are non-unique rows
        try {
            methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)",
                    this.getTableReference(tableName), tableName+"_UC"));
            Assert.fail("Expected exception - attempt to create a unique constraint on a table with duplicates.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            Assert.assertTrue(e.getLocalizedMessage().contains("would have caused a duplicate key value in a unique or primary key constraint or unique index identified"));
            Assert.assertTrue(e.getLocalizedMessage().contains(tableName));
        }

        TestUtils.tableLookupByNumber(methodWatcher);

        // Prints the index (unique constraint) info
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, CLASS_NAME, tableName, false, false);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        // No exception. We couldn't create the unique constraint so this insert works
        methodWatcher.getStatement().execute(format("insert into %s values (1,'220 BOLYSTON','COND','M-1','M-8','2000-04-12')",
                this.getTableReference(tableName)));
    }

    /**
     * Bug DB-552
     * Should not be able to alter table to create a unique constraint with non-unique values already in column
     * The only difference between this test and the one immediately above is that the unique index column
     * is NOT created with NOT NULL criteria.
     * @throws Exception
     */
    @Test
    public void testAlterTableCreateUniqueConstraintWithDuplicate() throws Exception {
        // PARCELID INTEGER, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(0);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testAlterTableCreateUniqueConstraintWithDuplicate"));

        methodWatcher.getStatement().execute(format("CREATE UNIQUE INDEX %s ON %s (PARCELID, HEARDATE)",
                                                           this.getTableReference(tableName), this.getTableReference(tableName)));

        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')",
                                                           this.getTableReference(tableName)));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')",
                                                           this.getTableReference(tableName)));

        String query = format("select * from %s", this.getTableReference(tableName));
        ResultSet rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        // expect exception because there are non-unique rows
        try {
            methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)",
                                                               this.getTableReference(tableName), tableName+"_UC"));
            // Prints the index (unique constraint) info
            rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, CLASS_NAME, tableName, false, false);
            fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
            System.out.println(fr.toString());
            Assert.fail("Expected exception - attempt to create a unique constraint on a table with duplicates.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            Assert.assertTrue(e.getLocalizedMessage().contains("would have caused a duplicate key value in a unique or primary key constraint or unique index identified"));
            Assert.assertTrue(e.getLocalizedMessage().contains(tableName));
        }

        TestUtils.tableLookupByNumber(methodWatcher);

        // Prints the index (unique constraint) info
        rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, CLASS_NAME, tableName, false, false);
        fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        // No exception. We couldn't create the unique constraint so this insert works
        methodWatcher.getStatement().execute(format("insert into %s values (1,'220 BOLYSTON','COND','M-1','M-8','2000-04-12')",
                                                           this.getTableReference(tableName)));
    }

    /**
     * Bug DB-552
     * Should be able to alter table to create a unique constraint with non-unique NULL values already in column
     * Table NOT created with NOT NULL criteria.
     * @throws Exception
     */
    @Test
    public void testAlterTableCreateUniqueConstraintWithDuplicateNulls() throws Exception {
        // PARCELID INTEGER, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(6);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME,
                                                               "testAlterTableCreateUniqueConstraintWithDuplicateNulls"));

        methodWatcher.getStatement().execute(format("insert into %s values (NULL,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')",
                                                           this.getTableReference(tableName)));
        methodWatcher.getStatement().execute(format("insert into %s values (NULL,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')",
                                                           this.getTableReference(tableName)));

        TestUtils.tableLookupByNumber(methodWatcher);

        String query = format("select * from %s", this.getTableReference(tableName));
        ResultSet rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        // expect exception because there are non-unique rows
        try {
            methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)",
                                                               this.getTableReference(tableName), tableName+"_UC"));
        } catch (Exception e) {
            Assert.fail("Expected to create unique constraint on table with duplicate null values but got an exception: "+e.getLocalizedMessage());
        }
        // Prints the index (unique constraint) info
        rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, CLASS_NAME, tableName, false, false);
        fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s values (NULL,'551 BOLYSTON','COND','M-1','M-8','2000-04-12')",
                                                           this.getTableReference(tableName)));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','2000-04-12')",
                                                    this.getTableReference(tableName)));
        methodWatcher.getStatement().execute(format("insert into %s values (2,'550 BOLYSTON','COND','M-1','M-8','2000-04-12')",
                                                    this.getTableReference(tableName)));

        // Now query result should be 5 rows
        rs = methodWatcher.getStatement().executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());
        Assert.assertEquals("Wrong number of rows after insert.", 5, fr.size());

        // try to insert a non-null duplicate
        try {
            methodWatcher.getStatement().execute(format("insert into %s values (1,'552 BOLYSTON','COND','M-1','M-8','2000-04-13')",
                                                        this.getTableReference(tableName)));
        } catch (Exception e) {
            Assert.assertTrue(e.getLocalizedMessage(), e instanceof SQLException);
            Assert.assertTrue(e.getLocalizedMessage(), e.getLocalizedMessage().contains("would have caused a duplicate key value in a unique or primary key constraint or unique index identified"));
            Assert.assertTrue(e.getLocalizedMessage(), e.getLocalizedMessage().contains(tableName));
            // expected
        }

        // now delete one of the rows with null value in index column and make sure it's gone from index too
        String delete = format("delete from %s where ADDRESS = '551 BOLYSTON'", this.getTableReference(tableName));
        boolean success = methodWatcher.getStatement().execute(delete);

        // Now query result should be 4 rows
        rs = methodWatcher.getStatement().executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());
        Assert.assertEquals("Wrong number of rows after delete.", 4, fr.size());
    }

    /**
     * Bug DB-552
     * Should not be able to insert a record with a duplicate key after unique constraint added
     * @throws Exception
     */
    @Test
    public void testAlterTableCreateUniqueConstraintInsertDupe() throws Exception {
        // PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(1);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testAlterTableCreateUniqueConstraintInsertDupe"));

        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')",
                this.getTableReference(tableName)));

        TestUtils.tableLookupByNumber(methodWatcher);

        String query = format("select * from %s", this.getTableReference(tableName));
        ResultSet rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)",
                this.getTableReference(tableName), tableName + "_UC"));

        TestUtils.tableLookupByNumber(methodWatcher);

        // Prints the index (unique constraint) info
        rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, CLASS_NAME, tableName, false, false);
        fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        // expect exception
        try {
            methodWatcher.getStatement().execute(format("insert into %s values (1,'220 BOLYSTON','COND','M-1','M-8','1989-04-12')",
                    this.getTableReference(tableName)));
        } catch (Exception e) {
            Assert.assertTrue(e.getLocalizedMessage(), e instanceof SQLException);
            Assert.assertTrue(e.getLocalizedMessage(), e.getLocalizedMessage().contains("would have caused a duplicate key value in a unique or primary key constraint or unique index identified"));
            Assert.assertTrue(e.getLocalizedMessage(), e.getLocalizedMessage().contains(tableName));
            // expected
            return;
        }
        rs = methodWatcher.getStatement().executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        Assert.fail("Expected duplicate key value exception.");
    }

    /**
     * Bug DB-552 - This duplicates the reproducing test defined in the bug.
     * Should not be able to alter table add constraint when non unique rows exist.
     * The difference between this test and the one above immediately above is that the
     * alter table column is NOT defined with NOT NULL criteria.
     * @throws Exception
     */
    @Test
    public void testCreateUniqueIndexAlterTableCreateUniqueConstraint() throws Exception {
        // PARCELID INTEGER, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(4);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testCreateUniqueIndexAlterTableCreateUniqueConstraint"));

        methodWatcher.getStatement().execute(format("CREATE UNIQUE INDEX %s ON %s (PARCELID, HEARDATE)",
                tableName + "_UI", this.getTableReference(tableName)));
        TestUtils.tableLookupByNumber(methodWatcher);

        // Prints the index (unique constraint) info
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, CLASS_NAME, tableName, false, false);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')",
                this.getTableReference(tableName)));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')",
                                                           this.getTableReference(tableName)));

        String query = format("select * from %s", this.getTableReference(tableName));
        rs = methodWatcher.getStatement().executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        try {
            methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)",
                    this.getTableReference(tableName), tableName + "_UC"));
        } catch (Exception e) {
            Assert.assertTrue(e.getLocalizedMessage(), e instanceof SQLException);
            Assert.assertTrue(e.getLocalizedMessage(), e.getLocalizedMessage().contains("would have caused a duplicate key value in a unique or primary key constraint or unique index identified"));
            Assert.assertTrue(e.getLocalizedMessage(), e.getLocalizedMessage().contains(tableName));
            // expected
            return;
        }

        Assert.fail("Expected exception - attempt to create a unique constraint on a table with duplicates.");
    }

    /**
     * Bug DB-552
     * Should not be able to alter table add constraint when non unique rows exist.<br/>
     * The difference between this test and the one above immediately above is that the
     * alter table column is defined as NOT NULL.
     * @throws Exception
     */
    @Test
    public void tesNotNulltCreateUniqueIndexAlterTableCreateUniqueConstraint() throws Exception {
        // PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(5);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "tesNotNulltCreateUniqueIndexAlterTableCreateUniqueConstraint"));

        methodWatcher.getStatement().execute(format("CREATE UNIQUE INDEX %s ON %s (PARCELID, HEARDATE)",
                tableName + "_UI", this.getTableReference(tableName)));
        TestUtils.tableLookupByNumber(methodWatcher);

        // Prints the index (unique constraint) info
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, CLASS_NAME, tableName, false, false);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')",
                this.getTableReference(tableName)));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')",
                this.getTableReference(tableName)));

        String query = format("select * from %s", this.getTableReference(tableName));
        rs = methodWatcher.getStatement().executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        try {
            methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)",
                    this.getTableReference(tableName), tableName + "_UC"));
        } catch (Exception e) {
            Assert.assertTrue(e.getLocalizedMessage(), e instanceof SQLException);
            Assert.assertTrue(e.getLocalizedMessage(), e.getLocalizedMessage().contains("would have caused a duplicate key value in a unique or primary key constraint or unique index identified"));
            Assert.assertTrue(e.getLocalizedMessage(), e.getLocalizedMessage().contains(tableName));
            // expected
            return;
        }

        Assert.fail("Expected exception - attempt to create a unique constraint on a table with duplicates.");
    }

    /**
     * Bug DB-552
     * Alter table created with unique constraint attempting to add same constraint
     * @throws Exception
     */
    @Test
    public void testCreateTableCreateUniqueConstraintAttemptToAddConstraintAgain() throws Exception {
        // PARCELID INTEGER UNIQUE NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(2);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER UNIQUE NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testCreateTableCreateUniqueConstraintAttemptToAddConstraintAgain"));
        TestUtils.tableLookupByNumber(methodWatcher);

        // Prints the index (unique constraint) info
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, CLASS_NAME, tableName, false, false);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        // expect exception
        try {
            methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)",
                    this.getTableReference(tableName), tableName+"_UC"));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            Assert.assertTrue(e.getLocalizedMessage().contains("have the same set of columns, which is not allowed."));
            Assert.assertTrue(e.getLocalizedMessage().contains(tableName));
            // expected
            return;
        }
        Assert.fail("Expected duplicate key value exception.");
    }

    /**
     * Bug DB-552
     * Control - table created with unique constraint and constraint honored when attempt to add dup is made
     * @throws Exception
     */
    @Test
    public void testCreateTableCreateUniqueConstraint() throws Exception {
        // PARCELID INTEGER UNIQUE NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(3);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER UNIQUE NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testCreateTableCreateUniqueConstraint"));
        TestUtils.tableLookupByNumber(methodWatcher);

        // Prints the index (unique constraint) info
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, CLASS_NAME, tableName, false, false);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')",
                this.getTableReference(tableName)));

        // expect exception
        try {
            methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')",
                    this.getTableReference(tableName)));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            Assert.assertTrue(e.getLocalizedMessage().contains("would have caused a duplicate key value in a unique or primary key constraint or unique index identified"));
            Assert.assertTrue(e.getLocalizedMessage().contains(tableName));
            // expected
            return;
        }
        Assert.fail("Expected duplicate key value exception.");
    }

    static class MyWatcher extends SpliceTableWatcher {

        public MyWatcher(String tableName, String schemaName, String createString) {
            super(tableName, schemaName, createString);
        }

        public void create(Description desc) {
            super.starting(desc);
        }
    }

}
