package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

/**
 * @author Jeff Cunningham
 *         Date: 1/13/14
 */
public class UniqueConstraintIT extends SpliceUnitTest {
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = UniqueConstraintIT.class.getSimpleName().toUpperCase();

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static List<String> tableNames = Arrays.asList("ZONING1", "ZONING2", "ZONING3", "ZONING4");
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
    public void testAlterTableCreateUniqueConstraintWithDuplicate() throws Exception {
        // PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(0);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testAlterTableCreateUniqueConstraint"));

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
     * Should not be able to insert a record with a duplicate key after unique constraint added
     * @throws Exception
     */
    @Test
    public void testAlterTableCreateUniqueConstraintInsertDupe() throws Exception {
        // PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE

        String tableName = tableNames.get(1);
        MyWatcher tableWatcher =
                new MyWatcher(tableName,CLASS_NAME,"(PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testAlterTableCreateUniqueConstraint"));

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

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testAlterTableCreateUniqueConstraint"));
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

        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testAlterTableCreateUniqueConstraint"));
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
