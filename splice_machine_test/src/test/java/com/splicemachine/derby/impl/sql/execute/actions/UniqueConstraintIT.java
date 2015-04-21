package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.junit.Assert.*;

public class UniqueConstraintIT {

    private static final String SCHEMA = UniqueConstraintIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    /**
     * Bug DB-552
     * Should not be able to alter table to create a unique constraint with non-unique values already in column
     */
    @Test @Ignore("DB-1755: alter table. Need unique constraints for this test.")
    public void testNotNullAlterTableCreateUniqueConstraintWithDuplicate() throws Exception {
        methodWatcher.executeUpdate("create table ZONING0 (PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");
        methodWatcher.getStatement().execute("CREATE UNIQUE INDEX ZONING0 ON ZONING0 (PARCELID, HEARDATE)");
        methodWatcher.getStatement().execute("insert into ZONING0 values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')");
        methodWatcher.getStatement().execute("insert into ZONING0 values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')");

        assertSqlFails("alter table ZONING0 add constraint ZONING0_UC unique(PARCELID)",
                "would have caused a duplicate key value in a unique or primary key constraint or unique index identified",
                "ZONING0");

        // Prints the index (unique constraint) info
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, SCHEMA, "ZONING0", false, false);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        // No exception. We couldn't create the unique constraint so this insert works
        methodWatcher.getStatement().execute("insert into ZONING0 values (1,'220 BOLYSTON','COND','M-1','M-8','2000-04-12')");
    }

    /**
     * Bug DB-552
     * Should not be able to alter table to create a unique constraint with non-unique values already in column
     * The only difference between this test and the one immediately above is that the unique index column
     * is NOT created with NOT NULL criteria.
     */
    @Test @Ignore("DB-1755: alter table. Need unique constraints for this test.")
    public void testAlterTableCreateUniqueConstraintWithDuplicate() throws Exception {
        String tableName = "ZONING1";
        methodWatcher.executeUpdate("create table " + tableName + "(PARCELID INTEGER, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");
        methodWatcher.getStatement().execute(format("CREATE UNIQUE INDEX %s ON %s (PARCELID, HEARDATE)", tableName, tableName));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')", tableName));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')", tableName));

        String query = format("select * from %s", tableName);
        ResultSet rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        // expect exception because there are non-unique rows
        try {
            methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)", tableName, tableName + "_UC"));
            // Prints the index (unique constraint) info
            rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, SCHEMA, tableName, false, false);
            fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
            System.out.println(fr.toString());
            fail("Expected exception - attempt to create a unique constraint on a table with duplicates.");
        } catch (SQLException e) {
            assertTrue(e.getLocalizedMessage().contains("would have caused a duplicate key value in a unique or primary key constraint or unique index identified"));
            assertTrue(e.getLocalizedMessage().contains(tableName));
        }

        // Prints the index (unique constraint) info
        rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, SCHEMA, tableName, false, false);
        fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        // No exception. We couldn't create the unique constraint so this insert works
        methodWatcher.getStatement().execute(format("insert into %s values (1,'220 BOLYSTON','COND','M-1','M-8','2000-04-12')", tableName));
    }

    /**
     * Bug DB-552
     * Should be able to alter table to create a unique constraint with non-unique NULL values already in column
     * Table NOT created with NOT NULL criteria.
     */
    @Test @Ignore("DB-1755: alter table. Need unique constraints for this test.")
    public void testAlterTableCreateUniqueConstraintWithDuplicateNulls() throws Exception {
        String tableName = "ZONING7";
        methodWatcher.executeUpdate("create table " + tableName + "(PARCELID INTEGER, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");
        methodWatcher.getStatement().execute(format("insert into %s values (NULL,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')", tableName));
        methodWatcher.getStatement().execute(format("insert into %s values (NULL,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')", tableName));

        String query = format("select * from %s", tableName);
        ResultSet rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        // expect exception because there are non-unique rows
        try {
            methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)", tableName, tableName + "_UC"));
        } catch (Exception e) {
            fail("Expected to create unique constraint on table with duplicate null values but got an exception: " + e.getLocalizedMessage());
        }

        // Prints the index (unique constraint) info
        rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, SCHEMA, tableName, false, false);
        fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s values (NULL,'551 BOLYSTON','COND','M-1','M-8','2000-04-12')", tableName));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','2000-04-12')", tableName));
        methodWatcher.getStatement().execute(format("insert into %s values (2,'550 BOLYSTON','COND','M-1','M-8','2000-04-12')", tableName));

        // Now query result should be 5 rows
        rs = methodWatcher.getStatement().executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());
        assertEquals("Wrong number of rows after insert.", 5, fr.size());

        assertSqlFails("insert into ZONING7 values (1,'552 BOLYSTON','COND','M-1','M-8','2000-04-13')",
                "would have caused a duplicate key value in a unique or primary key constraint or unique index identified",
                tableName);

        // now delete one of the rows with null value in index column and make sure it's gone from index too
        String delete = format("delete from %s where ADDRESS = '551 BOLYSTON'", tableName);
        assertFalse(methodWatcher.getStatement().execute(delete));

        // Now query result should be 4 rows
        rs = methodWatcher.getStatement().executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());
        assertEquals("Wrong number of rows after delete.", 4, fr.size());
    }

    /**
     * Bug DB-552
     * Should not be able to insert a record with a duplicate key after unique constraint added
     */
    @Test @Ignore("DB-1755: alter table. Need unique constraints for this test.")
    public void testAlterTableCreateUniqueConstraintInsertDupe() throws Exception {
        String tableName = "ZONING2";
        methodWatcher.executeUpdate("create table " + tableName + "(PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')", tableName));

        String query = format("select * from %s", tableName);
        ResultSet rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("alter table %s add constraint %s unique(PARCELID)", tableName, tableName + "_UC"));

        // Prints the index (unique constraint) info
        rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, SCHEMA, tableName, false, false);
        fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        assertSqlFails("insert into ZONING2 values (1,'220 BOLYSTON','COND','M-1','M-8','1989-04-12')",
                "would have caused a duplicate key value in a unique or primary key constraint or unique index identified",
                tableName);
    }

    /**
     * Bug DB-552 - This duplicates the reproducing test defined in the bug.
     * Should not be able to alter table add constraint when non unique rows exist.
     * The difference between this test and the one above immediately above is that the
     * alter table column is NOT defined with NOT NULL criteria.
     */
    @Test @Ignore("DB-1755: alter table. Need unique constraints for this test.")
    public void testCreateUniqueIndexAlterTableCreateUniqueConstraint() throws Exception {
        String tableName = "ZONING5";
        methodWatcher.executeUpdate("create table " + tableName + "(PARCELID INTEGER, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");
        methodWatcher.getStatement().execute(format("CREATE UNIQUE INDEX %s ON %s (PARCELID, HEARDATE)", tableName + "_UI", tableName));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')", tableName));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')", tableName));

        String query = format("select * from %s", tableName);
        ResultSet rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        assertSqlFails("alter table ZONING5 add constraint ZONING5_UC unique(PARCELID)",
                "would have caused a duplicate key value in a unique or primary key constraint or unique index identified",
                tableName);
    }

    /**
     * Bug DB-552
     * Should not be able to alter table add constraint when non unique rows exist.<br/>
     * The difference between this test and the one above immediately above is that the
     * alter table column is defined as NOT NULL.
     */
    @Test @Ignore("DB-1755: alter table. Need unique constraints for this test.")
    public void tesNotNullCreateUniqueIndexAlterTableCreateUniqueConstraint() throws Exception {
        String tableName = "ZONING6";
        methodWatcher.executeUpdate("create table " + tableName + "(PARCELID INTEGER NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");
        methodWatcher.getStatement().execute(format("CREATE UNIQUE INDEX %s ON %s (PARCELID, HEARDATE)", tableName + "_UI", tableName));

        // Prints the index (unique constraint) info
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, SCHEMA, tableName, false, false);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')", tableName));
        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')", tableName));

        String query = format("select * from %s", tableName);
        rs = methodWatcher.getStatement().executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        System.out.println(fr.toString());

        assertSqlFails("alter table ZONING6 add constraint ZONING6_UC unique(PARCELID)",
                "would have caused a duplicate key value in a unique or primary key constraint or unique index identified",
                tableName);
    }

    /**
     * Bug DB-552
     * Alter table created with unique constraint attempting to add same constraint
     */
    @Test
    public void testCreateTableCreateUniqueConstraintAttemptToAddConstraintAgain() throws Exception {
        String tableName = "ZONING3";
        methodWatcher.executeUpdate("create table " + tableName + "(PARCELID INTEGER UNIQUE NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        // Prints the index (unique constraint) info
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, SCHEMA, tableName, false, false);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        assertSqlFails("alter table ZONING3 add constraint ZONING3_UC unique(PARCELID)",
                "have the same set of columns, which is not allowed.",
                tableName);
    }

    /**
     * Bug DB-552
     * Control - table created with unique constraint and constraint honored when attempt to add dup is made
     */
    @Test
    public void testCreateTableCreateUniqueConstraint() throws Exception {
        String tableName = "ZONING4";
        methodWatcher.executeUpdate("create table " + tableName + "(PARCELID INTEGER UNIQUE NOT NULL, ADDRESS VARCHAR(15), BOARDDEC VARCHAR(11), EXSZONE VARCHAR(8), PRPZONE VARCHAR(8), HEARDATE DATE)");

        // Prints the index (unique constraint) info
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getIndexInfo(null, SCHEMA, tableName, false, false);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("get table metadata", rs);
        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s values (1,'550 BOLYSTON','COND','M-1','M-4','1989-11-12')", tableName));

        assertSqlFails("insert into ZONING4 values (1,'550 BOLYSTON','COND','M-1','M-8','1989-04-12')",
                "would have caused a duplicate key value in a unique or primary key constraint or unique index identified",
                tableName);
    }

    @Test
    public void uniqueConstraintEnforcedOnUpdate_updateOneRow() throws Exception {
        methodWatcher.executeUpdate("create table ZONING_08 (a int, b int unique)");
        methodWatcher.executeUpdate("insert into ZONING_08 values(1,1),(2,2)");
        assertSqlFails("update ZONING_08 set b=2 where b=1", "The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index", "ZONING_08");
    }

    @Test
    public void uniqueConstraintEnforcedOnUpdate_updateMultipleRows() throws Exception {
        methodWatcher.executeUpdate("create table ZONING_09 (a int, b int unique)");
        methodWatcher.executeUpdate("insert into ZONING_09 values(1,1),(2,2)");
        assertSqlFails("update ZONING_09 set b=3", "The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index", "ZONING_09");
    }

    private void assertSqlFails(String sql, String expectedException, String tableName) {
        try {
            methodWatcher.executeUpdate(sql);
            fail("expected this sql to fail: " + sql);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(expectedException));
            assertTrue(e.getLocalizedMessage().contains(tableName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
