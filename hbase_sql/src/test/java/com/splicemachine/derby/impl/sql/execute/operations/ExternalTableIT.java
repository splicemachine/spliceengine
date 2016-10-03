package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TriggerBuilder;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by jleach on 9/30/16.
 */
public class ExternalTableIT {

    private static final String SCHEMA_NAME = ExternalTableIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private TriggerBuilder tb = new TriggerBuilder();
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Test
    public void testInvalidSyntaxParquet() throws Exception {
        try {
            // Row Format not supported for Parquet
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 int) partitioned by (col1) " +
                    "row format delimited fields terminated by ',' escaped by '\\' " +
                    "lines terminated by '\\n' STORED AS PARQUET LOCATION '/foobar/foobar'");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT01",e.getSQLState());
        }
    }

    @Test
    public void testInvalidSyntaxORC() throws Exception {
        try {
            // Row Format not supported for Parquet
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 int) partitioned by (col1) " +
                    "row format delimited fields terminated by ',' escaped by '\\' " +
                    "lines terminated by '\\n' STORED AS ORC LOCATION '/foobar/foobar'");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT02",e.getSQLState());
        }
    }


    @Test
    public void testStoredAsRequired() throws Exception {
        try {
            // Location Required For Parquet
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 int) LOCATION 'foobar'");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT03",e.getSQLState());
        }
    }

    @Test
    public void testLocationRequired() throws Exception {
        try {
            // Location Required For Parquet
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 int) STORED AS PARQUET");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT04",e.getSQLState());
        }
    }


    @Test
    public void testNoPrimaryKeysOnExternalTables() throws Exception {
        try {
            // Location Required For Parquet
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 int, primary key (col1)) STORED AS PARQUET LOCATION 'HUMPTY_DUMPTY_MOLITOR'");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT06",e.getSQLState());
        }
    }

    @Test
    public void testNoCheckConstraintsOnExternalTables() throws Exception {
        try {
            // Location Required For Parquet
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 int, SALARY DECIMAL(9,2) CONSTRAINT SAL_CK CHECK (SALARY >= 10000)) STORED AS PARQUET LOCATION 'HUMPTY_DUMPTY_MOLITOR'");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT07",e.getSQLState());
        }
    }

    @Test
    public void testNoReferenceConstraintsOnExternalTables() throws Exception {
        try {
            // Location Required For Parquet
            methodWatcher.executeUpdate("create table Cities (col1 int, col2 int, primary key (col1))");
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 int, CITY_ID INT CONSTRAINT city_foreign_key\n" +
                    " REFERENCES Cities) STORED AS PARQUET LOCATION 'HUMPTY_DUMPTY_MOLITOR'");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT08",e.getSQLState());
        }
    }

        @Test
        public void testNoUniqueConstraintsOnExternalTables() throws Exception {
            try {
                // Location Required For Parquet
                methodWatcher.executeUpdate("create external table foo (col1 int, col2 int unique)" +
                        " STORED AS PARQUET LOCATION 'HUMPTY_DUMPTY_MOLITOR'");
                Assert.fail("Exception not thrown");
            } catch (SQLException e) {
                Assert.assertEquals("Wrong Exception","EXT09",e.getSQLState());
            }
        }

    @Test
    public void testNoGenerationClausesOnExternalTables() throws Exception {
        try {
            // Location Required For Parquet
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 varchar(24), col3 GENERATED ALWAYS AS ( UPPER(col2) ))" +
                    " STORED AS PARQUET LOCATION 'HUMPTY_DUMPTY_MOLITOR'");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT10",e.getSQLState());
        }
    }

    @Test
    public void testCannotUpdateExternalTable() throws Exception {
        try {
            methodWatcher.executeUpdate("create external table update_foo (col1 int, col2 varchar(24))" +
                    " STORED AS PARQUET LOCATION 'HUMPTY_DUMPTY_MOLITOR'");
            methodWatcher.executeQuery("update table update_foo set col1 = 4");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT05",e.getSQLState());
        }
    }

    @Test
    public void testCannotDeleteExternalTable() throws Exception {
        try {
            methodWatcher.executeUpdate("create external table update_foo (col1 int, col2 varchar(24))" +
                    " STORED AS PARQUET LOCATION 'HUMPTY_DUMPTY_MOLITOR'");
            methodWatcher.executeQuery("delete from update_foo where col1 = 4");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT05",e.getSQLState());
        }
    }

    @Test
    public void testFileNotFoundORC() {

    }

    @Test
    public void testFileNotFoundTextFile() {

    }

    @Test
    public void testWriteToWrongExistingPartitionsParquet() {

    }

    @Test
    public void testWriteToWrongExistingPartitionsORC() {

    }

    @Test
    public void testWriteToExistingParquetWithPartition() {

    }

    @Test
    public void testWriteToExistingParquetWithoutPartition() {

    }

    @Test
    public void testWriteToExistingORCWithPartition() {

    }

    @Test
    public void testWriteToExistingORCWithoutPartition() {

    }

    @Test
    public void testWriteToExistingTextfile() {

    }

    @Test
    public void testAttemptToDeleteFromExternalTable() {

    }

    @Test
    public void testAttemptToUpdateFromExternalTable() {

    }

    @Test
    public void testAttemptToIndexExternalTable() {

    }

    @Test
    public void testAttemptToPlaceTriggersOnExternalTable() {

    }

    @Test
    public void testAttemptToPlaceForeignKeysOnExternalTable() {

    }

    @Test
    public void testCanDropExternalTable() {

    }

    @Test
    public void testCreateStatsOnParquetTable() {

    }

    @Test
    public void testCreateStatsOnORCTable() {

    }

    @Test
    public void testCreateStatsOnTextFile() {

    }

    @Test
    public void testAlterExternalTableAddColumn() {

    }

    @Test
    public void testAlterExternalTableDropColumn() {

    }

    @Test
    public void testExternalTableInDictionary() {

    }

    @Test
    public void testExternalTableCannotHavePrimaryKey() {

    }

//    @Test
    public void create_illegal_triggersNotAllowedOnSystemTables() throws Exception {
        // Cannot create triggers on external tables.
        verifyTriggerCreateFails(tb.on("SYS.SYSTABLES").named("trig").before().delete().row().then("select * from sys.systables"),
                "'CREATE TRIGGER' is not allowed on the System table '\"SYS\".\"SYSTABLES\"'.");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void createTrigger(TriggerBuilder tb) throws Exception {
        methodWatcher.executeUpdate(tb.build());
    }

    private void verifyTriggerCreateFails(TriggerBuilder tb, String expectedError) throws Exception {
        try {
            createTrigger(tb);
            fail("expected trigger creation to fail for=" + tb.build());
        } catch (Exception e) {
            assertEquals(expectedError, e.getMessage());
        }

    }

}