package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.PatternFilenameFilter;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;

import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

/**
 * This IT assumes the server side writes to the local files system accessible to IT itself.  Currently true only
 * because SpliceTestPlatform starts a cluster than uses local FS instead of HDFS, may not be in the future.
 */
@Ignore
public class ExportOperationIT {

    private static final String CLASS_NAME = ExportOperationIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher SCHEMA_WATCHER = new SpliceSchemaWatcher(CLASS_NAME);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void export() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table export_test(\n" +
                        "a smallint,\n" +
                        "b integer,\n" +
                        "c bigint,\n" +
                        "d real,\n" +
                        "e double,\n" +
                        "f decimal,\n" +
                        "g DECIMAL(11, 2),\n" +
                        "h varchar(20),\n" +
                        "i char,\n" +
                        "j date,\n" +
                        "k time\n" +
                        ")")
                .withInsert("insert into export_test values(?,?,?,?,?,?,?,?,?,?,?)")
                .withRows(
                        rows(
                                row(25, 1000000000, 2000000000000000L, 3.14159, 3.14159, 2.1, 2.3423423423, "varchar", "c", "2014-10-01", "14:30:20"),
                                row(26, 1000000000, 2000000000000000L, 3.14159, 3.14159, 2.1, 2.3423423423, "varchar", "c", "2014-10-01", "14:30:20"),
                                row(27, 1000000000, 2000000000000000L, 3.14159, 3.14159, 2.1, 2.3423423423, "varchar", "c", "2014-10-01", "14:30:20"),
                                row(28, 1000000000, 2000000000000000L, 3.14159, 3.14159, 2.1, 2.3423423423, "varchar", "c", "2014-10-01", "14:30:20"),
                                row(29, 1000000000, 2000000000000000L, 3.14159, 3.14159, 2.1, 2.3423423423, "varchar", "c", "2014-10-01", "14:30:20"),
                                row(30, 1000000000, 2000000000000000L, 3.14159, 3.14159, 2.1, 2.3423423423, "varchar", "c", "2014-10-01", "14:30:20"),
                                row(31, 1000000000, 2000000000000000L, 3.14159, 3.14159, 2.1, 2.3423423423, "varchar", "c", "2014-10-01", "14:30:20"),
                                row(32, 1000000000, 2000000000000000L, 3.14159, 3.14159, 2.1, 2.3423423423, "varchar", "c", "2014-10-01", "14:30:20")
                        )
                ).create();

        String exportSQL = buildExportSQL("select * from export_test");

        exportAndAssertExportResults(exportSQL, 8);
        File[] files = temporaryFolder.getRoot().listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "25,1000000000,2000000000000000,3.14159,3.14159,2,2.34,varchar,c,2014-10-01,14:30:20\n" +
                        "26,1000000000,2000000000000000,3.14159,3.14159,2,2.34,varchar,c,2014-10-01,14:30:20\n" +
                        "27,1000000000,2000000000000000,3.14159,3.14159,2,2.34,varchar,c,2014-10-01,14:30:20\n" +
                        "28,1000000000,2000000000000000,3.14159,3.14159,2,2.34,varchar,c,2014-10-01,14:30:20\n" +
                        "29,1000000000,2000000000000000,3.14159,3.14159,2,2.34,varchar,c,2014-10-01,14:30:20\n" +
                        "30,1000000000,2000000000000000,3.14159,3.14159,2,2.34,varchar,c,2014-10-01,14:30:20\n" +
                        "31,1000000000,2000000000000000,3.14159,3.14159,2,2.34,varchar,c,2014-10-01,14:30:20\n" +
                        "32,1000000000,2000000000000000,3.14159,3.14159,2,2.34,varchar,c,2014-10-01,14:30:20\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    @Test
    public void export_defaultDelimiter() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table export_local(a smallint,b double, c time,d varchar(20))")
                .withInsert("insert into export_local values(?,?,?,?)")
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL("select * from export_local", false);

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.getRoot().listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "25,3.14159,14:31:20,varchar1\n" +
                        "26,3.14159,14:31:20,varchar1\n" +
                        "27,3.14159,14:31:20,varchar1 space\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\"\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\"\n" +
                        "30,3.14159,14:31:20,varchar1\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    @Test
    public void export_withAlternateRecordDelimiter() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table pipe(a smallint,b double, c time,d varchar(20))")
                .withInsert("insert into pipe values(?,?,?,?)")
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL("select * from pipe", false, "|");

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.getRoot().listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "25|3.14159|14:31:20|varchar1\n" +
                        "26|3.14159|14:31:20|varchar1\n" +
                        "27|3.14159|14:31:20|varchar1 space\n" +
                        "28|3.14159|14:31:20|varchar1 , comma\n" +
                        "29|3.14159|14:31:20|\"varchar1 \"\" quote\"\n" +
                        "30|3.14159|14:31:20|varchar1\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    @Test
    public void export_withTabs() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table tabs(a smallint,b double, c time,d varchar(20))")
                .withInsert("insert into tabs values(?,?,?,?)")
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL("select * from tabs", false, "\\t");

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.getRoot().listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "25\t3.14159\t14:31:20\tvarchar1\n" +
                        "26\t3.14159\t14:31:20\tvarchar1\n" +
                        "27\t3.14159\t14:31:20\tvarchar1 space\n" +
                        "28\t3.14159\t14:31:20\tvarchar1 , comma\n" +
                        "29\t3.14159\t14:31:20\t\"varchar1 \"\" quote\"\n" +
                        "30\t3.14159\t14:31:20\tvarchar1\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    @Test
    public void exportEmptyTableDoesNotBlowup() throws Exception {
        methodWatcher.executeUpdate("create table empty (a int)");
        String exportSQL = buildExportSQL("select * from empty");
        exportAndAssertExportResults(exportSQL, 0);
    }

    @Test
    public void exportOverFiveTableJoin() throws Exception {

        TableCreator tc =
                new TableCreator(methodWatcher.getOrCreateConnection())
                        .withCreate("create table %s (a int, b int, c int)")
                        .withInsert("insert into %s values(?,?,?)")
                        .withRows(rows(row(1, 2, 3), row(4, 5, 6), row(7, 8, 9)));

        tc.withTableName("a").create();
        tc.withTableName("b").create();
        tc.withTableName("c").create();
        tc.withTableName("d").create();
        tc.withTableName("e").create();

        String exportSQL = buildExportSQL("select * from a cross join b cross join c cross join d cross join e");

        exportAndAssertExportResults(exportSQL, 243);
    }

    @Test
    public void exportWithJoinsProjectionsAndRestrictions() throws Exception {

        TableCreator tc =
                new TableCreator(methodWatcher.getOrCreateConnection())
                        .withCreate("create table %s (c1 int, c2 int, c3 int)")
                        .withInsert("insert into %s values(?,?,?)")
                        .withRows(rows(row(1, 1, 1), row(2, 2, 2), row(3, 3, 3), row(4, 4, 4), row(5, 5, 5)));

        tc.withTableName("aa").create();
        tc.withTableName("bb").create();

        String exportSQL = buildExportSQL("" +
                "select aa.c1,aa.c2*100,bb.c2*300,bb.c3 " +
                "from aa " +
                "join bb on aa.c1 =bb.c1 " +
                "where bb.c3 > 2");

        exportAndAssertExportResults(exportSQL, 3);
    }

    @Test
    public void export_compressed() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table export_compressed(a smallint,b double, c time,d varchar(20))")
                .withInsert("insert into export_compressed values(?,?,?,?)")
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL("select * from export_compressed", true);

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.getRoot().listFiles(new PatternFilenameFilter(".*csv.gz"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "25,3.14159,14:31:20,varchar1\n" +
                        "26,3.14159,14:31:20,varchar1\n" +
                        "27,3.14159,14:31:20,varchar1 space\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\"\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\"\n" +
                        "30,3.14159,14:31:20,varchar1\n",
                IOUtils.toString(new GZIPInputStream(new FileInputStream(files[0]))));
    }


    /* It is important that we throw SQLException, given invalid parameters, rather than other exceptions which cause IJ to drop the connection.  */
    @Test
    public void export_throwsSQLException_givenBadArguments() throws Exception {
        // export path
        try {
            methodWatcher.executeQuery("export('', false, null,null,null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'export path'=''.", e.getMessage());
        }

        // encoding
        try {
            methodWatcher.executeQuery("export('/tmp/', false, 1,'BAD_ENCODING',null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'encoding'='BAD_ENCODING'.", e.getMessage());
        }

        // field delimiter
        try {
            methodWatcher.executeQuery("export('/tmp/', false, 1,'utf-8','AAA', null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'field delimiter'='AAA'.", e.getMessage());
        }

        // quote character
        try {
            methodWatcher.executeQuery("export('/tmp/', false, 1,'utf-8',',', 'BBB') select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'quote character'='BBB'.", e.getMessage());
        }

        // no permission to create export dir
        try {
            methodWatcher.executeQuery("export('/ExportOperationIT/', false, 1,'utf-8',null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'cannot create export directory'='/ExportOperationIT/'.", e.getMessage());
        }
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private String buildExportSQL(String selectQuery) {
        return buildExportSQL(selectQuery, false);
    }

    private String buildExportSQL(String selectQuery, Boolean compression) {
        return buildExportSQL(selectQuery, compression, ",");
    }

    private String buildExportSQL(String selectQuery, Boolean compression, String fieldDelimiter) {
        String exportPath = temporaryFolder.getRoot().getAbsolutePath();
        return String.format("EXPORT('%s', %s, 3, NULL, '%s', NULL)", exportPath, compression, fieldDelimiter) + " " + selectQuery;
    }

    private void exportAndAssertExportResults(String exportSQL, long expectedExportRowCount) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(exportSQL);
        assertTrue(resultSet.next());
        long exportedRowCount = resultSet.getLong(1);
        long exportTimeMs = resultSet.getLong(2);
        assertEquals(expectedExportRowCount, exportedRowCount);
        assertTrue(exportTimeMs >= 0);
    }

    private Iterable<Iterable<Object>> getTestRows() {
        return rows(
                row(25, 3.14159, "14:31:20", "varchar1"),
                row(26, 3.14159, "14:31:20", "varchar1"),
                row(27, 3.14159, "14:31:20", "varchar1 space"),
                row(28, 3.14159, "14:31:20", "varchar1 , comma"),
                row(29, 3.14159, "14:31:20", "varchar1 \" quote"),
                row(30, 3.14159, "14:31:20", "varchar1")
        );
    }

}
