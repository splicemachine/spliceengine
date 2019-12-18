/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.spark_project.guava.base.Charsets;
import org.spark_project.guava.io.Files;
import org.spark_project.guava.io.PatternFilenameFilter;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;
import org.junit.Assert;
import org.xerial.snappy.SnappyInputStream;

/**
 * This IT assumes the server side writes to the local files system accessible to IT itself.  Currently true only
 * because SpliceTestPlatform starts a cluster than uses local FS instead of HDFS, may not be in the future.
 */
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

        TestConnection conn=methodWatcher.getOrCreateConnection();
        new TableCreator(conn)
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

        String exportSQL = buildExportSQL("select * from export_test order by a");

        exportAndAssertExportResults(exportSQL,8);
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

        try(CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
            cs.setString(1,SCHEMA_WATCHER.schemaName);
            cs.execute();
        }

    }

    @Test
    public void export_defaultDelimiter() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table export_local(a smallint,b double, c time,d varchar(20))")
                .withInsert("insert into export_local values(?,?,?,?)")
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL("select * from export_local order by a asc", "None");

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

        String exportSQL = buildExportSQL("select * from pipe order by a asc", "NONE", "|");

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

        String exportSQL = buildExportSQL("select * from tabs order by a asc", " none ", "\\t");

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
    public void export_compressed_bz2() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table export_compressed_bz2(a smallint,b double, c time,d varchar(20))")
                .withInsert("insert into export_compressed_bz2 values(?,?,?,?)")
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL("select * from export_compressed_bz2 order by a asc", "BZ2");

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.getRoot().listFiles(new PatternFilenameFilter(".*csv.bz2"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "25,3.14159,14:31:20,varchar1\n" +
                        "26,3.14159,14:31:20,varchar1\n" +
                        "27,3.14159,14:31:20,varchar1 space\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\"\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\"\n" +
                        "30,3.14159,14:31:20,varchar1\n",
                IOUtils.toString(new BZip2CompressorInputStream(new FileInputStream(files[0]))));
    }

    @Test
    public void export_compressed_gz() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table export_compressed_gz(a smallint,b double, c time,d varchar(20))")
                .withInsert("insert into export_compressed_gz values(?,?,?,?)")
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL("select * from export_compressed_gz order by a asc", "GZIP");

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

    @Test
    public void export_compressed_gz2() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table export_compressed_gz2(a smallint,b double, c time,d varchar(20))")
                .withInsert("insert into export_compressed_gz2 values(?,?,?,?)")
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL("select * from export_compressed_gz2 order by a asc", true);

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

    @Test
    public void export_decimalFormatting() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table export_decimal(a smallint, b decimal(31, 25), c decimal(31, 2))")
                .withInsert("insert into export_decimal values(?,?,?)")
                .withRows(rows(row(1, 2000.0, 3.00005), row(1, 2000.0, 3.00005))).create();

        //
        // default column order
        //
        String exportSQL = buildExportSQL("select * from export_decimal order by a asc", null);

        exportAndAssertExportResults(exportSQL, 2);
        File[] files = temporaryFolder.getRoot().listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "1,2000.0000000000000000000000000,3.00\n" +
                        "1,2000.0000000000000000000000000,3.00\n",
                Files.toString(files[0], Charsets.UTF_8));

        //
        // alternate column order
        //
        FileUtils.deleteDirectory(temporaryFolder.getRoot());
        exportSQL = buildExportSQL("select c,b,a from export_decimal order by a asc", "NONE");

        exportAndAssertExportResults(exportSQL, 2);
        files = temporaryFolder.getRoot().listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "3.00,2000.0000000000000000000000000,1\n" +
                        "3.00,2000.0000000000000000000000000,1\n",
                Files.toString(files[0], Charsets.UTF_8));

        //
        // column subset
        //
        FileUtils.deleteDirectory(temporaryFolder.getRoot());
        exportSQL = buildExportSQL("select b from export_decimal order by a asc", "NONE");

        exportAndAssertExportResults(exportSQL, 2);
        files = temporaryFolder.getRoot().listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "2000.0000000000000000000000000\n" +
                        "2000.0000000000000000000000000\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    /* sysaliases contains a column of type 'com.splicemachine.db.catalog.AliasInfo'. Make sure we can handle with out throwing.  */
    @Test
    public void export_sysaliases() throws Exception {
        Long expectedRowCount = methodWatcher.query("select count(*) from sys.sysaliases");
        String exportSQL = buildExportSQL("select * from sys.sysaliases", "none");
        exportAndAssertExportResults(exportSQL, expectedRowCount);
    }

    /* It is important that we throw SQLException, given invalid parameters, rather than other exceptions which cause IJ to drop the connection.  */
    @Test
    public void export_throwsSQLException_givenBadArguments() throws Exception {
        // export path
        try {
            methodWatcher.executeQuery("export('', 'None', null,null,null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'export path'=''.", e.getMessage());
        }

        // encoding
        try {
            methodWatcher.executeQuery("export('/tmp/', null, 1,'BAD_ENCODING',null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'encoding'='BAD_ENCODING'.", e.getMessage());
        }

        // field delimiter
        try {
            methodWatcher.executeQuery("export('/tmp/', null, 1,'utf-8','AAA', null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'field delimiter'='AAA'.", e.getMessage());
        }

        // quote character
        try {
            methodWatcher.executeQuery("export('/tmp/', null, 1,'utf-8',',', 'BBB') select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'quote character'='BBB'.", e.getMessage());
        }

        // no permission to create export dir
        try {
            methodWatcher.executeQuery("export('/ExportOperationIT/', null, 1,'utf-8',null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'cannot create export directory'='/ExportOperationIT/'.", e.getMessage());
        }

        // wrong replica count
        try {
            methodWatcher.executeQuery("export('/tmp/', null, -100, null, null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", "XIE0U", e.getSQLState());
        }

        // wrong field separator
        try {
            methodWatcher.executeQuery("export('/tmp/', null, null, null, 10, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", "XIE0X", e.getSQLState());
            assertEquals("Invalid error message", "The export operation was not performed, because value of the specified parameter (10) is wrong.", e.getMessage());
        }

        // wrong quote character
        try {
            methodWatcher.executeQuery("export('/tmp/', null, null, null, null, 100) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", "XIE0X", e.getSQLState());
            assertEquals("Invalid error message", "The export operation was not performed, because value of the specified parameter (100) is wrong.", e.getMessage());
        }

        // wrong replication parameter
        try {
            methodWatcher.executeQuery("export('/tmp/', null, 'a', null, null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", "XIE0X", e.getSQLState());
            assertEquals("Invalid error message", "The export operation was not performed, because value of the specified parameter (a) is wrong.", e.getMessage());
        }

        // wrong compression format
        try {
            methodWatcher.executeQuery("export('/tmp/', 'WrongFormat', 1, null, null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", "XIE13", e.getSQLState());
            assertEquals("Invalid error message", "Unsuported compression format: WRONGFORMAT", e.getMessage());
        }
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private String buildExportSQL(String selectQuery) {
        return buildExportSQL(selectQuery, null);
    }

    private String buildExportSQL(String selectQuery, String compression) {
        return buildExportSQL(selectQuery, compression, ",");
    }

    private String buildExportSQL(String selectQuery, boolean compression) {
        return buildExportSQL(selectQuery, compression, ",");
    }

    private String buildExportSQL(String selectQuery, String compression, String fieldDelimiter) {
        String exportPath = temporaryFolder.getRoot().getAbsolutePath();
        if (compression == null) {
            return String.format("EXPORT('%s', false, 3, NULL, '%s', NULL)", exportPath, fieldDelimiter) + " " + selectQuery;
        }
        else {
            return String.format("EXPORT('%s', '%s', 3, NULL, '%s', NULL)", exportPath, compression, fieldDelimiter) + " " + selectQuery;
        }
    }

    private String buildExportSQL(String selectQuery, boolean compression, String fieldDelimiter) {
        String exportPath = temporaryFolder.getRoot().getAbsolutePath();
        return String.format("EXPORT('%s', %s, 3, NULL, '%s', NULL)", exportPath, compression, fieldDelimiter) + " " + selectQuery;
    }

    private void exportAndAssertExportResults(String exportSQL, long expectedExportRowCount) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(exportSQL);
        assertTrue(resultSet.next());
        long exportedRowCount = resultSet.getLong(1);
//        long exportTimeMs = resultSet.getLong(2);
        assertEquals(expectedExportRowCount, exportedRowCount);
//        assertTrue(exportTimeMs >= 0);
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

    @Test
    public void exportExceptionsS3() throws Exception {
        try {
            new TableCreator(methodWatcher.getOrCreateConnection())
                    .withCreate("create table export_s3_test (c1 int, c2 int, c3 int)")
                    .withInsert("insert into export_s3_test values(?,?,?)")
                    .withRows(rows(row(1, 1, 1), row(2, 2, 2), row(3, 3, 3), row(4, 4, 4), row(5, 5, 5)))
                    .create();

            Long expectedRowCount = methodWatcher.query("EXPORT ( 's3a://molitorisspechial/temp/', null, null, null, null, null ) select * from export_s3_test");
            fail();
        } catch (SQLException sqle) {
            String sqlState = sqle.getSQLState();
            Assert.assertTrue(sqlState, sqlState.compareToIgnoreCase("EXT26") == 0 ||
                    sqlState.compareToIgnoreCase("XCZ02") == 0 ||
                    sqlState.compareToIgnoreCase("XJ001") == 0);
        }
    }

}
