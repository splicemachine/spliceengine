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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.test.SerialTest;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.base.Charsets;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.io.Files;
import splice.com.google.common.io.PatternFilenameFilter;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.zip.GZIPInputStream;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

/**
 * This IT assumes the server side writes to the local files system accessible to IT itself.  Currently true only
 * because SpliceTestPlatform starts a cluster than uses local FS instead of HDFS, may not be in the future.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class ExportOperationIT {

    private static final String CLASS_NAME = ExportOperationIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher SCHEMA_WATCHER = new SpliceSchemaWatcher(CLASS_NAME);
    private final Boolean useNativeSyntax;
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }

    static File temporaryFolder;
    @BeforeClass
    public static void createTempDirectory() throws Exception {
        temporaryFolder = SpliceUnitTest.createTempDirectory(CLASS_NAME);
    }

    @AfterClass
    public static void deleteTempDirectory() throws Exception {
        SpliceUnitTest.deleteTempDirectory(temporaryFolder);
    }

    public ExportOperationIT(Boolean useNativeSyntax) {
        this.useNativeSyntax= useNativeSyntax;
    }

    @Test
    public void export() throws Exception {

        TestConnection conn=methodWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate(String.format("create table export_test_%s(\n" +
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
                        ")", useNativeSyntax))
                .withInsert(String.format("insert into export_test_%s values(?,?,?,?,?,?,?,?,?,?,?)", useNativeSyntax))
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

        String exportSQL = buildExportSQL(String.format("select * from export_test_%s order by a", useNativeSyntax));

        exportAndAssertExportResults(exportSQL,8);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
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
                .withCreate(String.format("create table export_local_%s(a smallint,b double, c time,d varchar(20))", useNativeSyntax))
                .withInsert(String.format("insert into export_local_%s values(?,?,?,?)", useNativeSyntax))
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL(String.format("select * from export_local_%s order by a asc", useNativeSyntax), "None");

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
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
                .withCreate(String.format("create table pipe_%s(a smallint,b double, c time,d varchar(20))", useNativeSyntax))
                .withInsert(String.format("insert into pipe_%s values(?,?,?,?)", useNativeSyntax))
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL(String.format("select * from pipe_%s order by a asc", useNativeSyntax), "NONE", "|");

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
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
                .withCreate(String.format("create table tabs_%s(a smallint,b double, c time,d varchar(20))", useNativeSyntax))
                .withInsert(String.format("insert into tabs_%s values(?,?,?,?)", useNativeSyntax))
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL(String.format("select * from tabs_%s order by a asc", useNativeSyntax), " none ", "\\t");

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
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
        methodWatcher.executeUpdate(String.format("create table empty_%s (a int)", useNativeSyntax));
        String exportSQL = buildExportSQL(String.format("select * from empty_%s", useNativeSyntax));
        exportAndAssertExportResults(exportSQL, 0);
    }

    @Test
    public void exportOverFiveTableJoin() throws Exception {

        TableCreator tc =
                new TableCreator(methodWatcher.getOrCreateConnection())
                        .withCreate("create table %s (a int, b int, c int)")
                        .withInsert("insert into %s values(?,?,?)")
                        .withRows(rows(row(1, 2, 3), row(4, 5, 6), row(7, 8, 9)));

        tc.withTableName(String.format("a_%s", useNativeSyntax)).create();
        tc.withTableName(String.format("b_%s", useNativeSyntax)).create();
        tc.withTableName(String.format("c_%s", useNativeSyntax)).create();
        tc.withTableName(String.format("d_%s", useNativeSyntax)).create();
        tc.withTableName(String.format("e_%s", useNativeSyntax)).create();

        String exportSQL = buildExportSQL(String.format(
                "select * from a_%s cross join b_%<s cross join c_%<s cross join d_%<s cross join e_%<s",
                useNativeSyntax));

        exportAndAssertExportResults(exportSQL, 243);
    }

    @Test
    public void exportWithJoinsProjectionsAndRestrictions() throws Exception {

        TableCreator tc =
                new TableCreator(methodWatcher.getOrCreateConnection())
                        .withCreate("create table %s (c1 int, c2 int, c3 int)")
                        .withInsert("insert into %s values(?,?,?)")
                        .withRows(rows(row(1, 1, 1), row(2, 2, 2), row(3, 3, 3), row(4, 4, 4), row(5, 5, 5)));

        tc.withTableName(String.format("aa_%s", useNativeSyntax)).create();
        tc.withTableName(String.format("bb_%s", useNativeSyntax)).create();

        String exportSQL = buildExportSQL(String.format("" +
                "select aa_%s.c1,aa_%<s.c2*100,bb_%<s.c2*300,bb_%<s.c3 " +
                "from aa_%<s " +
                "join bb_%<s on aa_%<s.c1 =bb_%<s.c1 " +
                "where bb_%<s.c3 > 2", useNativeSyntax));

        exportAndAssertExportResults(exportSQL, 3);
    }

    @Test
    public void export_compressed_bz2() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table export_compressed_bz2_%s(a smallint,b double, c time,d varchar(20))", useNativeSyntax))
                .withInsert(String.format("insert into export_compressed_bz2_%s values(?,?,?,?)", useNativeSyntax))
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL(String.format("select * from export_compressed_bz2_%s order by a asc", useNativeSyntax), "BZ2");

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv.bz2"));
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
                .withCreate(String.format("create table export_compressed_gz_%s(a smallint,b double, c time,d varchar(20))", useNativeSyntax))
                .withInsert(String.format("insert into export_compressed_gz_%s values(?,?,?,?)", useNativeSyntax))
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL(String.format("select * from export_compressed_gz_%s order by a asc", useNativeSyntax), "GZIP");

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv.gz"));
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
                .withCreate(String.format("create table export_compressed_gz2_%s(a smallint,b double, c time,d varchar(20))", useNativeSyntax))
                .withInsert(String.format("insert into export_compressed_gz2_%s values(?,?,?,?)", useNativeSyntax))
                .withRows(getTestRows()).create();

        String exportSQL = buildExportSQL(String.format("select * from export_compressed_gz2_%s order by a asc", useNativeSyntax), true);

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv.gz"));
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
                .withCreate(String.format("create table export_decimal_%s(a smallint, b decimal(31, 25), c decimal(31, 2))", useNativeSyntax))
                .withInsert(String.format("insert into export_decimal_%s values(?,?,?)", useNativeSyntax))
                .withRows(rows(row(1, 2000.0, 3.00005), row(1, 2000.0, 3.00005))).create();

        //
        // default column order
        //
        String exportSQL = buildExportSQL(String.format("select * from export_decimal_%s order by a asc", useNativeSyntax), null);

        exportAndAssertExportResults(exportSQL, 2);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "1,2000.0000000000000000000000000,3.00\n" +
                        "1,2000.0000000000000000000000000,3.00\n",
                Files.toString(files[0], Charsets.UTF_8));

        //
        // alternate column order
        //
        FileUtils.deleteDirectory(temporaryFolder);
        exportSQL = buildExportSQL(String.format("select c,b,a from export_decimal_%s order by a asc", useNativeSyntax), "NONE");

        exportAndAssertExportResults(exportSQL, 2);
        files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "3.00,2000.0000000000000000000000000,1\n" +
                        "3.00,2000.0000000000000000000000000,1\n",
                Files.toString(files[0], Charsets.UTF_8));

        //
        // column subset
        //
        FileUtils.deleteDirectory(temporaryFolder);
        exportSQL = buildExportSQL(String.format("select b from export_decimal_%s order by a asc", useNativeSyntax), "NONE");

        exportAndAssertExportResults(exportSQL, 2);
        files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
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
            methodWatcher.executeQuery(buildExportSQL("select 1 from sys.sysaliases", "", "None", null, null, null, null));
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'export path'=''.", e.getMessage());
        }

        // encoding
        try {
            methodWatcher.executeQuery(buildExportSQL("select 1 from sys.sysaliases", "/tmp/", null, 1, "BAD_ENCODING", null, null));
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'encoding'='BAD_ENCODING'.", e.getMessage());
        }

        // field delimiter
        try {
            methodWatcher.executeQuery(buildExportSQL("select 1 from sys.sysaliases", "/tmp/", null, 1, "utf-8", "AAA", null));
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'field delimiter'='AAA'.", e.getMessage());
        }

        // quote character
        try {
            methodWatcher.executeQuery(buildExportSQL("select 1 from sys.sysaliases", "/tmp/", null, 1, "utf-8", ",", "BBB"));
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'quote character'='BBB'.", e.getMessage());
        }

        // no permission to create export dir
        try {
            methodWatcher.executeQuery(buildExportSQL("select 1 from sys.sysaliases", "/ExportOperationIT/", null, 1, "utf-8", null, null));
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid parameter 'cannot create export directory'='/ExportOperationIT/'.", e.getMessage());
        }

        // wrong replica count
        try {
            methodWatcher.executeQuery(buildExportSQL("select 1 from sys.sysaliases", "/tmp/", null, -100, null, null, null));
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

        // wrong field separator
        try {
            methodWatcher.executeQuery("EXPORT TO '/tmp/' AS 'csv' FIELD_SEPARATOR 10 select 1 from sys.sysaliases ");
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

        // wrong quote character
        try {
            methodWatcher.executeQuery("EXPORT TO '/tmp/' AS 'csv' QUOTE_CHARACTER 100 select 1 from sys.sysaliases ");
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

        // wrong replication parameter
        try {
            methodWatcher.executeQuery("EXPORT TO '/tmp/' AS 'csv' REPLICATION_COUNT 'a' select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", "XIE0X", e.getSQLState());
            assertEquals("Invalid error message", "The export operation was not performed, because value of the specified parameter (a) is wrong.", e.getMessage());
        }

        // wrong compression format
        try {
            methodWatcher.executeQuery("EXPORT TO '/tmp/' AS 'csv' COMPRESSION 'WrongFormat' select 1 from sys.sysaliases");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", "XIE13", e.getSQLState());
            assertEquals("Invalid error message", "Unsuported compression format: WRONGFORMAT", e.getMessage());
        }

        try {
            methodWatcher.executeQuery("EXPORT('/tmp/', 'WrongFormat', null, null, null, null) select 1 from sys.sysaliases");
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

    private String buildExportSQL(String selectQuery, boolean compression, String fieldDelimiter) {
        return buildExportSQL(selectQuery, Boolean.toString(compression), fieldDelimiter);
    }

    private String buildExportSQL(String selectQuery, String compression, String fieldDelimiter) {
        String exportPath = temporaryFolder.getAbsolutePath();
        if (compression == null) {
            compression = "false";
        }
        return buildExportSQL(selectQuery, exportPath, compression, 3, null, fieldDelimiter, null);
    }

    private String buildExportSQL(String selectQuery, String exportPath, String compression, Integer replicationCount, String encoding, String fieldSeparator, String quoteCharacter) {
        return buildExportSQL(selectQuery, exportPath, compression, replicationCount, encoding, fieldSeparator, quoteCharacter, null);
    }

    private String buildExportSQL(String selectQuery, String exportPath, String compression, Integer replicationCount, String encoding, String fieldSeparator, String quoteCharacter, String quoteMode) {
        if (useNativeSyntax) {
            StringBuilder sql = new StringBuilder();
            sql.append("EXPORT TO '").append(exportPath).append("'");
            sql.append(" AS csv ");
            if (compression != null) {
                sql.append(" COMPRESSION ").append(compression);
            }
            if (replicationCount != null) {
                sql.append(" REPLICATION_COUNT ").append(replicationCount);
            }
            if (encoding != null) {
                sql.append(" ENCODING '").append(encoding).append("'");
            }
            if (fieldSeparator != null) {
                sql.append(" FIELD_SEPARATOR '").append(fieldSeparator).append("'");
            }
            if (quoteCharacter != null) {
                sql.append(" QUOTE_CHARACTER '").append(quoteCharacter).append("'");
            }
            if (quoteMode != null) {
                sql.append(" QUOTE_MODE ").append(quoteMode);
            }
            sql.append(" ").append(selectQuery);
            return sql.toString();
        } else {
            StringBuilder sql = new StringBuilder();
            sql.append("EXPORT('").append(exportPath).append("', ");
            if (compression == null) {
                sql.append("null, ");
            } else if (compression.toUpperCase().equals("TRUE") || compression.toLowerCase().equals("FALSE")) {
                sql.append(compression.toLowerCase()).append(", ");
            } else {
                sql.append("'").append(compression).append("', ");
            }
            if (replicationCount == null) {
                sql.append("null, ");
            } else {
                sql.append(replicationCount).append(", ");
            }
            if (encoding == null) {
                sql.append("null, ");
            } else {
                sql.append("'").append(encoding).append("', ");
            }
            if (fieldSeparator == null) {
                sql.append("null, ");
            } else {
                sql.append("'").append(fieldSeparator).append("', ");
            }
            if (quoteCharacter == null) {
                sql.append("null)");
            } else {
                sql.append("'").append(quoteCharacter).append("')");
            }
            sql.append(" ").append(selectQuery);
            assert quoteMode == null;
            return sql.toString();
        }
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
                    .withCreate(String.format("create table export_s3_test_%s (c1 int, c2 int, c3 int)", useNativeSyntax))
                    .withInsert(String.format("insert into export_s3_test_%s values(?,?,?)", useNativeSyntax))
                    .withRows(rows(row(1, 1, 1), row(2, 2, 2), row(3, 3, 3), row(4, 4, 4), row(5, 5, 5)))
                    .create();

            Long expectedRowCount = methodWatcher.query(buildExportSQL(String.format("select * from export_s3_test_%s", useNativeSyntax),
                    "s3a://molitorisspechial/temp/", null, null, null, null, null));
            fail();
        } catch (SQLException sqle) {
            String sqlState = sqle.getSQLState();
            Assert.assertTrue(sqlState, sqlState.compareToIgnoreCase("EXT26") == 0 ||
                    sqlState.compareToIgnoreCase("XCZ02") == 0 ||
                    sqlState.compareToIgnoreCase("XJ001") == 0);
        }
    }

    @Test
    public void exportQuoteAlways() throws Exception {
        if (!useNativeSyntax)
            return;

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table export_quote_always_%s (a smallint,b double, c time,d varchar(20))", useNativeSyntax))
                .withInsert(String.format("insert into export_quote_always_%s values(?,?,?,?)", useNativeSyntax))
                .withRows(getTestRows()).create();

        String exportPath = temporaryFolder.getAbsolutePath();
        String exportSQL = buildExportSQL(String.format("select * from export_quote_always_%s order by a asc", useNativeSyntax),
                exportPath, null, null, null, null, null, "always");

        exportAndAssertExportResults(exportSQL, 6);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("" +
                        "25,3.14159,14:31:20,\"varchar1\"\n" +
                        "26,3.14159,14:31:20,\"varchar1\"\n" +
                        "27,3.14159,14:31:20,\"varchar1 space\"\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\"\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\"\n" +
                        "30,3.14159,14:31:20,\"varchar1\"\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

}
