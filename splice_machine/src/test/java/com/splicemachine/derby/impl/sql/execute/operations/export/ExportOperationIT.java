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

import com.splicemachine.db.iapi.reference.SQLState;
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
    private final Boolean useKeywords;
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    String paramWrong = SQLState.EXPORT_PARAMETER_IS_WRONG.split("\\.")[0];
    String invalidParam = SQLState.UU_INVALID_PARAMETER.split("\\.")[0];
    String exportParamWrong = SQLState.EXPORT_PARAMETER_VALUE_IS_WRONG.split("\\.")[0];


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(3);
        params.add(new Object[]{true, true});
        params.add(new Object[]{true, false});
        params.add(new Object[]{false, false});
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

    public ExportOperationIT(Boolean useNativeSyntax, Boolean useKeywords) {
        this.useNativeSyntax = useNativeSyntax;
        this.useKeywords = useKeywords;
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
                        ")", getSuffix()))
                .withInsert(String.format("insert into export_test_%s values(?,?,?,?,?,?,?,?,?,?,?)", getSuffix()))
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

        String exportSQL = new ExportBuilder(String.format("select * from export_test_%s order by a", getSuffix())).getSql();

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
                .withCreate(String.format("create table export_local_%s(a smallint,b double, c time,d varchar(20), e varchar(20) for bit data)", getSuffix()))
                .withInsert(String.format("insert into export_local_%s values(?,?,?,?,?)", getSuffix()))
                .withRows(getTestRows()).create();

        String exportSQL = new ExportBuilder(String.format("select * from export_local_%s order by a asc", getSuffix()))
                .compression("None").getSql();

        exportAndAssertExportResults(exportSQL, 8);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals(
                "0,0.0,00:00:00,,\n" +
                        "25,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "26,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "27,3.14159,14:31:20,varchar1 space,626974206461746131\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\",62697464617461202c2031\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\",6269746461746120222c2031\n" +
                        "30,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        ",,,,\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    @Test
    public void export_withAlternateRecordDelimiter() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table pipe_%s(a smallint,b double, c time,d varchar(20), e varchar(20) for bit data)", getSuffix()))
                .withInsert(String.format("insert into pipe_%s values(?,?,?,?,?)", getSuffix()))
                .withRows(getTestRows()).create();

        String exportSQL = new ExportBuilder(String.format("select * from pipe_%s order by a asc", getSuffix()))
                                    .compression("None").fieldSeparator("|").getSql();

        exportAndAssertExportResults(exportSQL, 8);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals(
                "0|0.0|00:00:00||\n" +
                        "25|3.14159|14:31:20|varchar1|6269746461746131\n" +
                        "26|3.14159|14:31:20|varchar1|6269746461746131\n" +
                        "27|3.14159|14:31:20|varchar1 space|626974206461746131\n" +
                        "28|3.14159|14:31:20|varchar1 , comma|62697464617461202c2031\n" +
                        "29|3.14159|14:31:20|\"varchar1 \"\" quote\"|6269746461746120222c2031\n" +
                        "30|3.14159|14:31:20|varchar1|6269746461746131\n" +
                        "||||\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    @Test
    public void export_withTabs() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table tabs_%s(a smallint,b double, c time,d varchar(20), e varchar(20) for bit data)", getSuffix()))
                .withInsert(String.format("insert into tabs_%s values(?,?,?,?,?)", getSuffix()))
                .withRows(getTestRows()).create();

        String exportSQL = new ExportBuilder(String.format("select * from tabs_%s order by a asc", getSuffix()))
                .compression(" none ").fieldSeparator("\\t").getSql();

        exportAndAssertExportResults(exportSQL, 8);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals(
                "0\t0.0\t00:00:00\t\t\n" +
                        "25\t3.14159\t14:31:20\tvarchar1\t6269746461746131\n" +
                        "26\t3.14159\t14:31:20\tvarchar1\t6269746461746131\n" +
                        "27\t3.14159\t14:31:20\tvarchar1 space\t626974206461746131\n" +
                        "28\t3.14159\t14:31:20\tvarchar1 , comma\t62697464617461202c2031\n" +
                        "29\t3.14159\t14:31:20\t\"varchar1 \"\" quote\"\t6269746461746120222c2031\n" +
                        "30\t3.14159\t14:31:20\tvarchar1\t6269746461746131\n" +
                        "\t\t\t\t\n",


                Files.toString(files[0], Charsets.UTF_8));
    }

    @Test
    public void exportEmptyTableDoesNotBlowup() throws Exception {
        methodWatcher.executeUpdate(String.format("create table empty_%s (a int)", getSuffix()));
        String exportSQL = new ExportBuilder(String.format("select * from empty_%s", getSuffix())).getSql();
        exportAndAssertExportResults(exportSQL, 0);
    }

    @Test
    public void exportOverFiveTableJoin() throws Exception {

        TableCreator tc =
                new TableCreator(methodWatcher.getOrCreateConnection())
                        .withCreate("create table %s (a int, b int, c int)")
                        .withInsert("insert into %s values(?,?,?)")
                        .withRows(rows(row(1, 2, 3), row(4, 5, 6), row(7, 8, 9)));

        tc.withTableName(String.format("a_%s", getSuffix())).create();
        tc.withTableName(String.format("b_%s", getSuffix())).create();
        tc.withTableName(String.format("c_%s", getSuffix())).create();
        tc.withTableName(String.format("d_%s", getSuffix())).create();
        tc.withTableName(String.format("e_%s", getSuffix())).create();

        String exportSQL = new ExportBuilder(String.format(
                "select * from a_%s cross join b_%<s cross join c_%<s cross join d_%<s cross join e_%<s",
                getSuffix())).getSql();

        exportAndAssertExportResults(exportSQL, 243);
    }

    @Test
    public void exportWithJoinsProjectionsAndRestrictions() throws Exception {

        TableCreator tc =
                new TableCreator(methodWatcher.getOrCreateConnection())
                        .withCreate("create table %s (c1 int, c2 int, c3 int)")
                        .withInsert("insert into %s values(?,?,?)")
                        .withRows(rows(row(1, 1, 1), row(2, 2, 2), row(3, 3, 3), row(4, 4, 4), row(5, 5, 5)));

        tc.withTableName(String.format("aa_%s", getSuffix())).create();
        tc.withTableName(String.format("bb_%s", getSuffix())).create();

        String exportSQL = new ExportBuilder(String.format("" +
                "select aa_%s.c1,aa_%<s.c2*100,bb_%<s.c2*300,bb_%<s.c3 " +
                "from aa_%<s " +
                "join bb_%<s on aa_%<s.c1 =bb_%<s.c1 " +
                "where bb_%<s.c3 > 2", getSuffix())).getSql();

        exportAndAssertExportResults(exportSQL, 3);
    }

    @Test
    public void export_compressed_bz2() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table export_compressed_bz2_%s(a smallint,b double, c time,d varchar(20), e varchar(20) for bit data)", getSuffix()))
                .withInsert(String.format("insert into export_compressed_bz2_%s values(?,?,?,?,?)", getSuffix()))
                .withRows(getTestRows()).create();

        String exportSQL = new ExportBuilder(String.format("select * from export_compressed_bz2_%s order by a asc", getSuffix()))
                                .compression("BZ2").getSql();

        exportAndAssertExportResults(exportSQL, 8);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv.bz2"));
        assertEquals(1, files.length);
        assertEquals(
                "0,0.0,00:00:00,,\n" +
                        "25,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "26,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "27,3.14159,14:31:20,varchar1 space,626974206461746131\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\",62697464617461202c2031\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\",6269746461746120222c2031\n" +
                        "30,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        ",,,,\n",
                IOUtils.toString(new BZip2CompressorInputStream(new FileInputStream(files[0]))));
    }

    @Test
    public void export_compressed_gz() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table export_compressed_gz_%s(a smallint,b double, c time,d varchar(20), e varchar(20) for bit data)", getSuffix()))
                .withInsert(String.format("insert into export_compressed_gz_%s values(?,?,?,?,?)", getSuffix()))
                .withRows(getTestRows()).create();

        String exportSQL = new ExportBuilder(String.format("select * from export_compressed_gz_%s order by a asc", getSuffix()))
                                    .compression("GZIP").getSql();

        exportAndAssertExportResults(exportSQL, 8);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv.gz"));
        assertEquals(1, files.length);
        assertEquals(
                "0,0.0,00:00:00,,\n" +
                        "25,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "26,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "27,3.14159,14:31:20,varchar1 space,626974206461746131\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\",62697464617461202c2031\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\",6269746461746120222c2031\n" +
                        "30,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        ",,,,\n",
                IOUtils.toString(new GZIPInputStream(new FileInputStream(files[0]))));
    }

    @Test
    public void export_compressed_gz2() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table export_compressed_gz2_%s(a smallint,b double, c time,d varchar(20),e varchar(20) for bit data)", getSuffix()))
                .withInsert(String.format("insert into export_compressed_gz2_%s values(?,?,?,?,?)", getSuffix()))
                .withRows(getTestRows()).create();

        String exportSQL = new ExportBuilder(String.format("select * from export_compressed_gz2_%s order by a asc", getSuffix()))
                                .compression("true").getSql();

        exportAndAssertExportResults(exportSQL, 8);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv.gz"));
        assertEquals(1, files.length);
        assertEquals("0,0.0,00:00:00,,\n" +
                        "25,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "26,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "27,3.14159,14:31:20,varchar1 space,626974206461746131\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\",62697464617461202c2031\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\",6269746461746120222c2031\n" +
                        "30,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        ",,,,\n",
                IOUtils.toString(new GZIPInputStream(new FileInputStream(files[0]))));
    }

    @Test
    public void export_decimalFormatting() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table export_decimal_%s(a smallint, b decimal(31, 25), c decimal(31, 2))", getSuffix()))
                .withInsert(String.format("insert into export_decimal_%s values(?,?,?)", getSuffix()))
                .withRows(rows(row(1, 2000.0, 3.00005), row(1, 2000.0, 3.00005))).create();

        //
        // default column order
        //
        String exportSQL = new ExportBuilder(String.format("select * from export_decimal_%s order by a asc", getSuffix())).getSql();

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
        exportSQL = new ExportBuilder(String.format("select c,b,a from export_decimal_%s order by a asc", getSuffix()))
                            .compression("NONE").getSql();

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
        exportSQL = new ExportBuilder(String.format("select b from export_decimal_%s order by a asc", getSuffix()))
                             .compression("NONE").getSql();

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
        String exportSQL = new ExportBuilder("select * from sys.sysaliases").compression("none").getSql();
        exportAndAssertExportResults(exportSQL, expectedRowCount);
    }

    ExportBuilder simpleExport() {
        return new ExportBuilder("select 1 from sys.sysaliases").exportPath("/tmp");
    }

    void testFail(String sql, String expectedState, String expectedMsg) {
        try {
            methodWatcher.executeQuery(sql);
            fail();
        } catch (SQLException e) {
            if(expectedMsg != null)
                assertEquals(expectedMsg, e.getMessage());
            if(expectedState != null)
                assertEquals(expectedState, e.getSQLState());
        }

    }
    void testFail(ExportBuilder exp, String expectedState, String expectedMsg) {
        testFail(exp.getSql(), expectedState, expectedMsg);
    }

    /* It is important that we throw SQLException, given invalid parameters, rather than other exceptions which cause IJ to drop the connection.  */
    @Test
    public void export_throwsSQLException_givenBadArguments() throws Exception {
        // export path
        testFail(simpleExport().exportPath(""), invalidParam,
            "Invalid parameter 'export path'=''.");

        // encoding
        testFail(simpleExport().encoding("BAD_ENCODING"),invalidParam,
                "Invalid parameter 'encoding'='BAD_ENCODING'.");

        // field delimiter
        testFail(simpleExport().fieldSeparator("AAA"),invalidParam,
                "Invalid parameter 'field delimiter'='AAA'.");

        // quote character
        testFail(simpleExport().quoteCharacter("BBB"),invalidParam,
                "Invalid parameter 'quote character'='BBB'.");

        // no permission to create export dir
        testFail(simpleExport().exportPath("/ExportOperationIT/"),invalidParam,
                "Invalid parameter 'cannot create export directory'='/ExportOperationIT/'.");

        // wrong replica count
        testFail(simpleExport().replicationCount("-100"), paramWrong,
                "The export operation was not performed, because the specified parameter (replicationCount) is less than or equal to zero.");

        // wrong field separator
        try {
            methodWatcher.executeQuery("EXPORT TO '/tmp/' AS 'csv' FIELD_SEPARATOR 10 select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", exportParamWrong, e.getSQLState());
            assertEquals("Invalid error message", "The export operation was not performed, because value of the specified parameter (10) is wrong.", e.getMessage());
        }

        // wrong quote character
        try {
            methodWatcher.executeQuery("export('/tmp/', null, null, null, null, 100) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", exportParamWrong, e.getSQLState());
            assertEquals("Invalid error message", "The export operation was not performed, because value of the specified parameter (100) is wrong.", e.getMessage());
        }

        // wrong quote character
        try {
            methodWatcher.executeQuery("EXPORT TO '/tmp/' AS 'csv' QUOTE_CHARACTER 100 select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", exportParamWrong, e.getSQLState());
            assertEquals("Invalid error message", "The export operation was not performed, because value of the specified parameter (100) is wrong.", e.getMessage());
        }

        // wrong replication parameter
        try {
            methodWatcher.executeQuery("export('/tmp/', null, 'a', null, null, null) select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", exportParamWrong, e.getSQLState());
            assertEquals("Invalid error message", "The export operation was not performed, because value of the specified parameter (a) is wrong.", e.getMessage());
        }

        // wrong replication parameter
        try {
            methodWatcher.executeQuery("EXPORT TO '/tmp/' AS 'csv' REPLICATION_COUNT 'a' select 1 from sys.sysaliases ");
            fail();
        } catch (SQLException e) {
            assertEquals("Invalid error state", exportParamWrong, e.getSQLState());
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

    class ExportBuilder {
        String selectQuery;
        String exportPath = temporaryFolder.getAbsolutePath();
        String compression;
        String replicationCount;
        String encoding;
        String fieldSeparator;
        String quoteCharacter;
        String quoteMode;
        String floatingPointNotation;
        String timestampFormat;
        ExportBuilder(String selectQuery) {
            this.selectQuery = selectQuery;
        }
        ExportBuilder exportPath(String val) { exportPath = val; return this; }
        ExportBuilder compression(String val) { compression = val; return this; }
        ExportBuilder replicationCount(String val) { replicationCount = val; return this; }
        ExportBuilder encoding(String val) { encoding = val; return this; }
        ExportBuilder fieldSeparator(String val) { fieldSeparator = val; return this; }
        ExportBuilder quoteCharacter(String val) { quoteCharacter = val; return this; }
        ExportBuilder quoteMode(String val) { quoteMode = val; return this; }
        ExportBuilder floatingPointNotation(String val) { floatingPointNotation = val; return this; }
        ExportBuilder timestampFormat(String val) { timestampFormat = val; return this; }

        String getSql() {
            return buildExportSQL(selectQuery, exportPath, compression,
                    replicationCount, encoding, fieldSeparator,
                    quoteCharacter, quoteMode, floatingPointNotation, timestampFormat);
        }
    }

    private String buildExportSQL(String selectQuery, String exportPath, String compression, String replicationCount,
                                  String encoding, String fieldSeparator, String quoteCharacter, String quoteMode,
                                  String floatingPointNotation, String timestampFormat) {
        if (useNativeSyntax) {
            StringBuilder sql = new StringBuilder();
            sql.append("EXPORT TO '").append(exportPath).append("'");
            if (useKeywords)
                sql.append(" AS CSV ");
            else
                sql.append(" AS 'csv' ");
            if (compression != null) {
                sql.append(" COMPRESSION ");
                if (useKeywords)
                    sql.append(compression);
                else
                    sql.append("'").append(compression).append("'");
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
                sql.append(" QUOTE_MODE ");
                if (useKeywords)
                    sql.append(quoteMode);
                else
                    sql.append("'").append(quoteMode).append("'");
            }
            if (floatingPointNotation != null) {
                sql.append(" FLOATING_POINT_NOTATION ");
                if (useKeywords)
                    sql.append(floatingPointNotation);
                else
                    sql.append("'").append(floatingPointNotation).append("'");
            }
            if (timestampFormat != null) {
                sql.append(" TIMESTAMP_FORMAT '").append(timestampFormat).append("'");
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
            assert floatingPointNotation == null;
            assert timestampFormat == null;
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
                row(25, 3.14159, "14:31:20", "varchar1", "bitdata1"),
                row(26, 3.14159, "14:31:20", "varchar1", "bitdata1"),
                row(27, 3.14159, "14:31:20", "varchar1 space", "bit data1"),
                row(28, 3.14159, "14:31:20", "varchar1 , comma", "bitdata , 1"),
                row(29, 3.14159, "14:31:20", "varchar1 \" quote", "bitdata \", 1"),
                row(30, 3.14159, "14:31:20", "varchar1", "bitdata1"),
                row(null, null, null, null, null),
                row(0, 0.0, "00:00:00", "", "")

        );
    }

    @Test
    public void exportExceptionsS3() throws Exception {
        try {
            new TableCreator(methodWatcher.getOrCreateConnection())
                    .withCreate(String.format("create table export_s3_test_%s (c1 int, c2 int, c3 int)", getSuffix()))
                    .withInsert(String.format("insert into export_s3_test_%s values(?,?,?)", getSuffix()))
                    .withRows(rows(row(1, 1, 1), row(2, 2, 2), row(3, 3, 3), row(4, 4, 4), row(5, 5, 5)))
                    .create();

            String exportSql = new ExportBuilder(String.format("select * from export_s3_test_%s", getSuffix()))
                    .exportPath("s3a://molitorisspechial/temp/").getSql();
            Long expectedRowCount = methodWatcher.query(exportSql);
            fail();
        } catch (SQLException sqle) {
            String sqlState = sqle.getSQLState();
            Assert.assertTrue(sqlState, sqlState.compareToIgnoreCase("EXT26") == 0 ||
                    sqlState.compareToIgnoreCase(invalidParam) == 0 ||
                    sqlState.compareToIgnoreCase("XJ001") == 0);
        }
    }

    @Test
    public void exportQuoteAlways() throws Exception {
        if (!useNativeSyntax)
            return;

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table export_quote_always_%s (a smallint, b double, c time, d varchar(20), e varchar(20) for bit data)", getSuffix()))
                .withInsert(String.format("insert into export_quote_always_%s values(?,?,?,?,?)", getSuffix()))
                .withRows(getTestRows()).create();

        String exportPath = temporaryFolder.getAbsolutePath();
        String exportSQL = new ExportBuilder(String.format("select * from export_quote_always_%s order by a asc", getSuffix()))
                .exportPath(exportPath).quoteMode("always").getSql();

        exportAndAssertExportResults(exportSQL, 8);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("0,0.0,00:00:00,\"\",\"\"\n" +
                        "25,3.14159,14:31:20,\"varchar1\",\"6269746461746131\"\n" +
                        "26,3.14159,14:31:20,\"varchar1\",\"6269746461746131\"\n" +
                        "27,3.14159,14:31:20,\"varchar1 space\",\"626974206461746131\"\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\",\"62697464617461202c2031\"\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\",\"6269746461746120222c2031\"\n" +
                        "30,3.14159,14:31:20,\"varchar1\",\"6269746461746131\"\n" +
                        ",,,,\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    @Test
    public void exportFloatingPointNotation() throws Exception {
        if (!useNativeSyntax)
            return;

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table normalized_notation_%s (a real, b double)", getSuffix()))
                .withInsert(String.format("insert into normalized_notation_%s values(?, ?)", getSuffix()))
                .withRows(rows(row("123.123", "-0.0123123"))).create();

        String exportPath = temporaryFolder.getAbsolutePath();
        String exportSQL = new ExportBuilder(String.format("select * from normalized_notation_%s order by a asc", getSuffix()))
                .exportPath(exportPath).floatingPointNotation("normalized").getSql();

        exportAndAssertExportResults(exportSQL, 1);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("1.23123E2,-1.23123E-2\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    @Test
    public void exportTimestampFormat() throws Exception {
        if (!useNativeSyntax)
            return;

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(String.format("create table timestamp_format_%s (a timestamp)", getSuffix()))
                .withInsert(String.format("insert into timestamp_format_%s values(?)", getSuffix()))
                .withRows(rows(row("2020-01-01 12:15:16.123456789"))).create();

        String exportPath = temporaryFolder.getAbsolutePath();
        String exportSQL = new ExportBuilder(String.format("select * from timestamp_format_%s order by a asc", getSuffix()))
                .exportPath(exportPath).timestampFormat("yyyy/MM/dd HH:mm:ss.SSSS").getSql();

        exportAndAssertExportResults(exportSQL, 1);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("2020/01/01 12:15:16.1234\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    private String getSuffix() {
        return useNativeSyntax + "_" + useKeywords;
    }

}
