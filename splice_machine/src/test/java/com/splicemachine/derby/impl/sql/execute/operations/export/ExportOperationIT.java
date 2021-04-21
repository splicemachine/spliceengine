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
import java.io.InputStream;
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

        String exportSql() {
            return buildExportSQL(selectQuery, exportPath, compression,
                    replicationCount, encoding, fieldSeparator,
                    quoteCharacter, quoteMode, floatingPointNotation, timestampFormat);
        }
        String strOrNull(String s) {
            if( s == null)
                return "null, ";
            else
                return "'" + s + "', ";
        }
        String importSql(String schema, String table) {
            return "call SYSCS_UTIL.IMPORT_DATA(" + strOrNull(schema) + strOrNull(table) +
                    "null, " + //  insertColumnList
                    strOrNull(exportPath) + // path
                    strOrNull(fieldSeparator) + // columnDelimiter
                    strOrNull(quoteCharacter) + // characterDelimiter
                    strOrNull( timestampFormat) +
                    strOrNull(null) + // dateFormat
                    strOrNull( null) + // timeFormat
                    "'0', " + // badRecordsAllowed
                    "'/tmp', " + // badRecordLogDirectory
                    "false, " + // oneLineRecords
                    "null" + // charset
                    ")";
        }

        void testImportExport(String tableName, String columns, long expectedExportRowCount) throws Exception {

            exportAndAssertExportResults(exportSql(), expectedExportRowCount);

            String res1 = methodWatcher.executeToString("select " + columns + " from " + tableName, true);

            methodWatcher.execute("DELETE FROM " + tableName);
            methodWatcher.execute(importSql( CLASS_NAME, tableName));

            String res2 = methodWatcher.executeToString("select " + columns + " from " + tableName, true);
            Assert.assertEquals(res1, res2);
        }

    }

    @Test
    public void export() throws Exception {
        String tableName = String.format("export_test_%s", getSuffix());

        TestConnection conn=methodWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table " + tableName + "(\n" +
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
                .withInsert("insert into " + tableName + " values(?,?,?,?,?,?,?,?,?,?,?)")
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

        if(!useNativeSyntax) return; // todo
        ExportBuilder builder = new ExportBuilder("select * from " + tableName + " order by a").quoteMode("always");
        builder.testImportExport(tableName, "*", 8);

        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("25,1000000000,2000000000000000,3.14159,3.14159,2,2.34,\"varchar\",\"c\",2014-10-01,14:30:20\n" +
                        "26,1000000000,2000000000000000,3.14159,3.14159,2,2.34,\"varchar\",\"c\",2014-10-01,14:30:20\n" +
                        "27,1000000000,2000000000000000,3.14159,3.14159,2,2.34,\"varchar\",\"c\",2014-10-01,14:30:20\n" +
                        "28,1000000000,2000000000000000,3.14159,3.14159,2,2.34,\"varchar\",\"c\",2014-10-01,14:30:20\n" +
                        "29,1000000000,2000000000000000,3.14159,3.14159,2,2.34,\"varchar\",\"c\",2014-10-01,14:30:20\n" +
                        "30,1000000000,2000000000000000,3.14159,3.14159,2,2.34,\"varchar\",\"c\",2014-10-01,14:30:20\n" +
                        "31,1000000000,2000000000000000,3.14159,3.14159,2,2.34,\"varchar\",\"c\",2014-10-01,14:30:20\n" +
                        "32,1000000000,2000000000000000,3.14159,3.14159,2,2.34,\"varchar\",\"c\",2014-10-01,14:30:20\n",
                Files.toString(files[0], Charsets.UTF_8));

        try(CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
            cs.setString(1,SCHEMA_WATCHER.schemaName);
            cs.execute();
        }
    }

    @Test
    public void export_defaultDelimiter() throws Exception {
        if(!useNativeSyntax) return;
        String tableName = createTestTableWithSuffix("export_local");
        ExportBuilder builder = new ExportBuilder("select * from " + tableName + " order by a asc");

        // todo: problems with varchar null/""
        String columns = //"a, b, c, d, cast(e AS VARCHAR(32))"
                        "a, b, c";
        builder.testImportExport(tableName, columns, 8);
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
        String tableName = createTestTableWithSuffix("pipe");
        ExportBuilder builder = new ExportBuilder("select * from " + tableName + " order by a asc")
                                    .compression("None").fieldSeparator("|");

        builder.testImportExport(tableName, "a, b, c", 8);
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
        String tableName = createTestTableWithSuffix("tabs");
        ExportBuilder builder = new ExportBuilder(String.format("select * from tabs_%s order by a asc", getSuffix()))
                .compression(" none ").fieldSeparator("\\t");

        builder.testImportExport(tableName, "a, b, c", 8);
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
        String exportSQL = new ExportBuilder(String.format("select * from empty_%s", getSuffix())).exportSql();
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
                getSuffix())).exportSql();

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
                "where bb_%<s.c3 > 2", getSuffix())).exportSql();

        exportAndAssertExportResults(exportSQL, 3);
    }

    @FunctionalInterface
    public interface FunctionWithException<T, R> {
        R apply(T t) throws Exception;
    }

    public void export_compressed(String compression, FunctionWithException<FileInputStream, InputStream> f,
                                  String pattern) throws Exception {
        String tableName = createTestTableWithSuffix("export_compressed_" + compression);
        ExportBuilder builder = new ExportBuilder("select * from " + tableName + " order by a asc")
                .compression(compression);

        // todo: column D, E have problems with NULL
        builder.testImportExport(tableName, "a, b, c", 8);
        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(pattern));
        assertEquals(1, files.length);

        InputStream is = f.apply(new FileInputStream(files[0]));
        assertEquals(
                "0,0.0,00:00:00,,\n" +
                        "25,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "26,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        "27,3.14159,14:31:20,varchar1 space,626974206461746131\n" +
                        "28,3.14159,14:31:20,\"varchar1 , comma\",62697464617461202c2031\n" +
                        "29,3.14159,14:31:20,\"varchar1 \"\" quote\",6269746461746120222c2031\n" +
                        "30,3.14159,14:31:20,varchar1,6269746461746131\n" +
                        ",,,,\n",

                IOUtils.toString( is) );
    }


    @Test
    public void export_compressed_bz2() throws Exception {
        export_compressed("bz2", BZip2CompressorInputStream::new, ".*csv.bz2" );
    }

    @Test
    public void export_compressed_gz() throws Exception {
        export_compressed("GZIP", GZIPInputStream::new, ".*csv.gz" );
    }

    @Test
    public void export_compressed_gz2() throws Exception {
        export_compressed("true", GZIPInputStream::new, ".*csv.gz" );
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
        String exportSQL = new ExportBuilder(String.format("select * from export_decimal_%s order by a asc", getSuffix())).exportSql();

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
                            .compression("NONE").exportSql();

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
                             .compression("NONE").exportSql();

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
        String exportSQL = new ExportBuilder("select * from sys.sysaliases").compression("none").exportSql();
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
        testFail(exp.exportSql(), expectedState, expectedMsg);
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

    String createTestTableWithSuffix(String name) throws SQLException {
        Iterable<Iterable<Object>> testRows =  rows(
                    row(25, 3.14159, "14:31:20", "varchar1", "bitdata1"),
                    row(26, 3.14159, "14:31:20", "varchar1", "bitdata1"),
                    row(27, 3.14159, "14:31:20", "varchar1 space", "bit data1"),
                    row(28, 3.14159, "14:31:20", "varchar1 , comma", "bitdata , 1"),
                    row(29, 3.14159, "14:31:20", "varchar1 \" quote", "bitdata \", 1"),
                    row(30, 3.14159, "14:31:20", "varchar1", "bitdata1"),
                    row(null, null, null, null, null),
                    row(0, 0.0, "00:00:00", "", "")

            );
        String tableName = String.format(name + "_%s", getSuffix());
        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table " + tableName + " (a smallint, b double, c time, d varchar(20), e varchar(20) for bit data)")
                .withInsert("insert into " + tableName + " values(?,?,?,?,?)")
                .withRows(testRows).create();
        return tableName;

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
                    .exportPath("s3a://molitorisspechial/temp/").exportSql();
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
        String tableName = createTestTableWithSuffix("export_quote_always");
        ExportBuilder builder = new ExportBuilder("select * from " + tableName + " order by a asc")
                                .quoteMode("always");

        // todo: column D, E have problems with NULL
        String columns = //"a, b, c, d, cast(e AS VARCHAR(32))"
                "a, b, c";
        builder.testImportExport(tableName, columns, 8);

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
        String tableName = String.format("normalized_notation_%s", getSuffix());
        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table " + tableName + " (a real, b double)")
                .withInsert("insert into " + tableName + " values(?, ?)")
                .withRows(rows(row("123.123", "-0.0123123"))).create();

        ExportBuilder builder = new ExportBuilder("select * from " + tableName + " order by a asc")
                .floatingPointNotation("normalized");

        builder.testImportExport(tableName, "*", 1);

        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("1.23123E2,-1.23123E-2\n",
                Files.toString(files[0], Charsets.UTF_8));
    }

    public void exportTimestampFormat(boolean truncated, String format, String expected, String expectedFile) throws Exception {
        if (!useNativeSyntax)
            return;

        String tableName = "timestamp_format";
        try( TableCreator tc = new TableCreator(methodWatcher.getOrCreateConnection())
                 .withTableName(tableName)
                 .withCreate("create table %s (a timestamp)")
                 .withInsert("insert into %s values(?)")
                 .withRows(rows(row("2020-01-01 12:15:16.123456789"))).create())
        {


            ExportBuilder builder = new ExportBuilder("select * from " + tableName + " order by a asc")
                    .timestampFormat(format);

            if (!truncated)
                builder.testImportExport(tableName, "*", 1);
            else
                exportAndAssertExportResults(builder.exportSql(), 1);
            File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
            assertEquals(1, files.length);
            assertEquals(expectedFile + "\n",
                    Files.toString(files[0], Charsets.UTF_8));

            methodWatcher.execute("DELETE FROM " + tableName);
            methodWatcher.execute(builder.importSql(CLASS_NAME, tableName));

            Assert.assertEquals(expected,
                    methodWatcher.executeGetString("select * from " + tableName, 1));
        }
    }

    @Test
    public void exportTimestampFormatDefault() throws Exception {
        exportTimestampFormat(false, null, "2020-01-01 12:15:16.123456",
                "2020-01-01 12:15:16.123456000");
    }

    @Test
    public void exportTimestampFormatCustom() throws Exception {
        exportTimestampFormat(false, "yyyy-MM-dd-HH.mm.ss.SSSSSS","2020-01-01 12:15:16.123456",
                "2020-01-01-12.15.16.123456");
    }

    @Test
    public void exportTimestampFormatCustomTruncated() throws Exception {
        exportTimestampFormat(true, "yyyy-MM-dd-HH.mm.ss.SSSS","2020-01-01 12:15:16.1234",
                "2020-01-01-12.15.16.1234");
    }

    private String getSuffix() {
        return useNativeSyntax + "_" + useKeywords;
    }

    public void exportNullEmpty(boolean empty_string_compatible, boolean quoteModeAlways) throws Exception {
        if(empty_string_compatible)
            methodWatcher.execute("call syscs_util.syscs_set_global_database_property('splice.db2.import.empty_string_compatible', 'true')");
        String tableName = String.format("export_test_null_empty_%s", getSuffix());

        TestConnection conn=methodWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table " + tableName + "(\n" +
                                "a integer, h varchar(20), i char)")
                .withInsert("insert into " + tableName + " values(?,?,?)")
                .withRows(
                        rows(
                                row("1", null, null),
                                row("2", "", "")
                        )
                ).create();

        if(!useNativeSyntax) return; // todo: no quote mode always
        ExportBuilder builder = new ExportBuilder("select * from " + tableName + " order by a");
        if(quoteModeAlways)
            builder.quoteMode("always");

        //builder.testImportExport(tableName, "a, b, c, d", 10);
        builder.testImportExport(tableName, "*", 2);

        File[] files = temporaryFolder.listFiles(new PatternFilenameFilter(".*csv"));
        assertEquals(1, files.length);
        assertEquals("1,,\n" +
                        "2,\"\",\" \"\n",
                Files.toString(files[0], Charsets.UTF_8));

        methodWatcher.execute("call syscs_util.syscs_set_global_database_property('splice.db2.import.empty_string_compatible', null)");
    }

    @Test
    public void exportNullEmpty_true_true() throws Exception {
        exportNullEmpty(true, true);
    }

    @Ignore // doesn't work: VARCHAR column "" considered to be NULL
    public void exportNullEmpty_true_false() throws Exception {
        exportNullEmpty(true, false);
    }
    @Ignore  // doesn't work: VARCHAR column "" considered to be NULL
    public void exportNullEmpty_false_true() throws Exception {
        exportNullEmpty(false, true);
    }
    @Ignore  // doesn't work: VARCHAR column "" considered to be NULL
    public void exportNullEmpty_false_false() throws Exception {
        exportNullEmpty(false, false);
    }

}
