/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * Test importing a file into a table with default or generated column values.
 * <p/>
 * Here are the main rules and their implications for default and generated column values we're testing for import:
 * <ol>
 *     <li>If not all columns are represented in CSV, then a column reference list must be provided to the import
 *     procedure.</li>
 *     <li>If all columns are represented in CSV, then a column reference list is not required.</li>
 *     <li>Empty columns, including String columns where the value in the CSV has zero length, are always considered to
 *     represent null.</li>
 * </ol>
 * See also http://docstest.splicemachine.com/Search.html#search-import
 * <p/>
 * The test flow is:
 * <ol>
 *     <li>Create CSV file, row by row, using one column by which to order the verification query so results can be
 *     compared deterministically and write the file</li>
 *     <li>Create expected results including default values with which to compare to the ordered query results</li>
 *     <li>Run the import optionally specifying a column insertion list</li>
 *     <li>Execute "select *" on the table ordering the results by the chosen order by column. <b>NOTE</b>: an
 *     alternative query can be specified to verify results if need be. Just make sure to change expected results to
 *     fit.</li>
 *     <li>Compare actual query results with the supplied expected results</li>
 * </ol>
 * Notes:
 * <uL>
 *     <li>The table is created using the same name as the CSV file with the schema definition given in the test.</li>
 *     <li>A SQL Error Code can be specified for test imports that expect an exception be thrown.</li>
 *     <li>You can create a new test by simply copying an existing one. Be sure to change the CSV file name, which
 *     changes the table name. Document your test as to what it's testing.</li>
 *     <li><b>NOTE</b>: If the import procedure is not given <i>any</i> columns in the column list argument, HdfsImport
 *     can only assume the CSV file contains <i>all</i> column values and will create a column list containing <i>all</i>
 *     table columns.  This is particularly troubling with default and generated columns where the CSV may not
 *     contain all columns, or something may get inserted that was unintended, or you might get an error for attempting
 *     to insert a value in a <i>GENERATED ALWAYS</i> column. <b>When you are importing into a table with default or
 *     generated values, you should always supply a column list (see rule number one above).</b></li>
 * </uL>
 *
 */
public class ImportDefaultValueIT {
    public static final String UTF_8_CHAR_SET_STR = "UTF-8";
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = ImportDefaultValueIT.class.getSimpleName().toUpperCase();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static File BADDIR;
    private static File IMPORTDIR;
    private TestConnection conn;

    @BeforeClass
    public static void beforeClass() throws Exception {
        BADDIR = SpliceUnitTest.createBadLogDirectory(spliceSchemaWatcher.schemaName);
        assertNotNull(BADDIR);
        IMPORTDIR = SpliceUnitTest.createImportFileDirectory(spliceSchemaWatcher.schemaName);
        assertNotNull(IMPORTDIR);
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).around(spliceSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Before
    public void setUp() throws Exception{
        this.conn = methodWatcher.getOrCreateConnection();
        this.conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception{
        this.conn.rollback();
        conn.reset();
    }

    @Test
    public void allValuesNoColumnList() throws Exception {
        String tableName = "all_values_given";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "ac").expected("1", "ac")
                                                                   .toFileRow("2", "a").expected("2", "a")
                                                                   .toFileRow("3", "ab").expected("3", "ab")
                                                                   .toFileRow("4", "b").expected("4", "b")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 CHAR(3)DEFAULT'abc'", "null", fileName,
                                        map, "COL1", "", null, null);
    }

    @Test
    public void allValuesSomeNullNoColumnList() throws Exception {
        // make sure we can import nulls explicitly
        String tableName = "explicit_nulls";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "ac").expected("1", "ac")
                                                                   .toFileRow("2", "NULL").expected("2", "abc")
                                                                   .toFileRow("3", "ab").expected("3", "ab")
                                                                   .toFileRow("4", "NULL").expected("4", "abc")
                                                                   .fill(writer);

        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 CHAR(3)DEFAULT'abc'", "null", fileName,
                                        map, "COL1", "", null, null);
    }

    @Test
    public void overrideDefaultValuesWithColumnList() throws Exception {
        // make sure, if we spec a value for a default col, we get the overridden value
        String tableName = "override_default_values_with_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "ac").expected("1", "ac")
                                                                   .toFileRow("2", "a").expected("2", "a")
                                                                   .toFileRow("3", "ab").expected("3", "ab")
                                                                   .toFileRow("4", "b").expected("4", "b")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 CHAR(3)DEFAULT'abc'", "COL1,COL2", fileName,
                                        map, "COL1", "", null, null);
    }

    @Test
    public void oneColumnValuesWithColumnList() throws Exception {
        // straight-forward import without value spec'd for default column
        String tableName = "one_column";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1").expected("1", "abc")
                                                                   .toFileRow("2").expected("2", "abc")
                                                                   .toFileRow("3").expected("3", "abc")
                                                                   .toFileRow("4").expected("4", "abc")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 CHAR(3)DEFAULT'abc'", "COL1", fileName,
                                        map, "COL1", "", null, null);
    }

    @Test
    public void commasEmptyValuesAndNoColumnList() throws Exception {
        // make sure we get NULL for empty CSV column even when it has a default
        String tableName = "commas_no_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "ac").expected("1", "ac")
                                                                   .toFileRow("2", "").expected("2", "abc")
                                                                   .toFileRow("3", "ab").expected("3", "ab")
                                                                   .toFileRow("4", "").expected("4", "abc")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 CHAR(3)DEFAULT'abc'", "null", fileName,
                                        map, "COL1", "", null, null);
    }

    @Test
    public void errorNotAllColumnsInFile() throws Exception {
        // this file has lines with uneven number of col separators. should be an error whether or not col list spec'd
        String tableName = "not_enough_values_no_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "ac")
                                                                   .toFileRow("2")
                                                                   .toFileRow("3", "ab")
                                                                   .toFileRow("4")
                                                                   .fill(writer);
        writer.close();

        // expect exception cause there's no col list and not all cols in csv
        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 CHAR(3)DEFAULT'abc'", "null", fileName,
                                        map, "COL1", "SE009", "XIE0A", null, 1);
    }

    @Test
    public void errorMissingValuesWithColumnList() throws Exception {
        // this file has lines with uneven number of col separators. should be an error whether or not col list spec'd
        String tableName = "not_enough_values_has_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "ac")
                                                                   .toFileRow("2")
                                                                   .toFileRow("3", "ab")
                                                                   .toFileRow("4")
                                                                   .fill(writer);
        writer.close();

        // CSV contains varying number of columns, should be an error
        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 CHAR(3)DEFAULT'abc'", "COL1,COL2", fileName,
                                        map, "COL1", "SE009", "XIE0A", null, 1);
    }

    @Test
    public void defaultColumnInMiddleWithColumnList() throws Exception {
        // if a default column is an interior column, a column list must be supplied
        String tableName = "default_col_middle";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "ac").expected("1", "abc", "ac")
                                                                   .toFileRow("2", "").expected("2", "abc", "NULL")
                                                                   .toFileRow("3", "ab").expected("3", "abc", "ab")
                                                                   .toFileRow("4", "ca").expected("4", "abc", "ca")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 CHAR(3)DEFAULT'abc', COL3 CHAR(2)", "COL1,COL3",
                                        fileName, map, "COL1", "", null, null);
    }

    @Test
    public void defaultTimestampNoColumnList() throws Exception {
        // TODO: This test was verifying old import behavior that is now unsupported. Not sure of its value anymore.
        // col2 is a timestamp column with default. If an empty value is given in CSV, we should see the default value
        // in query results. This is how it worked in Lassen
        String tableName = "default_timestamp_middle";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "", "ac").expected("1", "2011-03-17 15:52:25.0", "ac")
                                                                   .toFileRow("2", "2016-01-29 15:38:45", "").expected("2", "2016-01-29 15:38:45.0", "NULL")
                                                                   .toFileRow("3", "", "ab").expected("3", "2011-03-17 15:52:25.0", "ab")
                                                                   .toFileRow("4", "2016-01-29 15:38:45", "ca").expected("4", "2016-01-29 15:38:45.0", "ca")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 TIMESTAMP DEFAULT {ts '2011-03-17 15:52:25'}, COL3 CHAR(2)",
                                        "null", fileName, map, "COL1", "", null, null);
    }

    @Test
    public void defaultTimestampMissingWithColumnList() throws Exception {
        // col2 is a timestamp column with default. If no value is given in CSV and we supply the outer columns in
        // the insert column list, we get correct default value for the timestamp col.
        String tableName = "default_timestamp_middle_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "ac").expected("1", "2011-03-17 15:52:25.0", "ac")
                                                                   .toFileRow("2", "").expected("2", "2011-03-17 15:52:25.0", "NULL")
                                                                   .toFileRow("3", "ab").expected("3", "2011-03-17 15:52:25.0", "ab")
                                                                   .toFileRow("4", "ca").expected("4", "2011-03-17 15:52:25.0", "ca")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 TIMESTAMP DEFAULT {ts '2011-03-17 15:52:25'}, COL3 CHAR(2)",
                                        "COL1,COL3", fileName, map, "COL1", "", null, null);
    }

    @Test
    public void defaultTimestampMissingNoColumnList() throws Exception {
        // TODO: This test was verifying old import behavior that is now unsupported. Not sure of its value anymore.
        // col2 is a timestamp column with default. If an empty value is given in CSV, we should see the default value
        // in query results. This is how it worked in Lassen
        String tableName = "default_timestamp_middle_col_list10";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "", "ac").expected("1", "2011-03-17 15:52:25.0", "ac")
                                                                   .toFileRow("2", "", "").expected("2", "2011-03-17 15:52:25.0", "NULL")
                                                                   .toFileRow("3", "", "ab").expected("3", "2011-03-17 15:52:25.0", "ab")
                                                                   .toFileRow("4", "", "ca").expected("4", "2011-03-17 15:52:25.0", "ca")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 TIMESTAMP DEFAULT {ts '2011-03-17 15:52:25'}, COL3 CHAR(2)",
                                        "null", fileName, map, "COL1", "", null, null);
    }

    @Test
    public void errorDefaultColumnInMiddleNoColumnList() throws Exception {
        // if a default column is an interior column, a column list must be supplied
        String tableName = "default_col_middle_no_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "ac")
                                                                   .toFileRow("2", "")
                                                                   .toFileRow("3", "ab")
                                                                   .toFileRow("4", "ca")
                                                                   .fill(writer);
        writer.close();

        // Exception expected - CSV has only two cols and we didn't spec a col list
        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 CHAR(3)DEFAULT'abc', COL3 CHAR(2)", "null",
                                        fileName, map, "COL1", "SE009", "XIE0A", null, 1);
    }

    @Test
    public void errorGeneratedAlwaysOverrideValuesColumnNoColumnList() throws Exception {
        // make sure, if we spec a value for a generated col, we get the overridden value
        String tableName = "generated_always_col_override_values_no_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "10", "ac").expected("1", "10", "ac")
                                                                   .toFileRow("2", "20", "").expected("2", "20", "NULL")
                                                                   .toFileRow("3", "30", "ab").expected("3", "30", "ab")
                                                                   .toFileRow("4", "40", "ca").expected("4", "40", "ca")
                                                                   .fill(writer);
        writer.close();

        // Can't modify an "always" generated identity
        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 int generated always as identity, COL3 CHAR(2)",
                                        "null", fileName, map, "COL1", "42Z23", null, null);
    }

    @Test
    public void errorGeneratedAlwaysEmptyColumnList() throws Exception {
        // empty col list spec'd so should turn into all columns in table, which is an
        // error in this case cause the file doesn't have all
        String tableName = "generated_always_no_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);


        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("ac").expected("1", "ac")
                                                                   .toFileRow("ac").expected("2", "ac")
                                                                   .toFileRow("ac").expected("3", "ac")
                                                                   .toFileRow("ac").expected("4", "ac")
                                                                   .fill(writer);
        writer.close();

        // Attempt to modify identity col. Since no col list was given that defaults to ALL columns on insert.
        // This gets thrown at statement prepare time when it sees you've specified an illegal insert column, not when
        // actually parsing the CSV.
        helpTestDefaultAndGeneratedCols(tableName, "COL1 int generated always as identity, COL2 CHAR(2)",
                                        "", fileName, map, "COL1", "42Z23", null, null);
    }

    @Test
    public void errorGeneratedAlwaysNoColumnList() throws Exception {
        // no col list spec'd so should turn into all columns in table, which is an
        // error in this case cause the file doesn't have all
        String tableName = "generated_always_no_col_list23";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);


        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("ac").expected("1", "ac")
                                                                   .toFileRow("ac").expected("2", "ac")
                                                                   .toFileRow("ac").expected("3", "ac")
                                                                   .toFileRow("ac").expected("4", "ac")
                                                                   .fill(writer);
        writer.close();

        // Attempt to modify identity col. Since no col list was given that defaults to ALL columns on insert.
        // This gets thrown at statement prepare time when it sees you've specified an illegal insert column, not when
        // actually parsing the CSV.
        helpTestDefaultAndGeneratedCols(tableName, "COL1 int generated always as identity, COL2 CHAR(2)",
                                        "null", fileName, map, "COL1", "42Z23", null, null);
    }

    @Test
    public void generatedAlwaysWithColumnList() throws Exception {
        // col list spec'd for values imported from file. Should see generated values in COL1
        String tableName = "generated_always_with_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);


        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("ac").expected("1", "ac")
                                                                   .toFileRow("ac").expected("2", "ac")
                                                                   .toFileRow("ac").expected("3", "ac")
                                                                   .toFileRow("ac").expected("4", "ac")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 int generated always as identity, COL2 CHAR(2)",
                                        "COL2", fileName, map, "COL1", "", null, null);
    }

    @Test
    public void generatedByDefaultOverrideValuesColumnNoColumnList() throws Exception {
        // test generated columns can be overridden
        String tableName = "generated_col_override_values_no_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "10", "ac").expected("1", "10", "ac")
                                                                   .toFileRow("2", "20", "").expected("2", "20", "NULL")
                                                                   .toFileRow("3", "30", "ab").expected("3", "30", "ab")
                                                                   .toFileRow("4", "40", "ca").expected("4", "40", "ca")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 int generated by default as identity, COL3 CHAR(2)",
                                        "null", fileName, map, "COL1", "", null, null);
    }

    @Test @Ignore("DB-5049: Intermittent assertion: Can't find conglomerate descriptor for seqConglomId: <xxxx>")
    public void errorGeneratedByDefaultColumnNoColumnList() throws Exception {
        // "Generated by default" column will not accept NULL value
        String tableName = "generated_col_with_nulls_no_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("1", "", "ac").expected("1", "1", "ac")
                                                                   .toFileRow("2", "", "").expected("2", "2", "NULL")
                                                                   .toFileRow("3", "", "ab").expected("3", "3", "ab")
                                                                   .toFileRow("4", "", "ca").expected("4", "4", "ca")
                                                                   .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, COL2 int generated by default as identity, COL3 CHAR(2)",
                                        "null", fileName, map, "COL1", "SE009", null, null, 0);
    }

    @Test
    public void generatedByDefaultColumnWithColumnList() throws Exception {
        // test that we get identity counter with diff inc'd value in each row
        String tableName = "generated_col_with_col_list";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                                                                   .toFileRow("zz", "ac").expected("1")
                                                                   .toFileRow("yy", "").expected("2")
                                                                   .toFileRow("xx", "ab").expected("3")
                                                                   .toFileRow("ww", "ca").expected("4")
                                                                   .fill(writer);
        writer.close();

        // create an alternative query and expected result to verify cause row insertion order is arbitrary and can't
        // compare whole result set - first row may get gen'd ID 3, 2nd 1, etc.
        String alternateQuery = String.format("select %s from %s.%s order by %s",
                                              "COL2", spliceSchemaWatcher.schemaName, tableName, "COL2");
        helpTestDefaultAndGeneratedCols(tableName, "COL1 CHAR(2), COL2 int generated by default as identity, COL3 CHAR(2)",
                                        "COL1,COL3", fileName, map, "COL2", "", null, alternateQuery);
    }

    @Test
    public void importSpacesForNotNullVarcharCols() throws Exception {
        // DB-4002: import column with only spaces imports as null
        String tableName = "import_spaces_in_non_null_varchar";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                .toFileRow("1", "\"    \"", "jean", "driggers").expected("1", "    ", "jean", "driggers")
                .toFileRow("2", "erin", "\"    \"", "driggers").expected("2", "erin", "    ", "driggers")
                .toFileRow("3", "erin", "jean", "\"    \"").expected("3", "erin", "jean", "    ")
                .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL1 INT, firstName varchar(20) NOT NULL, middleName varchar(20) NOT NULL default '', lastName varchar(20) NOT NULL default 'test'",
                "null", fileName, map, "COL1", "", null, null);
    }

    @Test
    public void importNullIntoDefaultNullCol() throws Exception {
        // DB-3940: can import a NULL value into a DEFAULT NULL column
        String tableName = "import_null_into_default_null";
        String fileName = IMPORTDIR.getCanonicalPath()+"/"+tableName+".csv";
        PrintWriter writer = new PrintWriter(fileName, UTF_8_CHAR_SET_STR);

        SpliceUnitTest.ResultList map = SpliceUnitTest.ResultList.create()
                .toFileRow("1", "1", "1.0", "a", "1").expected("1", "1", "1.000", "a", "1")
                .toFileRow("2", "1", "", "a", "1").expected("2", "1", "NULL", "a", "1")
                .toFileRow("3", "1", "1.0", "", "1").expected("3", "1", "1.000", "NULL", "1")
                .toFileRow("4", "1", "1.0", "", "1").expected("4", "1", "1.000", "NULL", "1")
                .toFileRow("5", "1", "1.0", "a", "1").expected("5", "1", "1.000", "a", "1")
                .toFileRow("6", "1", "1.0", "a", "").expected("6", "1", "1.000", "a", "NULL")
                .toFileRow("7", "1", "1.0", "", "1").expected("7", "1", "1.000", "NULL", "1")
                .toFileRow("8", "1", "1.0", "a", "1").expected("8", "1", "1.000", "a", "1")
                .fill(writer);
        writer.close();

        helpTestDefaultAndGeneratedCols(tableName, "COL0 INT, col1 integer, col2 numeric(12,3) default NULL, col3 char(3), col4 integer",
                "null", fileName, map, "COL0", "", null, null);
    }

    //==============================================================================================================

    private void helpTestDefaultAndGeneratedCols(String tableName, String tableDef, String colList, String fileName,
                                                 SpliceUnitTest.ResultList expectedResultList, String orderByCol,
                                                 String sqlStateCode, String sqlStateCodeInErrorFile, String alternateQueryString)
      throws Exception {
        helpTestDefaultAndGeneratedCols(tableName, tableDef, colList, fileName, expectedResultList, orderByCol, sqlStateCode,
            sqlStateCodeInErrorFile, alternateQueryString, /*maxBadRecords=*/0);
    }

    private void helpTestDefaultAndGeneratedCols(String tableName, String tableDef, String colList, String fileName,
                                                 SpliceUnitTest.ResultList expectedResultList, String orderByCol,
                                                 String sqlStateCode, String sqlStateCodeInErrorFile, String alternateQueryString,
                                                 int maxBadRecordsAllowed)
        throws Exception {

        if (tableDef.startsWith("(")) {
            throw new Exception("Don't enclose the create table definition in parens.");
        }

        String controlTableDef = "("+tableDef+")";
        try(Statement s = conn.createStatement()){
            s.executeUpdate(String.format("create table %s ",spliceSchemaWatcher.schemaName+"."+tableName)+controlTableDef);
        }

        String importString=String.format("call SYSCS_UTIL.IMPORT_DATA("+
                        "'%s',"+  // schema name
                        "'%s',"+  // table name
                        "'%s',"+  // insert column list
                        "'%s',"+  // file path
                        "',',"+   // column delimiter
                        "'\"',"+  // character delimiter
                        "null,"+  // timestamp format
                        "null,"+  // date format
                        "null,"+  // time format
                        "%d,"+    // max bad records
                        "'%s',"+  // bad record dir
                        "null,"+  // has one line records
                        "'%s')",   // char set
                spliceSchemaWatcher.schemaName,tableName,colList,
                fileName,maxBadRecordsAllowed,
                BADDIR.getCanonicalPath(),UTF_8_CHAR_SET_STR);
        try(PreparedStatement ps=conn.prepareStatement(importString)){
            ps.execute();
            if (sqlStateCode != null && ! sqlStateCode.isEmpty()) {
                fail("Expected import exception: "+sqlStateCode+ " but got no error.  Printing \"bad\" file: "+
                         SpliceUnitTest.printBadFile(BADDIR, fileName));
            }
        } catch (SQLException e) {
            if (sqlStateCode == null || sqlStateCode.isEmpty()) {
                fail("Didn't expect exception but got: "+e.getSQLState()+" "+e.getLocalizedMessage()+".  Printing \"bad\" file: "+
                        SpliceUnitTest.printBadFile(BADDIR, fileName));
            } else if (sqlStateCodeInErrorFile != null && ! sqlStateCodeInErrorFile.isEmpty()) {
                SpliceUnitTest.assertBadFileContainsError(BADDIR, new File(fileName).getName(), sqlStateCodeInErrorFile, null);
            }
            assertEquals("Expected different error but got: ["+e.getSQLState()+" "+e.getLocalizedMessage()+"].  Printing \"bad\" file: "+
                             SpliceUnitTest.printBadFile(BADDIR, fileName), sqlStateCode, e.getSQLState());
            // we got the error we expected. Done.
            return;
        }
        String queryString = String.format("select * from %s.%s order by %s",
                                           spliceSchemaWatcher.schemaName, tableName, orderByCol);
        if (alternateQueryString != null && ! alternateQueryString.isEmpty()) {
            queryString = alternateQueryString;
        }

        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery(queryString)){
                SpliceUnitTest.ResultList actualResultList=SpliceUnitTest.ResultList.create();
                int nRows=0;
                int nCols=rs.getMetaData().getColumnCount();
                while(rs.next()){
                    ++nRows;
                    String[] actualRow=new String[nCols];
                    // skipping first col because it's the "control column"
                    for(int index=1;index<=nCols;++index){
                        Object object=rs.getObject(index);
                        actualRow[index-1]=rs.wasNull()?"NULL":object.toString();
                    }
                    actualResultList.expected((String[])actualRow);
                }

                assertEquals(String.format("Expected %d rows imported, got: %d: %s",expectedResultList.nRows(),nRows,
                        SpliceUnitTest.printBadFile(BADDIR,fileName)),expectedResultList.nRows(),nRows);

                assertEquals("Results differ: ",expectedResultList.toString(),actualResultList.toString());
            }
        }
    }

}
