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

package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.io.FileUtils;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import splice.com.google.common.collect.Lists;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HdfsUnsafeImportIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = HdfsUnsafeImportIT.class.getSimpleName().toUpperCase();
    protected static String TABLE_1 = "A";
    protected static String TABLE_2 = "B";
    protected static String TABLE_6 = "F";
    protected static String TABLE_11 = "K";
    protected static String TABLE_18 = "R";
    protected static String TABLE_20 = "T";
    private static final String AUTO_INCREMENT_TABLE = "INCREMENT";


    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_1, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_2, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int,PRIMARY KEY(name))");
    protected static SpliceTableWatcher spliceTableWatcher20 = new SpliceTableWatcher(TABLE_20, spliceSchemaWatcher
            .schemaName, "(i int, j int check (j<15))");
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_6, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableWatcher11 = new SpliceTableWatcher(TABLE_11, spliceSchemaWatcher
            .schemaName, "(i int default 10, j int)");
    protected static SpliceTableWatcher spliceTableWatcher18 = new SpliceTableWatcher(TABLE_18, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");

    private static SpliceTableWatcher  multiLine = new SpliceTableWatcher("mytable",spliceSchemaWatcher.schemaName,
            "(a int, b char(10),c timestamp, d varchar(100),e bigint)");
    private static SpliceTableWatcher  multiPK = new SpliceTableWatcher("withpk",spliceSchemaWatcher.schemaName,
            "(a int primary key)");


    protected static SpliceTableWatcher autoIncTableWatcher = new SpliceTableWatcher(AUTO_INCREMENT_TABLE,
            spliceSchemaWatcher.schemaName,
            "(i int generated always as " +
                    "identity, j int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher6)
            .around(spliceTableWatcher11)
            .around(spliceTableWatcher18)
            .around(spliceTableWatcher20)
            .around(multiLine)
            .around(multiPK)
            .around(autoIncTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @BeforeClass
    public static void beforeClass() throws Exception {
        createDataSet();
        BADDIR = SpliceUnitTest.createBadLogDirectory(spliceSchemaWatcher.schemaName);
        assertNotNull(BADDIR);
    }

    private static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();

        //noinspection unchecked
        new TableCreator(conn)
                .withCreate(format("create table %s.num_dt1 (i smallint, j int, k bigint, primary key(j))",
                        spliceSchemaWatcher.schemaName))
                .withInsert(format("insert into %s.num_dt1 values(?,?,?)", spliceSchemaWatcher.schemaName))
                .withRows(rows(
                        row(4256, 42031, 87049),
                        row(1140, 30751, 791),
                        row(25, 81278, 975),
                        row(-54, 62648, 3115),
                        row(57, 21099, 1081),
                        row(1430, 68915, null),
                        row(49, 19765, null),
                        row(-31, 10610, null),
                        row(-47, 34483, 40801),
                        row(7694, 20015, 52662),
                        row(35, 14202, 80476),
                        row(9393, 61174, 68211),
                        row(7058, 75830, null),
                        row(302, 5770, 53257),
                        row(3567, 15812, null),
                        row(-71, 92497, 85),
                        row(6229, 65149, 1583),
                        row(-36, 53846, 9128),
                        row(57, 95839, null),
                        row(3832, 90042, 433),
                        row(4818, 1483, 71600),
                        row(4493, 31875, 75291),
                        row(58, 85771, 3383),
                        row(9477, 77588, null),
                        row(6150, 88770, null),
                        row(8755, 44597, null),
                        row(68, 51844, 29940),
                        row(5926, 74926, 90887),
                        row(6017, 45829, 146),
                        row(8053, 45192, null)))
                .withIndex(format("create index idx1 on %s.num_dt1(k)", spliceSchemaWatcher.schemaName))
                .create();
    }

    private static File BADDIR;

    @Test
    public void testHdfsUnsafeImport() throws Exception {
        testImport(spliceSchemaWatcher.schemaName, TABLE_1, getResourceDirectory() + "importTest.in", "NAME,TITLE," +
                "AGE", 0);
    }

    @Test
    public void testImportWithPrimaryKeys() throws Exception {
        testImport(spliceSchemaWatcher.schemaName, TABLE_2, getResourceDirectory() + "importTest.in", "NAME,TITLE," +
                "AGE", 0);
    }

    @Test
    public void testImportMultiFilesPKViolations() throws Exception {
        try(PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA_UNSAFE(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "null," +  // insert column list
                        "'%s'," +  // file path
                        "','," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "-1," +    // max bad records
                        "'%s'," +  // bad record dir
                        "'true'," +  // has one line records
                        "null)",   // char set
                spliceSchemaWatcher.schemaName,multiPK.tableName, getResourceDirectory()+"/multiFilePKViolation",
                BADDIR.getCanonicalPath()))){
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());

                // IMPORT_DATA_UNSAFE can report PK violations under some circumstances (repeated PK on same batch)
            }
        }
        try(ResultSet rs = methodWatcher.executeQuery("select count(*) from "+multiPK)){
            Assert.assertTrue("Did not return a row!",rs.next());
            long c = rs.getLong(1);
            Assert.assertEquals("Incorrect row count!",9,c);
            assertFalse("Returned too many rows!",rs.next());
        }
    }

    // more tests to write:
    // test bad records at threshold and beyond threshold

    private void testImport(String schemaName, String tableName, String location, String colList, long
            badRecordsAllowed) throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA_UNSAFE(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "'%s'," +  // insert column list
                        "'%s'," +  // file path
                        "','," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "%d," +    // max bad records
                        "'%s'," +  // bad record dir
                        "null," +  // has one line records
                        "null)",   // char set
                schemaName,            tableName, colList,
                location,              badRecordsAllowed,
                BADDIR.getCanonicalPath()));


        ps.execute();
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", schemaName, tableName));
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            String name = rs.getString(1);
            String title = rs.getString(2);
            int age = rs.getInt(3);
            Assert.assertTrue("age was null!", !rs.wasNull());
            assertNotNull("Name is null!", name);
            assertNotNull("Title is null!", title);
            assertNotNull("Age is null!", age);
            results.add(String.format("name:%s,title:%s,age:%d", name, title, age));
        }
        Assert.assertTrue("no rows imported!", results.size() > 0);
    }

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testConstaintsImportNullBadDir() throws Exception {
        // DB-5017: When bad record dir is null or empty, the input file dir becomes the bad record dir
        String inputFileName =  "constraintViolation.csv";
        String inputFileOrigin = getResourceDirectory() +inputFileName;
        // copy the given input file under a temp folder so that it will get cleaned up
        // this used to go under the "target/test-classes" folder but doesn't work when we execute test from
        // a different location.
        File newImportFile = tempFolder.newFile(inputFileName);
        FileUtils.copyFile(new File(inputFileOrigin), newImportFile);
        assertTrue("Import file copy failed: "+newImportFile.getCanonicalPath(), newImportFile.exists());
        String badFileName = newImportFile.getParent()+"/" +inputFileName+".bad";

        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA_UNSAFE(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "null," +  // insert column list
                        "'%s'," +  // file path
                        "','," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "%d," +    // max bad records
                        "null," +  // bad record dir
                        "null," +  // has one line records
                        "null)",   // char set
                spliceSchemaWatcher.schemaName, TABLE_20,
                newImportFile.getCanonicalPath(), 0));
        try {
            ps.execute();
            fail("Too many bad records.");
        } catch (SQLException e) {
            assertEquals("Expected too many bad records, but got: "+e.getLocalizedMessage(), "SE009", e.getSQLState());
        }
        boolean exists = existsBadFile(new File(newImportFile.getParent()), inputFileName+".bad");
        assertTrue("Bad file " +badFileName+" does not exist.", exists);
    }

    @Test
    public void testHdfsImportGzipFile() throws Exception {
        testImport(spliceSchemaWatcher.schemaName, TABLE_6, getResourceDirectory() + "importTest.in.gz", "NAME,TITLE," +
                "AGE", 0);
    }


    @Test
    public void testImportTabWithDefaultColumnValue() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA_UNSAFE(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "'J'," +   // insert column list
                        "'%s'," +  // file path
                        "null," +  // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "%d," +    // max bad records
                        "'%s'," +  // bad record dir
                        "null," +  // has one line records
                        "null)",   // char set
                spliceSchemaWatcher.schemaName,
                TABLE_11,
                getResourceDirectory() + "default_column.txt", 0,
                BADDIR.getCanonicalPath()));
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", spliceSchemaWatcher.schemaName,
                TABLE_11));
        while (rs.next()) {
            Assert.assertEquals(10, rs.getInt(1));
            Assert.assertEquals(1, rs.getInt(2));
        }
    }

    @Test
    public void testImportTableWithAutoIncrementColumn() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA_UNSAFE(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "'J'," +   // insert column list
                        "'%s'," +  // file path
                        "null," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "%d," +    // max bad records
                        "'%s'," +  // bad record dir
                        "null," +  // has one line records
                        "null)",   // char set
                spliceSchemaWatcher.schemaName,
                AUTO_INCREMENT_TABLE,
                getResourceDirectory() + "default_column.txt", 0,
                BADDIR.getCanonicalPath()));
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", spliceSchemaWatcher.schemaName,
                AUTO_INCREMENT_TABLE));
        while (rs.next()) {
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals(1, rs.getInt(2));
        }
    }

    /**
     * Tests an import scenario where a quoted column is missing the end quote and the EOF is
     * reached before the maximum number of lines in a quoted column is exceeded.
     *
     * @throws Exception
     */
    @Test
    public void testMissingEndQuoteForQuotedColumnEOF() throws Exception {
        String badDirPath = BADDIR.getCanonicalPath();
        String csvPath = getResourceDirectory() + "import/missing-end-quote/employees.csv";
        try {
            testMissingEndQuoteForQuotedColumn(spliceSchemaWatcher.schemaName, TABLE_18, csvPath, "NAME,TITLE,AGE",
                    badDirPath, 0, 1, "false");
            fail("Expected to many bad records.");
        } catch (SQLException e) {
            assertEquals("Expected too many bad records but got: "+e.getLocalizedMessage(), "SE009", e.getSQLState());
            SpliceUnitTest.assertBadFileContainsError(new File(badDirPath), "employees.csv",
                                                      null, "unexpected end of file while reading quoted column beginning on line 2 and ending on line 6");
        }
    }

    /**
     * Tests an import scenario where a quoted column is missing the end quote and the
     * maximum number of lines in a quoted column is exceeded.
     *
     * @throws Exception
     */
    @Test
    public void testMissingEndQuoteForQuotedColumnMax() throws Exception {
        String badDirPath = BADDIR.getCanonicalPath();
        String csvPath = getResourceDirectory() + "import/missing-end-quote/employeesMaxQuotedColumnLines.csv";
        try {
            testMissingEndQuoteForQuotedColumn(spliceSchemaWatcher.schemaName, TABLE_18, csvPath, "NAME,TITLE,AGE",
                    badDirPath, 0, 199999, "false");
            fail("Expected to many bad records.");
        } catch (SQLException e) {
            assertEquals("Expected too many bad records but got: "+e.getLocalizedMessage(), "SE009", e.getSQLState());
            SpliceUnitTest.assertBadFileContainsError(new File(badDirPath), "employeesMaxQuotedColumnLines.csv",
                                                      null, "Quoted column beginning on line 3 has exceed the maximum allowed lines");
        }
    }

    /**
     * Worker method for import tests related to CSV files that are missing the end quote for a quoted column.
     *
     * @param schemaName     table schema
     * @param tableName      table name
     * @param importFilePath full path to the import file
     * @param colList        list of columns and their order
     * @param badDir         where to place the error file
     * @param failErrorCount how many errors do we allow before failing the whole import
     * @param importCount    verification of number of rows imported
     * @param oneLineRecords whether the import file has one record per line or records span lines
     * @throws Exception
     */
    private void testMissingEndQuoteForQuotedColumn(
            String schemaName, String tableName, String importFilePath, String colList, String badDir, int
            failErrorCount, int importCount, String oneLineRecords)
            throws Exception {
        methodWatcher.executeUpdate("delete from " + schemaName + "." + tableName);
        PreparedStatement ps = methodWatcher.prepareStatement(
                format("call SYSCS_UTIL.IMPORT_DATA_UNSAFE('%s','%s','%s','%s',',',null,null,null,null,%d,'%s','%s',null)",
                        schemaName, tableName, colList, importFilePath, failErrorCount, badDir, oneLineRecords));
        ps.execute();
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", schemaName, tableName));
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            String name = rs.getString(1);
            String title = rs.getString(2);
            int age = rs.getInt(3);
            Assert.assertTrue("age was null!", !rs.wasNull());
            assertNotNull("name is null!", name);
            assertNotNull("title is null!", title);
            results.add(String.format("name:%s,title:%s,age:%d", name, title, age));
        }
        Assert.assertEquals("Incorrect number of rows imported", importCount, results.size());
    }

    @Test
    public void testCheckConstraintLoad() throws Exception {
        // File has 3 lines 2 fo which are constraint violations.
        // No error -- tolerating 2 errors so 1 row should be inserted
        helpTestConstraints(2, 1, false);
    }

    @Test
    public void testCheckConstraintLoad2() throws Exception {
        // File has 3 lines 2 fo which are constraint violations.
        // No error -- tolerating 3 errors so 1 row should be inserted
        helpTestConstraints(3, 1, false);
    }

    @Test
    public void testCheckConstraintLoadError() throws Exception {
        // File has 3 lines 2 fo which are constraint violations.
        // Expect error
        helpTestConstraints(1, 0, true);
    }

    @Test
    public void testCheckConstraintLoadPermissive() throws Exception {
        // File has 3 lines 2 fo which are constraint violations.
        // No error -- we are tolerating ALL errors so 1 row should be inserted
        helpTestConstraints(-1, 1, false);
    }

    @Test
    public void testCheckConstraintLoadPermissive2() throws Exception {
        // File has 3 lines 2 fo which are constraint violations.
        // No error -- we are tolerating ALL errors so 1 row should be inserted
        // Notice ANYTHING < 0 is considered -1 (permissive)
        helpTestConstraints(-20, 1, false);
    }

    public void helpTestConstraints(long maxBadRecords, int insertRowsExpected, boolean expectException) throws Exception {
        String tableName = "CONSTRAINED_TABLE";
        TableDAO td = new TableDAO(methodWatcher.getOrCreateConnection());
        td.drop(spliceSchemaWatcher.schemaName, tableName);

        methodWatcher.getOrCreateConnection().createStatement().executeUpdate(
                format("create table %s ",spliceSchemaWatcher.schemaName+"."+tableName)+
                        "(EMPNO CHAR(6) NOT NULL CONSTRAINT EMP_PK PRIMARY KEY, " +
                        "SALARY DECIMAL(9,2) CONSTRAINT SAL_CK CHECK (SALARY >= 10000), " +
                        "BONUS DECIMAL(9,2),TAX DECIMAL(9,2),CONSTRAINT BONUS_CK CHECK (BONUS > TAX))");

        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA_UNSAFE(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "'%s'," +  // insert column list
                        "'%s'," +  // file path
                        "','," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "%d," +    // max bad records
                        "'%s'," +  // bad record dir
                        "null," +  // has one line records
                        "null)",   // char set
                spliceSchemaWatcher.schemaName, tableName,
                "EMPNO,SALARY,BONUS,TAX",
                getResourceDirectory() + "test_data/salary_check_constraint.csv",
                maxBadRecords, BADDIR.getCanonicalPath()));

        try {
            ps.execute();
        } catch (SQLException e) {
            if (! expectException) {
                throw e;
            }
        }
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", spliceSchemaWatcher.schemaName,
                tableName));
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            String name = rs.getString(1);
            String title = rs.getString(2);
            int age = rs.getInt(3);
            Assert.assertTrue("age was null!", !rs.wasNull());
            assertNotNull("Name is null!", name);
            assertNotNull("Title is null!", title);
            assertNotNull("Age is null!", age);
            results.add(String.format("name:%s,title:%s,age:%d", name, title, age));
        }
        Assert.assertEquals(format("Expected %s row1 imported", insertRowsExpected), insertRowsExpected, results.size());

        boolean exists = existsBadFile(BADDIR, "salary_check_constraint.csv.bad");
        String badFile = getBadFile(BADDIR, "salary_check_constraint.csv.bad");
        assertTrue("Bad file " +badFile+" does not exist.",exists);
        List<String> badLines = Files.readAllLines((new File(BADDIR, badFile)).toPath(), Charset.defaultCharset());
        assertEquals("Expected 2 lines in bad file "+badFile, 2, badLines.size());
        assertContains(badLines, containsString("BONUS_CK"));
        assertContains(badLines, containsString(spliceSchemaWatcher.schemaName+"."+tableName));
        assertContains(badLines, containsString("SAL_CK"));
    }

    private static void assertContains(List<String> collection, Matcher<String> target) {
        for (String source : collection) {
            if (target.matches(source)) {
                return;
            }
        }
        fail("Expected to contain " + target.toString() + " in: " + collection);
    }
}
