/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Created by changli on 1/10/2019.
 */
public class ShowCreateTableIT extends SpliceUnitTest
{
    private static final String SCHEMA = ShowCreateTableIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createTables() throws Exception {
        TestConnection conn = classWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T1 (A1 INT PRIMARY KEY, B1 INT, C1 INT, D1 VARCHAR(10))")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T2 (A2 INT , B2 INT ,PRIMARY KEY(A2,B2))")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T3 (A3 INT, A2 INT, B2 INT,CONSTRAINT T3_FK_1 FOREIGN KEY (A2, B2) REFERENCES T2)")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T4 (A4 INT, B2 INT, A2 INT, A1 INT, CONSTRAINT T4_FK_1 FOREIGN KEY (A2, B2) REFERENCES T2, CONSTRAINT T4_FK_2 FOREIGN KEY (A1) REFERENCES T1 )")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T5 (A5 INT, B5F INT, A5F INT,CONSTRAINT T5_FK_1 FOREIGN KEY (A5F, B5F) REFERENCES T2(A2,B2) )")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T6 (A6 INT, B6 INT, C6 INT, D6 INT UNIQUE, CONSTRAINT U_T6_1 UNIQUE (A6), CONSTRAINT U_T6_2 UNIQUE (B6,C6) )")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T7 (A7 INT, B7 INT CONSTRAINT B7_CONSTRAINT CHECK (B7 > 100) , C7 CHAR(1) CONSTRAINT C7_CONSTRAINT CHECK (C7 IN ('B', 'L', 'D', 'S','''','\"')))")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T8 (I INT GENERATED ALWAYS AS IDENTITY, CH CHAR(50) DEFAULT 'HELLO', D DECIMAL(5,2) DEFAULT 2.2)")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T9 (A9 INT, B9 INT, PRIMARY KEY(B9,A9))")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T11 (A11 INT, B2 INT, A2 INT,CONSTRAINT T11_FK_1 FOREIGN KEY (A2, B2) REFERENCES T2 ON UPDATE RESTRICT ON DELETE RESTRICT)")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T12 (A12 INT, B2 INT, A2 INT,CONSTRAINT T12_FK_1 FOREIGN KEY (A2, B2) REFERENCES T2 ON UPDATE NO ACTION ON DELETE NO ACTION)")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T13 (\"a13\" INT, \"b13\" INT, \"c13\" INT, \"d13\" INT default -1, CONSTRAINT U_T13_1 UNIQUE (\"d13\"),constraint PK_T13 PRIMARY KEY (\"c13\",\"a13\"))")
                .create();
        new TableCreator(conn)
                .withCreate("CREATE TABLE T14 (A14 INT, B14 INT, C14 INT, D14 INT, CONSTRAINT T14_FK_1 FOREIGN KEY (C14, a14) REFERENCES T13(\"c13\", \"a13\") ON UPDATE NO ACTION ON DELETE NO ACTION)")
                .create();
    }

    @AfterClass
    public static void cleanup()
    {
        classWatcher.closeAll();
    }

    static File tempDir;

    @BeforeClass
    public static void createTempDirectory() throws Exception
    {
        tempDir = createTempDirectory(SCHEMA);
    }

    @AfterClass
    public static void deleteTempDirectory() throws Exception
    {
        deleteTempDirectory(tempDir);
    }

    /// this will return the temp directory, that is created on demand once for each test
    public String getExternalResourceDirectory() throws Exception
    {
        return tempDir.toString() + "/";
    }

    @Test
    public void testSimpleTableDefinition() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T1')");
        rs.next();
        String ddl = rs.getString(1);
        Pattern pattern = Pattern.compile("SQL\\d+");
        Matcher m1 = pattern.matcher(ddl);
        String csName = null;
        while (m1.find())
            csName = m1.group();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T1\" (\n" +
                "\"A1\" INTEGER NOT NULL\n" +
                ",\"B1\" INTEGER\n" +
                ",\"C1\" INTEGER\n" +
                ",\"D1\" VARCHAR(10)\n" +
                ", CONSTRAINT " + csName +" PRIMARY KEY(\"A1\")) ;", ddl);
    }

    @Test
    public void testPrimaryKey() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T2')");
        rs.next();
        String ddl = rs.getString(1);
        Pattern pattern = Pattern.compile("SQL\\d+");
        Matcher m1 = pattern.matcher(ddl);
        String csName = null;
        while (m1.find())
            csName = m1.group();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T2\" (\n" +
                "\"A2\" INTEGER NOT NULL\n" +
                ",\"B2\" INTEGER NOT NULL\n" +
                ", CONSTRAINT " + csName + " PRIMARY KEY(\"A2\",\"B2\")) ;", ddl);
    }

    @Test
    public void testPrimaryKeyReverseOrder() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T9')");
        rs.next();
        String ddl = rs.getString(1);
        Pattern pattern = Pattern.compile("SQL\\d+");
        Matcher m1 = pattern.matcher(ddl);
        String csName = null;
        while (m1.find())
            csName = m1.group();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T9\" (\n" +
                "\"A9\" INTEGER NOT NULL\n" +
                ",\"B9\" INTEGER NOT NULL\n" +
                ", CONSTRAINT " + csName + " PRIMARY KEY(\"B9\",\"A9\")) ;", ddl);
    }
    @Test
    public void testSingleForeignKey() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T3')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T3\" (\n" +
                "\"A3\" INTEGER\n" +
                ",\"A2\" INTEGER\n" +
                ",\"B2\" INTEGER\n" +
                ", CONSTRAINT T3_FK_1 FOREIGN KEY (\"A2\",\"B2\") REFERENCES \"SHOWCREATETABLEIT\".\"T2\"(\"A2\",\"B2\") ON UPDATE NO ACTION ON DELETE NO ACTION) ;", rs.getString(1));
    }

    @Test
    public void testMultipleForeignKeys() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T4')");
        rs.next();

        checkEqualIgnoreConstraintOrder("CREATE TABLE \"SHOWCREATETABLEIT\".\"T4\" (\n" +
                "\"A4\" INTEGER\n" +
                ",\"B2\" INTEGER\n" +
                ",\"A2\" INTEGER\n" +
                ",\"A1\" INTEGER\n" +
                ", CONSTRAINT T4_FK_1 FOREIGN KEY (\"A2\",\"B2\") REFERENCES \"SHOWCREATETABLEIT\".\"T2\"(\"A2\",\"B2\") ON UPDATE NO ACTION ON DELETE NO ACTION, CONSTRAINT T4_FK_2 FOREIGN KEY (\"A1\") REFERENCES \"SHOWCREATETABLEIT\".\"T1\"(\"A1\") ON UPDATE NO ACTION ON DELETE NO ACTION) ;", rs.getString(1));
    }

    @Test
    public void testForeignKeyswithDifferentReferenceColumnName() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T5')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T5\" (\n" +
                "\"A5\" INTEGER\n" +
                ",\"B5F\" INTEGER\n" +
                ",\"A5F\" INTEGER\n" +
                ", CONSTRAINT T5_FK_1 FOREIGN KEY (\"A5F\",\"B5F\") REFERENCES \"SHOWCREATETABLEIT\".\"T2\"(\"A2\",\"B2\") ON UPDATE NO ACTION ON DELETE NO ACTION) ;", rs.getString(1));
    }

    @Test
    public void testForeignKeyRestrict() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T11')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T11\" (\n" +
                "\"A11\" INTEGER\n" +
                ",\"B2\" INTEGER\n" +
                ",\"A2\" INTEGER\n" +
                ", CONSTRAINT T11_FK_1 FOREIGN KEY (\"A2\",\"B2\") REFERENCES \"SHOWCREATETABLEIT\".\"T2\"(\"A2\",\"B2\") ON UPDATE RESTRICT ON DELETE RESTRICT) ;", rs.getString(1));
    }

    @Test
    public void testForeignKeyNoAction() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T12')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T12\" (\n" +
                "\"A12\" INTEGER\n" +
                ",\"B2\" INTEGER\n" +
                ",\"A2\" INTEGER\n" +
                ", CONSTRAINT T12_FK_1 FOREIGN KEY (\"A2\",\"B2\") REFERENCES \"SHOWCREATETABLEIT\".\"T2\"(\"A2\",\"B2\") ON UPDATE NO ACTION ON DELETE NO ACTION) ;", rs.getString(1));
    }

    @Test
    public void testUnique() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T6')");
        rs.next();
        String ddl = rs.getString(1);
        Pattern pattern = Pattern.compile("SQL\\d+");
        Matcher m1 = pattern.matcher(ddl);
        String csName = null;
        while (m1.find())
            csName = m1.group();

        String expectedDDL = "CREATE TABLE \"SHOWCREATETABLEIT\".\"T6\" (\n" +
                "\"A6\" INTEGER\n" +
                ",\"B6\" INTEGER\n" +
                ",\"C6\" INTEGER\n" +
                ",\"D6\" INTEGER\n" +
                ", CONSTRAINT " + csName + " UNIQUE (\"D6\"), CONSTRAINT U_T6_1 UNIQUE (\"A6\"), CONSTRAINT U_T6_2 UNIQUE (\"B6\",\"C6\")) ;";
        checkEqualIgnoreConstraintOrder(expectedDDL, ddl);
    }

    @Test
    public void testCheck() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T7')");
        rs.next();
        checkEqualIgnoreConstraintOrder("CREATE TABLE \"SHOWCREATETABLEIT\".\"T7\" (\n" +
                "\"A7\" INTEGER\n" +
                ",\"B7\" INTEGER\n" +
                ",\"C7\" CHAR(1)\n" +
                ", CONSTRAINT B7_CONSTRAINT CHECK (B7 > 100), CONSTRAINT C7_CONSTRAINT CHECK (C7 IN ('B', 'L', 'D', 'S','''','\"'))) ;", rs.getString(1));
    }

    @Test
    public void testDefaultValue() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T8')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T8\" (\n" +
                "\"I\" INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)\n" +
                ",\"CH\" CHAR(50) DEFAULT 'HELLO'\n" +
                ",\"D\" DECIMAL(5,2) DEFAULT 2.2\n" +
                ") ;", rs.getString(1));
    }

    @Test
    public void testNonExistSchema() throws Exception
    {
        try
        {
            methodWatcher.execute("call syscs_util.SHOW_CREATE_TABLE('nonexist','t1')");
            Assert.fail("Expected to fail");
        }
        catch (SQLException e)
        {
            String sqlState = e.getSQLState();
            Assert.assertEquals("42Y07",sqlState);
        }
    }

    @Test
    public void testNonExistTable() throws Exception
    {
        try
        {
            methodWatcher.execute("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','foo')");
            Assert.fail("Expected to fail");
        }
        catch (SQLException e)
        {
            String sqlState = e.getSQLState();
            Assert.assertEquals("XIE0M",sqlState);
        }
    }

    @Test
    public void testDateColumnHavingDefaultValue() throws Exception
    {
        String DDL = "CREATE TABLE \"SHOWCREATETABLEIT\".\"T19\"\n" +
                "(\n" +
                "\"I\" DATE DEFAULT '2019-01-01'\n" +
                ",\"J\" DATE DEFAULT CURRENT_DATE\n" +
                ")";
        methodWatcher.executeUpdate(DDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T19')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T19\" (\n" +
                "\"I\" DATE DEFAULT '2019-01-01'\n" +
                ",\"J\" DATE DEFAULT CURRENT_DATE\n" +
                ") ;", rs.getString(1));

    }

    @Test
    public void testTimeColumnHavingDefaultValue() throws Exception
    {
        String DDL = "CREATE TABLE \"SHOWCREATETABLEIT\".\"T18\"\n" +
                "(\n" +
                "\"I\" TIME DEFAULT '18:18:18'\n" +
                ",\"J\" TIME DEFAULT CURRENT_TIME\n" +
                ")";
        methodWatcher.executeUpdate(DDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T18')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T18\" (\n" +
                "\"I\" TIME DEFAULT '18:18:18'\n" +
                ",\"J\" TIME DEFAULT CURRENT_TIME\n" +
                ") ;", rs.getString(1));

    }

    @Test
    public void testTimestampColumnHavingDefaultValue() throws Exception
    {
        String DDL = "CREATE TABLE \"SHOWCREATETABLEIT\".\"T15\"\n" +
                "(\n" +
                "\"I\" TIMESTAMP DEFAULT '2013-03-23 09:45:00'\n" +
                ",\"J\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
                ")";
        methodWatcher.executeUpdate(DDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T15')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T15\" (\n" +
                "\"I\" TIMESTAMP DEFAULT '2013-03-23 09:45:00'\n" +
                ",\"J\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
                ") ;", rs.getString(1));

    }

    @Test
    public void testTextDefaultValue() throws Exception
    {
        String DDL = "CREATE TABLE \"SHOWCREATETABLEIT\".\"T16\"\n" +
                "(\n" +
                "\"I\" LONG VARCHAR DEFAULT 'I'\n" +
                ",\"J\" TEXT DEFAULT 'J'\n" +
                ",\"K\" CLOB DEFAULT 'K'\n" +
                ")";
        methodWatcher.executeUpdate(DDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T16')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T16\" (\n" +
                "\"I\" LONG VARCHAR DEFAULT 'I'\n" +
                ",\"J\" CLOB(2147483647) DEFAULT 'J'\n" +
                ",\"K\" CLOB(2147483647) DEFAULT 'K'\n" +
                ") ;", rs.getString(1));

    }

    @Test
    public void testTableWithQLBitColumnType() throws Exception
    {
        String DDL = "CREATE TABLE SHOWCREATETABLEIT.T20\n" +
                "(A20 INT, " +
                "B20 char(10) for bit data default, " +
                "C20 varchar(10) for bit data default, " +
                "D20 long varchar for bit data default, " +
                "E20 clob default)";
        methodWatcher.executeUpdate(DDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T20')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T20\" (\n" +
                "\"A20\" INTEGER\n" +
                ",\"B20\" CHAR (10) FOR BIT DATA DEFAULT X'00000000000000000000'\n" +
                ",\"C20\" VARCHAR (10) FOR BIT DATA DEFAULT X''\n" +
                ",\"D20\" LONG VARCHAR FOR BIT DATA DEFAULT X''\n" +
                ",\"E20\" CLOB(2147483647) DEFAULT \n" +
                ") ;", rs.getString(1));
    }

    @Test
    public void testTextDefaultUserRole() throws Exception
    {
        String DDL = "CREATE TABLE \"SHOWCREATETABLEIT\".\"T17\"\n" +
                "(\n" +
                "\"I\" VARCHAR(20) DEFAULT CURRENT_USER\n" +
                ",\"J\" VARCHAR(20) DEFAULT CURRENT_ROLE\n" +
                ",\"K\" VARCHAR(20) DEFAULT 'CURRENT_ROLE'\n" +
                ")";
        methodWatcher.executeUpdate(DDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T17')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T17\" (\n" +
                "\"I\" VARCHAR(20) DEFAULT CURRENT_USER\n" +
                ",\"J\" VARCHAR(20) DEFAULT CURRENT_ROLE\n" +
                ",\"K\" VARCHAR(20) DEFAULT 'CURRENT_ROLE'\n" +
                ") ;", rs.getString(1));

    }

    @Category(HBaseTest.class)
    @Test
    public void testExternalTablePlainText() throws Exception {
        //Plain text
        String textDLL = String.format("CREATE EXTERNAL TABLE SHOWCREATETABLEIT.testCsvFile (id INT, c_text varchar(30)) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY ','\n" +
                "STORED AS TEXTFILE\n" +
                "location '%s'", getExternalResourceDirectory() + "testCsvFile");
        methodWatcher.executeUpdate(textDLL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','TESTCSVFILE')");
        rs.next();
        Assert.assertEquals("CREATE EXTERNAL TABLE \"SHOWCREATETABLEIT\".\"TESTCSVFILE\" (\n" +
                "\"ID\" INTEGER\n" +
                ",\"C_TEXT\" VARCHAR(30)\n" +
                ") \n" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\n" +
                "STORED AS TEXTFILE\n" +
                "LOCATION '"+getExternalResourceDirectory()+"testCsvFile';", rs.getString(1));
    }

    @Category(HBaseTest.class)
    @Test
    public void testExternalTableParquetWithoutCompression() throws Exception {
        //Parquet Without compression
        String parquetDDL = String.format("create external table SHOWCREATETABLEIT.testParquet (col1 int, col2 varchar(24))" +
                "partitioned by (col1) STORED AS parquet LOCATION '%s'", getExternalResourceDirectory() + "testParquet");

        methodWatcher.executeUpdate(parquetDDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','TESTPARQUET')");
        rs.next();
        Assert.assertEquals("CREATE EXTERNAL TABLE \"SHOWCREATETABLEIT\".\"TESTPARQUET\" (\n" +
                "\"COL1\" INTEGER\n" +
                ",\"COL2\" VARCHAR(24)\n" +
                ") \n" +
                "PARTITIONED BY (\"COL1\")\n" +
                "STORED AS PARQUET\n" +
                "LOCATION '"+getExternalResourceDirectory()+"testParquet';", rs.getString(1));
    }

    @Category(HBaseTest.class)
    @Test
    public void testExternalTableOrcSnappy() throws Exception {
        //Orc With compression
        String orcDDL = String.format("create external table SHOWCREATETABLEIT.testOrcSnappy (col1 int, col2 varchar(24))" +
                "compressed with snappy partitioned by (col2) STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"testOrcSnappy");
        methodWatcher.executeUpdate(orcDDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','TESTORCSNAPPY')");
        rs.next();
        Assert.assertEquals("CREATE EXTERNAL TABLE \"SHOWCREATETABLEIT\".\"TESTORCSNAPPY\" (\n" +
                "\"COL1\" INTEGER\n" +
                ",\"COL2\" VARCHAR(24)\n" +
                ") \n" +
                "COMPRESSED WITH snappy\n" +
                "PARTITIONED BY (\"COL2\")\n" +
                "STORED AS ORC\n" +
                "LOCATION '"+getExternalResourceDirectory()+"testOrcSnappy';", rs.getString(1));
    }

    @Test
    public void testSystemTable() throws Exception {
        try
        {
            methodWatcher.execute("call syscs_util.SHOW_CREATE_TABLE('SYS','SYSTABLES')");
            Assert.fail("Expected to fail");
        }
        catch (SQLException e)
        {
            String sqlState = e.getSQLState();
            Assert.assertEquals("42X62",sqlState);
        }
    }

    @Test
    public void testView() throws Exception {
        try
        {
            methodWatcher.executeUpdate("create view showcreatetableit.v1 as (select a1 from showcreatetableit.t1)");
            methodWatcher.execute("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','V1')");
            Assert.fail("Expected to fail");
        }
        catch (SQLException e)
        {
            String sqlState = e.getSQLState();
            Assert.assertEquals("42Y62",sqlState);
        }
    }

    @Test
    public void testTableWithCaseSensitiveColumnNames() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T13')");
        rs.next();
        String ddl = rs.getString(1);
        checkEqualIgnoreConstraintOrder("CREATE TABLE \"SHOWCREATETABLEIT\".\"T13\" (\n" +
                "\"a13\" INTEGER NOT NULL\n" +
                ",\"b13\" INTEGER\n" +
                ",\"c13\" INTEGER NOT NULL\n" +
                ",\"d13\" INTEGER DEFAULT -1\n" +
                ", CONSTRAINT U_T13_1 UNIQUE (\"d13\"), CONSTRAINT PK_T13 PRIMARY KEY(\"c13\",\"a13\")) ;", ddl);
    }

    @Test
    public void testTableWithColumnDropped() throws Exception
    {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T14')");
            rs.next();
            String ddl = rs.getString(1);
            Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T14\" (\n" +
                    "\"A14\" INTEGER\n" +
                    ",\"B14\" INTEGER\n" +
                    ",\"C14\" INTEGER\n" +
                    ",\"D14\" INTEGER\n" +
                    ", CONSTRAINT T14_FK_1 FOREIGN KEY (\"C14\",\"A14\") REFERENCES \"SHOWCREATETABLEIT\".\"T13\"(\"c13\",\"a13\") ON UPDATE NO ACTION ON DELETE NO ACTION) ;", ddl);
            rs.close();

            /*drop a middle column abd add a new column*/
            statement.execute("alter table SHOWCREATETABLEIt.T13 drop column \"b13\"");
            statement.execute("alter table SHOWCREATETABLEIt.T14 drop column B14");
            statement.execute("alter table SHOWCREATETABLEIt.T13 add column \"e13\" int");
            statement.execute("alter table SHOWCREATETABLEIt.T14 add column E14 int");

            rs = statement.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T13')");
            rs.next();
            ddl = rs.getString(1);
            checkEqualIgnoreConstraintOrder("CREATE TABLE \"SHOWCREATETABLEIT\".\"T13\" (\n" +
                    "\"a13\" INTEGER NOT NULL\n" +
                    ",\"c13\" INTEGER NOT NULL\n" +
                    ",\"d13\" INTEGER DEFAULT -1\n" +
                    ",\"e13\" INTEGER\n" +
                    ", CONSTRAINT U_T13_1 UNIQUE (\"d13\"), CONSTRAINT PK_T13 PRIMARY KEY(\"c13\",\"a13\")) ;", ddl);
            rs.close();

            rs = statement.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T14')");
            rs.next();
            ddl = rs.getString(1);
            checkEqualIgnoreConstraintOrder("CREATE TABLE \"SHOWCREATETABLEIT\".\"T14\" (\n" +
                    "\"A14\" INTEGER\n" +
                    ",\"C14\" INTEGER\n" +
                    ",\"D14\" INTEGER\n" +
                    ",\"E14\" INTEGER\n" +
                    ", CONSTRAINT T14_FK_1 FOREIGN KEY (\"C14\",\"A14\") REFERENCES \"SHOWCREATETABLEIT\".\"T13\"(\"c13\",\"a13\") ON UPDATE NO ACTION ON DELETE NO ACTION) ;", ddl);
            rs.close();

        } finally {
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    @Test
    public void testTempTable() throws Exception
    {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.execute("create global temporary table t_temp(a1 int, b1 int, c1 int)");
        ResultSet rs = conn.query("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T_TEMP')");
        rs.next();
        String ddl = rs.getString(1);
        Assert.assertEquals("CREATE LOCAL TEMPORARY TABLE \"SHOWCREATETABLEIT\".\"T_TEMP\" (\n" +
                "\"A1\" INTEGER\n" +
                ",\"B1\" INTEGER\n" +
                ",\"C1\" INTEGER\n" +
                ") ;", ddl);
        rs.close();
        conn.close();
    }

    private static void  checkEqualIgnoreConstraintOrder(String expected, String actual) {
        // remove the semi column at the end
        expected = expected.substring(0, expected.length()-1);
        actual = actual.substring(0, actual.length()-1);
        String result = Stream.of(actual.split("CONSTRAINT ")).map(str -> str.substring(0, str.length() - 2)).sorted().collect(Collectors.joining(", "));

        String expectedResult = Stream.of(expected.split("CONSTRAINT ")).map(str -> str.substring(0, str.length() - 2)).sorted().collect(Collectors.joining(", "));
        Assert.assertEquals(expectedResult, result);
    }

    @Test
    public void testTable2100Columns() throws Exception {
        String ddl = "CREATE TABLE \"" + SCHEMA + "\".\"TEST_2100\" (\n" +
                "\"COL_0\" VARCHAR(30)\n" +
                ",\"COL_1\" CHAR(10)\n" +
                ",\"COL_2\" BIGINT\n" +
                ",\"COL_3\" DATE\n" +
                ",\"COL_4\" VARCHAR(30)\n" +
                ",\"COL_5\" INTEGER\n" +
                ",\"COL_6\" BIGINT\n" +
                ",\"COL_7\" DECIMAL(4,2)\n" +
                ",\"COL_8\" VARCHAR(30)\n" +
                ",\"COL_9\" DECIMAL(4,2)\n" +
                ",\"COL_10\" DATE\n" +
                ",\"COL_11\" DECIMAL(4,2)\n" +
                ",\"COL_12\" DECIMAL(4,2)\n" +
                ",\"COL_13\" DECIMAL(4,2)\n" +
                ",\"COL_14\" INTEGER\n" +
                ",\"COL_15\" VARCHAR(30)\n" +
                ",\"COL_16\" BIGINT\n" +
                ",\"COL_17\" INTEGER\n" +
                ",\"COL_18\" DATE\n" +
                ",\"COL_19\" CHAR(10)\n" +
                ",\"COL_20\" VARCHAR(30)\n" +
                ",\"COL_21\" DATE\n" +
                ",\"COL_22\" BIGINT\n" +
                ",\"COL_23\" CHAR(10)\n" +
                ",\"COL_24\" BIGINT\n" +
                ",\"COL_25\" DECIMAL(4,2)\n" +
                ",\"COL_26\" CHAR(10)\n" +
                ",\"COL_27\" VARCHAR(30)\n" +
                ",\"COL_28\" VARCHAR(30)\n" +
                ",\"COL_29\" VARCHAR(30)\n" +
                ",\"COL_30\" INTEGER\n" +
                ",\"COL_31\" BIGINT\n" +
                ",\"COL_32\" INTEGER\n" +
                ",\"COL_33\" INTEGER\n" +
                ",\"COL_34\" BIGINT\n" +
                ",\"COL_35\" INTEGER\n" +
                ",\"COL_36\" INTEGER\n" +
                ",\"COL_37\" DECIMAL(4,2)\n" +
                ",\"COL_38\" DECIMAL(4,2)\n" +
                ",\"COL_39\" DATE\n" +
                ",\"COL_40\" INTEGER\n" +
                ",\"COL_41\" CHAR(10)\n" +
                ",\"COL_42\" VARCHAR(30)\n" +
                ",\"COL_43\" BIGINT\n" +
                ",\"COL_44\" BIGINT\n" +
                ",\"COL_45\" DECIMAL(4,2)\n" +
                ",\"COL_46\" CHAR(10)\n" +
                ",\"COL_47\" CHAR(10)\n" +
                ",\"COL_48\" DECIMAL(4,2)\n" +
                ",\"COL_49\" VARCHAR(30)\n" +
                ",\"COL_50\" DATE\n" +
                ",\"COL_51\" DECIMAL(4,2)\n" +
                ",\"COL_52\" BIGINT\n" +
                ",\"COL_53\" INTEGER\n" +
                ",\"COL_54\" VARCHAR(30)\n" +
                ",\"COL_55\" INTEGER\n" +
                ",\"COL_56\" VARCHAR(30)\n" +
                ",\"COL_57\" DATE\n" +
                ",\"COL_58\" DECIMAL(4,2)\n" +
                ",\"COL_59\" DATE\n" +
                ",\"COL_60\" CHAR(10)\n" +
                ",\"COL_61\" BIGINT\n" +
                ",\"COL_62\" DECIMAL(4,2)\n" +
                ",\"COL_63\" DATE\n" +
                ",\"COL_64\" VARCHAR(30)\n" +
                ",\"COL_65\" VARCHAR(30)\n" +
                ",\"COL_66\" DECIMAL(4,2)\n" +
                ",\"COL_67\" INTEGER\n" +
                ",\"COL_68\" DECIMAL(4,2)\n" +
                ",\"COL_69\" DECIMAL(4,2)\n" +
                ",\"COL_70\" VARCHAR(30)\n" +
                ",\"COL_71\" CHAR(10)\n" +
                ",\"COL_72\" CHAR(10)\n" +
                ",\"COL_73\" VARCHAR(30)\n" +
                ",\"COL_74\" INTEGER\n" +
                ",\"COL_75\" BIGINT\n" +
                ",\"COL_76\" BIGINT\n" +
                ",\"COL_77\" CHAR(10)\n" +
                ",\"COL_78\" DECIMAL(4,2)\n" +
                ",\"COL_79\" INTEGER\n" +
                ",\"COL_80\" DECIMAL(4,2)\n" +
                ",\"COL_81\" BIGINT\n" +
                ",\"COL_82\" VARCHAR(30)\n" +
                ",\"COL_83\" VARCHAR(30)\n" +
                ",\"COL_84\" VARCHAR(30)\n" +
                ",\"COL_85\" INTEGER\n" +
                ",\"COL_86\" BIGINT\n" +
                ",\"COL_87\" VARCHAR(30)\n" +
                ",\"COL_88\" CHAR(10)\n" +
                ",\"COL_89\" CHAR(10)\n" +
                ",\"COL_90\" INTEGER\n" +
                ",\"COL_91\" INTEGER\n" +
                ",\"COL_92\" BIGINT\n" +
                ",\"COL_93\" INTEGER\n" +
                ",\"COL_94\" DATE\n" +
                ",\"COL_95\" DATE\n" +
                ",\"COL_96\" CHAR(10)\n" +
                ",\"COL_97\" INTEGER\n" +
                ",\"COL_98\" BIGINT\n" +
                ",\"COL_99\" CHAR(10)\n" +
                ",\"COL_100\" INTEGER\n" +
                ",\"COL_101\" DATE\n" +
                ",\"COL_102\" DATE\n" +
                ",\"COL_103\" CHAR(10)\n" +
                ",\"COL_104\" DECIMAL(4,2)\n" +
                ",\"COL_105\" VARCHAR(30)\n" +
                ",\"COL_106\" DATE\n" +
                ",\"COL_107\" INTEGER\n" +
                ",\"COL_108\" DATE\n" +
                ",\"COL_109\" INTEGER\n" +
                ",\"COL_110\" DECIMAL(4,2)\n" +
                ",\"COL_111\" DECIMAL(4,2)\n" +
                ",\"COL_112\" BIGINT\n" +
                ",\"COL_113\" BIGINT\n" +
                ",\"COL_114\" DECIMAL(4,2)\n" +
                ",\"COL_115\" INTEGER\n" +
                ",\"COL_116\" VARCHAR(30)\n" +
                ",\"COL_117\" DECIMAL(4,2)\n" +
                ",\"COL_118\" DECIMAL(4,2)\n" +
                ",\"COL_119\" DECIMAL(4,2)\n" +
                ",\"COL_120\" BIGINT\n" +
                ",\"COL_121\" CHAR(10)\n" +
                ",\"COL_122\" INTEGER\n" +
                ",\"COL_123\" CHAR(10)\n" +
                ",\"COL_124\" INTEGER\n" +
                ",\"COL_125\" INTEGER\n" +
                ",\"COL_126\" CHAR(10)\n" +
                ",\"COL_127\" DATE\n" +
                ",\"COL_128\" DATE\n" +
                ",\"COL_129\" CHAR(10)\n" +
                ",\"COL_130\" INTEGER\n" +
                ",\"COL_131\" CHAR(10)\n" +
                ",\"COL_132\" INTEGER\n" +
                ",\"COL_133\" BIGINT\n" +
                ",\"COL_134\" DECIMAL(4,2)\n" +
                ",\"COL_135\" INTEGER\n" +
                ",\"COL_136\" BIGINT\n" +
                ",\"COL_137\" CHAR(10)\n" +
                ",\"COL_138\" DATE\n" +
                ",\"COL_139\" DATE\n" +
                ",\"COL_140\" VARCHAR(30)\n" +
                ",\"COL_141\" CHAR(10)\n" +
                ",\"COL_142\" DATE\n" +
                ",\"COL_143\" VARCHAR(30)\n" +
                ",\"COL_144\" VARCHAR(30)\n" +
                ",\"COL_145\" VARCHAR(30)\n" +
                ",\"COL_146\" BIGINT\n" +
                ",\"COL_147\" INTEGER\n" +
                ",\"COL_148\" VARCHAR(30)\n" +
                ",\"COL_149\" BIGINT\n" +
                ",\"COL_150\" DECIMAL(4,2)\n" +
                ",\"COL_151\" CHAR(10)\n" +
                ",\"COL_152\" DATE\n" +
                ",\"COL_153\" BIGINT\n" +
                ",\"COL_154\" INTEGER\n" +
                ",\"COL_155\" DECIMAL(4,2)\n" +
                ",\"COL_156\" DATE\n" +
                ",\"COL_157\" VARCHAR(30)\n" +
                ",\"COL_158\" VARCHAR(30)\n" +
                ",\"COL_159\" CHAR(10)\n" +
                ",\"COL_160\" INTEGER\n" +
                ",\"COL_161\" INTEGER\n" +
                ",\"COL_162\" INTEGER\n" +
                ",\"COL_163\" INTEGER\n" +
                ",\"COL_164\" DECIMAL(4,2)\n" +
                ",\"COL_165\" VARCHAR(30)\n" +
                ",\"COL_166\" CHAR(10)\n" +
                ",\"COL_167\" BIGINT\n" +
                ",\"COL_168\" INTEGER\n" +
                ",\"COL_169\" DECIMAL(4,2)\n" +
                ",\"COL_170\" DECIMAL(4,2)\n" +
                ",\"COL_171\" BIGINT\n" +
                ",\"COL_172\" BIGINT\n" +
                ",\"COL_173\" DATE\n" +
                ",\"COL_174\" VARCHAR(30)\n" +
                ",\"COL_175\" VARCHAR(30)\n" +
                ",\"COL_176\" CHAR(10)\n" +
                ",\"COL_177\" INTEGER\n" +
                ",\"COL_178\" VARCHAR(30)\n" +
                ",\"COL_179\" INTEGER\n" +
                ",\"COL_180\" CHAR(10)\n" +
                ",\"COL_181\" DECIMAL(4,2)\n" +
                ",\"COL_182\" INTEGER\n" +
                ",\"COL_183\" INTEGER\n" +
                ",\"COL_184\" BIGINT\n" +
                ",\"COL_185\" VARCHAR(30)\n" +
                ",\"COL_186\" INTEGER\n" +
                ",\"COL_187\" VARCHAR(30)\n" +
                ",\"COL_188\" CHAR(10)\n" +
                ",\"COL_189\" DATE\n" +
                ",\"COL_190\" CHAR(10)\n" +
                ",\"COL_191\" DATE\n" +
                ",\"COL_192\" BIGINT\n" +
                ",\"COL_193\" INTEGER\n" +
                ",\"COL_194\" VARCHAR(30)\n" +
                ",\"COL_195\" DATE\n" +
                ",\"COL_196\" VARCHAR(30)\n" +
                ",\"COL_197\" VARCHAR(30)\n" +
                ",\"COL_198\" DATE\n" +
                ",\"COL_199\" VARCHAR(30)\n" +
                ",\"COL_200\" BIGINT\n" +
                ",\"COL_201\" DECIMAL(4,2)\n" +
                ",\"COL_202\" VARCHAR(30)\n" +
                ",\"COL_203\" DECIMAL(4,2)\n" +
                ",\"COL_204\" DATE\n" +
                ",\"COL_205\" BIGINT\n" +
                ",\"COL_206\" VARCHAR(30)\n" +
                ",\"COL_207\" DATE\n" +
                ",\"COL_208\" DECIMAL(4,2)\n" +
                ",\"COL_209\" BIGINT\n" +
                ",\"COL_210\" INTEGER\n" +
                ",\"COL_211\" INTEGER\n" +
                ",\"COL_212\" INTEGER\n" +
                ",\"COL_213\" DECIMAL(4,2)\n" +
                ",\"COL_214\" DATE\n" +
                ",\"COL_215\" DATE\n" +
                ",\"COL_216\" BIGINT\n" +
                ",\"COL_217\" VARCHAR(30)\n" +
                ",\"COL_218\" INTEGER\n" +
                ",\"COL_219\" VARCHAR(30)\n" +
                ",\"COL_220\" CHAR(10)\n" +
                ",\"COL_221\" INTEGER\n" +
                ",\"COL_222\" INTEGER\n" +
                ",\"COL_223\" BIGINT\n" +
                ",\"COL_224\" DECIMAL(4,2)\n" +
                ",\"COL_225\" BIGINT\n" +
                ",\"COL_226\" INTEGER\n" +
                ",\"COL_227\" CHAR(10)\n" +
                ",\"COL_228\" INTEGER\n" +
                ",\"COL_229\" DATE\n" +
                ",\"COL_230\" DATE\n" +
                ",\"COL_231\" INTEGER\n" +
                ",\"COL_232\" DECIMAL(4,2)\n" +
                ",\"COL_233\" BIGINT\n" +
                ",\"COL_234\" INTEGER\n" +
                ",\"COL_235\" VARCHAR(30)\n" +
                ",\"COL_236\" INTEGER\n" +
                ",\"COL_237\" INTEGER\n" +
                ",\"COL_238\" VARCHAR(30)\n" +
                ",\"COL_239\" CHAR(10)\n" +
                ",\"COL_240\" BIGINT\n" +
                ",\"COL_241\" CHAR(10)\n" +
                ",\"COL_242\" VARCHAR(30)\n" +
                ",\"COL_243\" INTEGER\n" +
                ",\"COL_244\" INTEGER\n" +
                ",\"COL_245\" CHAR(10)\n" +
                ",\"COL_246\" DATE\n" +
                ",\"COL_247\" CHAR(10)\n" +
                ",\"COL_248\" DATE\n" +
                ",\"COL_249\" VARCHAR(30)\n" +
                ",\"COL_250\" INTEGER\n" +
                ",\"COL_251\" DECIMAL(4,2)\n" +
                ",\"COL_252\" CHAR(10)\n" +
                ",\"COL_253\" INTEGER\n" +
                ",\"COL_254\" DECIMAL(4,2)\n" +
                ",\"COL_255\" CHAR(10)\n" +
                ",\"COL_256\" DATE\n" +
                ",\"COL_257\" DATE\n" +
                ",\"COL_258\" INTEGER\n" +
                ",\"COL_259\" CHAR(10)\n" +
                ",\"COL_260\" DECIMAL(4,2)\n" +
                ",\"COL_261\" CHAR(10)\n" +
                ",\"COL_262\" VARCHAR(30)\n" +
                ",\"COL_263\" DATE\n" +
                ",\"COL_264\" DECIMAL(4,2)\n" +
                ",\"COL_265\" CHAR(10)\n" +
                ",\"COL_266\" BIGINT\n" +
                ",\"COL_267\" BIGINT\n" +
                ",\"COL_268\" CHAR(10)\n" +
                ",\"COL_269\" VARCHAR(30)\n" +
                ",\"COL_270\" INTEGER\n" +
                ",\"COL_271\" DECIMAL(4,2)\n" +
                ",\"COL_272\" VARCHAR(30)\n" +
                ",\"COL_273\" INTEGER\n" +
                ",\"COL_274\" DATE\n" +
                ",\"COL_275\" DATE\n" +
                ",\"COL_276\" DECIMAL(4,2)\n" +
                ",\"COL_277\" CHAR(10)\n" +
                ",\"COL_278\" DATE\n" +
                ",\"COL_279\" DATE\n" +
                ",\"COL_280\" INTEGER\n" +
                ",\"COL_281\" DATE\n" +
                ",\"COL_282\" VARCHAR(30)\n" +
                ",\"COL_283\" DATE\n" +
                ",\"COL_284\" INTEGER\n" +
                ",\"COL_285\" INTEGER\n" +
                ",\"COL_286\" INTEGER\n" +
                ",\"COL_287\" CHAR(10)\n" +
                ",\"COL_288\" DECIMAL(4,2)\n" +
                ",\"COL_289\" BIGINT\n" +
                ",\"COL_290\" INTEGER\n" +
                ",\"COL_291\" BIGINT\n" +
                ",\"COL_292\" INTEGER\n" +
                ",\"COL_293\" DATE\n" +
                ",\"COL_294\" DATE\n" +
                ",\"COL_295\" CHAR(10)\n" +
                ",\"COL_296\" INTEGER\n" +
                ",\"COL_297\" BIGINT\n" +
                ",\"COL_298\" DATE\n" +
                ",\"COL_299\" VARCHAR(30)\n" +
                ",\"COL_300\" DATE\n" +
                ",\"COL_301\" CHAR(10)\n" +
                ",\"COL_302\" INTEGER\n" +
                ",\"COL_303\" BIGINT\n" +
                ",\"COL_304\" DECIMAL(4,2)\n" +
                ",\"COL_305\" DECIMAL(4,2)\n" +
                ",\"COL_306\" CHAR(10)\n" +
                ",\"COL_307\" DECIMAL(4,2)\n" +
                ",\"COL_308\" CHAR(10)\n" +
                ",\"COL_309\" DATE\n" +
                ",\"COL_310\" VARCHAR(30)\n" +
                ",\"COL_311\" VARCHAR(30)\n" +
                ",\"COL_312\" BIGINT\n" +
                ",\"COL_313\" CHAR(10)\n" +
                ",\"COL_314\" CHAR(10)\n" +
                ",\"COL_315\" CHAR(10)\n" +
                ",\"COL_316\" INTEGER\n" +
                ",\"COL_317\" INTEGER\n" +
                ",\"COL_318\" INTEGER\n" +
                ",\"COL_319\" INTEGER\n" +
                ",\"COL_320\" VARCHAR(30)\n" +
                ",\"COL_321\" INTEGER\n" +
                ",\"COL_322\" DATE\n" +
                ",\"COL_323\" DATE\n" +
                ",\"COL_324\" BIGINT\n" +
                ",\"COL_325\" VARCHAR(30)\n" +
                ",\"COL_326\" VARCHAR(30)\n" +
                ",\"COL_327\" VARCHAR(30)\n" +
                ",\"COL_328\" BIGINT\n" +
                ",\"COL_329\" BIGINT\n" +
                ",\"COL_330\" VARCHAR(30)\n" +
                ",\"COL_331\" CHAR(10)\n" +
                ",\"COL_332\" DECIMAL(4,2)\n" +
                ",\"COL_333\" BIGINT\n" +
                ",\"COL_334\" VARCHAR(30)\n" +
                ",\"COL_335\" DECIMAL(4,2)\n" +
                ",\"COL_336\" BIGINT\n" +
                ",\"COL_337\" VARCHAR(30)\n" +
                ",\"COL_338\" CHAR(10)\n" +
                ",\"COL_339\" DATE\n" +
                ",\"COL_340\" VARCHAR(30)\n" +
                ",\"COL_341\" DATE\n" +
                ",\"COL_342\" VARCHAR(30)\n" +
                ",\"COL_343\" CHAR(10)\n" +
                ",\"COL_344\" INTEGER\n" +
                ",\"COL_345\" CHAR(10)\n" +
                ",\"COL_346\" VARCHAR(30)\n" +
                ",\"COL_347\" BIGINT\n" +
                ",\"COL_348\" BIGINT\n" +
                ",\"COL_349\" CHAR(10)\n" +
                ",\"COL_350\" DATE\n" +
                ",\"COL_351\" DATE\n" +
                ",\"COL_352\" DECIMAL(4,2)\n" +
                ",\"COL_353\" CHAR(10)\n" +
                ",\"COL_354\" DATE\n" +
                ",\"COL_355\" VARCHAR(30)\n" +
                ",\"COL_356\" DECIMAL(4,2)\n" +
                ",\"COL_357\" VARCHAR(30)\n" +
                ",\"COL_358\" BIGINT\n" +
                ",\"COL_359\" DECIMAL(4,2)\n" +
                ",\"COL_360\" VARCHAR(30)\n" +
                ",\"COL_361\" DECIMAL(4,2)\n" +
                ",\"COL_362\" VARCHAR(30)\n" +
                ",\"COL_363\" INTEGER\n" +
                ",\"COL_364\" DATE\n" +
                ",\"COL_365\" INTEGER\n" +
                ",\"COL_366\" VARCHAR(30)\n" +
                ",\"COL_367\" DECIMAL(4,2)\n" +
                ",\"COL_368\" CHAR(10)\n" +
                ",\"COL_369\" VARCHAR(30)\n" +
                ",\"COL_370\" DATE\n" +
                ",\"COL_371\" VARCHAR(30)\n" +
                ",\"COL_372\" DECIMAL(4,2)\n" +
                ",\"COL_373\" INTEGER\n" +
                ",\"COL_374\" DATE\n" +
                ",\"COL_375\" VARCHAR(30)\n" +
                ",\"COL_376\" DATE\n" +
                ",\"COL_377\" DATE\n" +
                ",\"COL_378\" DATE\n" +
                ",\"COL_379\" CHAR(10)\n" +
                ",\"COL_380\" DECIMAL(4,2)\n" +
                ",\"COL_381\" DECIMAL(4,2)\n" +
                ",\"COL_382\" VARCHAR(30)\n" +
                ",\"COL_383\" BIGINT\n" +
                ",\"COL_384\" DECIMAL(4,2)\n" +
                ",\"COL_385\" BIGINT\n" +
                ",\"COL_386\" DATE\n" +
                ",\"COL_387\" DECIMAL(4,2)\n" +
                ",\"COL_388\" INTEGER\n" +
                ",\"COL_389\" INTEGER\n" +
                ",\"COL_390\" CHAR(10)\n" +
                ",\"COL_391\" BIGINT\n" +
                ",\"COL_392\" DATE\n" +
                ",\"COL_393\" INTEGER\n" +
                ",\"COL_394\" BIGINT\n" +
                ",\"COL_395\" CHAR(10)\n" +
                ",\"COL_396\" INTEGER\n" +
                ",\"COL_397\" INTEGER\n" +
                ",\"COL_398\" CHAR(10)\n" +
                ",\"COL_399\" INTEGER\n" +
                ",\"COL_400\" VARCHAR(30)\n" +
                ",\"COL_401\" INTEGER\n" +
                ",\"COL_402\" INTEGER\n" +
                ",\"COL_403\" DATE\n" +
                ",\"COL_404\" CHAR(10)\n" +
                ",\"COL_405\" DATE\n" +
                ",\"COL_406\" DATE\n" +
                ",\"COL_407\" CHAR(10)\n" +
                ",\"COL_408\" DATE\n" +
                ",\"COL_409\" CHAR(10)\n" +
                ",\"COL_410\" DECIMAL(4,2)\n" +
                ",\"COL_411\" BIGINT\n" +
                ",\"COL_412\" CHAR(10)\n" +
                ",\"COL_413\" INTEGER\n" +
                ",\"COL_414\" DECIMAL(4,2)\n" +
                ",\"COL_415\" CHAR(10)\n" +
                ",\"COL_416\" DECIMAL(4,2)\n" +
                ",\"COL_417\" DATE\n" +
                ",\"COL_418\" DATE\n" +
                ",\"COL_419\" BIGINT\n" +
                ",\"COL_420\" VARCHAR(30)\n" +
                ",\"COL_421\" VARCHAR(30)\n" +
                ",\"COL_422\" BIGINT\n" +
                ",\"COL_423\" BIGINT\n" +
                ",\"COL_424\" DECIMAL(4,2)\n" +
                ",\"COL_425\" CHAR(10)\n" +
                ",\"COL_426\" INTEGER\n" +
                ",\"COL_427\" DECIMAL(4,2)\n" +
                ",\"COL_428\" VARCHAR(30)\n" +
                ",\"COL_429\" CHAR(10)\n" +
                ",\"COL_430\" DATE\n" +
                ",\"COL_431\" VARCHAR(30)\n" +
                ",\"COL_432\" VARCHAR(30)\n" +
                ",\"COL_433\" BIGINT\n" +
                ",\"COL_434\" CHAR(10)\n" +
                ",\"COL_435\" INTEGER\n" +
                ",\"COL_436\" DATE\n" +
                ",\"COL_437\" DECIMAL(4,2)\n" +
                ",\"COL_438\" CHAR(10)\n" +
                ",\"COL_439\" VARCHAR(30)\n" +
                ",\"COL_440\" CHAR(10)\n" +
                ",\"COL_441\" CHAR(10)\n" +
                ",\"COL_442\" INTEGER\n" +
                ",\"COL_443\" BIGINT\n" +
                ",\"COL_444\" DATE\n" +
                ",\"COL_445\" CHAR(10)\n" +
                ",\"COL_446\" DATE\n" +
                ",\"COL_447\" DATE\n" +
                ",\"COL_448\" INTEGER\n" +
                ",\"COL_449\" BIGINT\n" +
                ",\"COL_450\" BIGINT\n" +
                ",\"COL_451\" DATE\n" +
                ",\"COL_452\" VARCHAR(30)\n" +
                ",\"COL_453\" INTEGER\n" +
                ",\"COL_454\" INTEGER\n" +
                ",\"COL_455\" CHAR(10)\n" +
                ",\"COL_456\" DECIMAL(4,2)\n" +
                ",\"COL_457\" INTEGER\n" +
                ",\"COL_458\" BIGINT\n" +
                ",\"COL_459\" DATE\n" +
                ",\"COL_460\" VARCHAR(30)\n" +
                ",\"COL_461\" VARCHAR(30)\n" +
                ",\"COL_462\" BIGINT\n" +
                ",\"COL_463\" DATE\n" +
                ",\"COL_464\" BIGINT\n" +
                ",\"COL_465\" BIGINT\n" +
                ",\"COL_466\" DECIMAL(4,2)\n" +
                ",\"COL_467\" CHAR(10)\n" +
                ",\"COL_468\" DATE\n" +
                ",\"COL_469\" CHAR(10)\n" +
                ",\"COL_470\" CHAR(10)\n" +
                ",\"COL_471\" DATE\n" +
                ",\"COL_472\" INTEGER\n" +
                ",\"COL_473\" DATE\n" +
                ",\"COL_474\" VARCHAR(30)\n" +
                ",\"COL_475\" VARCHAR(30)\n" +
                ",\"COL_476\" CHAR(10)\n" +
                ",\"COL_477\" BIGINT\n" +
                ",\"COL_478\" DATE\n" +
                ",\"COL_479\" VARCHAR(30)\n" +
                ",\"COL_480\" DATE\n" +
                ",\"COL_481\" VARCHAR(30)\n" +
                ",\"COL_482\" CHAR(10)\n" +
                ",\"COL_483\" CHAR(10)\n" +
                ",\"COL_484\" VARCHAR(30)\n" +
                ",\"COL_485\" CHAR(10)\n" +
                ",\"COL_486\" BIGINT\n" +
                ",\"COL_487\" INTEGER\n" +
                ",\"COL_488\" DATE\n" +
                ",\"COL_489\" VARCHAR(30)\n" +
                ",\"COL_490\" BIGINT\n" +
                ",\"COL_491\" CHAR(10)\n" +
                ",\"COL_492\" BIGINT\n" +
                ",\"COL_493\" BIGINT\n" +
                ",\"COL_494\" BIGINT\n" +
                ",\"COL_495\" DECIMAL(4,2)\n" +
                ",\"COL_496\" CHAR(10)\n" +
                ",\"COL_497\" DATE\n" +
                ",\"COL_498\" DATE\n" +
                ",\"COL_499\" BIGINT\n" +
                ",\"COL_500\" DATE\n" +
                ",\"COL_501\" DATE\n" +
                ",\"COL_502\" DECIMAL(4,2)\n" +
                ",\"COL_503\" DATE\n" +
                ",\"COL_504\" DATE\n" +
                ",\"COL_505\" CHAR(10)\n" +
                ",\"COL_506\" INTEGER\n" +
                ",\"COL_507\" BIGINT\n" +
                ",\"COL_508\" DATE\n" +
                ",\"COL_509\" BIGINT\n" +
                ",\"COL_510\" DATE\n" +
                ",\"COL_511\" VARCHAR(30)\n" +
                ",\"COL_512\" DECIMAL(4,2)\n" +
                ",\"COL_513\" DATE\n" +
                ",\"COL_514\" INTEGER\n" +
                ",\"COL_515\" DECIMAL(4,2)\n" +
                ",\"COL_516\" INTEGER\n" +
                ",\"COL_517\" DATE\n" +
                ",\"COL_518\" VARCHAR(30)\n" +
                ",\"COL_519\" VARCHAR(30)\n" +
                ",\"COL_520\" BIGINT\n" +
                ",\"COL_521\" INTEGER\n" +
                ",\"COL_522\" INTEGER\n" +
                ",\"COL_523\" DATE\n" +
                ",\"COL_524\" BIGINT\n" +
                ",\"COL_525\" INTEGER\n" +
                ",\"COL_526\" DATE\n" +
                ",\"COL_527\" CHAR(10)\n" +
                ",\"COL_528\" DATE\n" +
                ",\"COL_529\" BIGINT\n" +
                ",\"COL_530\" DATE\n" +
                ",\"COL_531\" VARCHAR(30)\n" +
                ",\"COL_532\" DECIMAL(4,2)\n" +
                ",\"COL_533\" CHAR(10)\n" +
                ",\"COL_534\" CHAR(10)\n" +
                ",\"COL_535\" BIGINT\n" +
                ",\"COL_536\" CHAR(10)\n" +
                ",\"COL_537\" CHAR(10)\n" +
                ",\"COL_538\" BIGINT\n" +
                ",\"COL_539\" VARCHAR(30)\n" +
                ",\"COL_540\" INTEGER\n" +
                ",\"COL_541\" INTEGER\n" +
                ",\"COL_542\" BIGINT\n" +
                ",\"COL_543\" INTEGER\n" +
                ",\"COL_544\" VARCHAR(30)\n" +
                ",\"COL_545\" CHAR(10)\n" +
                ",\"COL_546\" INTEGER\n" +
                ",\"COL_547\" CHAR(10)\n" +
                ",\"COL_548\" DATE\n" +
                ",\"COL_549\" VARCHAR(30)\n" +
                ",\"COL_550\" BIGINT\n" +
                ",\"COL_551\" DATE\n" +
                ",\"COL_552\" INTEGER\n" +
                ",\"COL_553\" VARCHAR(30)\n" +
                ",\"COL_554\" INTEGER\n" +
                ",\"COL_555\" BIGINT\n" +
                ",\"COL_556\" BIGINT\n" +
                ",\"COL_557\" CHAR(10)\n" +
                ",\"COL_558\" INTEGER\n" +
                ",\"COL_559\" DATE\n" +
                ",\"COL_560\" DECIMAL(4,2)\n" +
                ",\"COL_561\" INTEGER\n" +
                ",\"COL_562\" BIGINT\n" +
                ",\"COL_563\" CHAR(10)\n" +
                ",\"COL_564\" DECIMAL(4,2)\n" +
                ",\"COL_565\" DATE\n" +
                ",\"COL_566\" INTEGER\n" +
                ",\"COL_567\" DECIMAL(4,2)\n" +
                ",\"COL_568\" BIGINT\n" +
                ",\"COL_569\" INTEGER\n" +
                ",\"COL_570\" CHAR(10)\n" +
                ",\"COL_571\" DECIMAL(4,2)\n" +
                ",\"COL_572\" DATE\n" +
                ",\"COL_573\" DATE\n" +
                ",\"COL_574\" DATE\n" +
                ",\"COL_575\" DATE\n" +
                ",\"COL_576\" DATE\n" +
                ",\"COL_577\" VARCHAR(30)\n" +
                ",\"COL_578\" CHAR(10)\n" +
                ",\"COL_579\" INTEGER\n" +
                ",\"COL_580\" VARCHAR(30)\n" +
                ",\"COL_581\" DATE\n" +
                ",\"COL_582\" BIGINT\n" +
                ",\"COL_583\" VARCHAR(30)\n" +
                ",\"COL_584\" INTEGER\n" +
                ",\"COL_585\" BIGINT\n" +
                ",\"COL_586\" DATE\n" +
                ",\"COL_587\" CHAR(10)\n" +
                ",\"COL_588\" VARCHAR(30)\n" +
                ",\"COL_589\" BIGINT\n" +
                ",\"COL_590\" DATE\n" +
                ",\"COL_591\" BIGINT\n" +
                ",\"COL_592\" INTEGER\n" +
                ",\"COL_593\" DECIMAL(4,2)\n" +
                ",\"COL_594\" DECIMAL(4,2)\n" +
                ",\"COL_595\" VARCHAR(30)\n" +
                ",\"COL_596\" INTEGER\n" +
                ",\"COL_597\" CHAR(10)\n" +
                ",\"COL_598\" BIGINT\n" +
                ",\"COL_599\" DECIMAL(4,2)\n" +
                ",\"COL_600\" CHAR(10)\n" +
                ",\"COL_601\" CHAR(10)\n" +
                ",\"COL_602\" VARCHAR(30)\n" +
                ",\"COL_603\" VARCHAR(30)\n" +
                ",\"COL_604\" INTEGER\n" +
                ",\"COL_605\" BIGINT\n" +
                ",\"COL_606\" BIGINT\n" +
                ",\"COL_607\" DECIMAL(4,2)\n" +
                ",\"COL_608\" VARCHAR(30)\n" +
                ",\"COL_609\" INTEGER\n" +
                ",\"COL_610\" BIGINT\n" +
                ",\"COL_611\" BIGINT\n" +
                ",\"COL_612\" VARCHAR(30)\n" +
                ",\"COL_613\" DATE\n" +
                ",\"COL_614\" BIGINT\n" +
                ",\"COL_615\" VARCHAR(30)\n" +
                ",\"COL_616\" DATE\n" +
                ",\"COL_617\" DECIMAL(4,2)\n" +
                ",\"COL_618\" BIGINT\n" +
                ",\"COL_619\" VARCHAR(30)\n" +
                ",\"COL_620\" BIGINT\n" +
                ",\"COL_621\" DATE\n" +
                ",\"COL_622\" DECIMAL(4,2)\n" +
                ",\"COL_623\" DECIMAL(4,2)\n" +
                ",\"COL_624\" INTEGER\n" +
                ",\"COL_625\" DATE\n" +
                ",\"COL_626\" CHAR(10)\n" +
                ",\"COL_627\" VARCHAR(30)\n" +
                ",\"COL_628\" INTEGER\n" +
                ",\"COL_629\" VARCHAR(30)\n" +
                ",\"COL_630\" DATE\n" +
                ",\"COL_631\" BIGINT\n" +
                ",\"COL_632\" INTEGER\n" +
                ",\"COL_633\" VARCHAR(30)\n" +
                ",\"COL_634\" CHAR(10)\n" +
                ",\"COL_635\" VARCHAR(30)\n" +
                ",\"COL_636\" VARCHAR(30)\n" +
                ",\"COL_637\" DATE\n" +
                ",\"COL_638\" BIGINT\n" +
                ",\"COL_639\" DATE\n" +
                ",\"COL_640\" BIGINT\n" +
                ",\"COL_641\" BIGINT\n" +
                ",\"COL_642\" INTEGER\n" +
                ",\"COL_643\" BIGINT\n" +
                ",\"COL_644\" INTEGER\n" +
                ",\"COL_645\" BIGINT\n" +
                ",\"COL_646\" DECIMAL(4,2)\n" +
                ",\"COL_647\" VARCHAR(30)\n" +
                ",\"COL_648\" BIGINT\n" +
                ",\"COL_649\" DATE\n" +
                ",\"COL_650\" CHAR(10)\n" +
                ",\"COL_651\" DATE\n" +
                ",\"COL_652\" DECIMAL(4,2)\n" +
                ",\"COL_653\" INTEGER\n" +
                ",\"COL_654\" DATE\n" +
                ",\"COL_655\" DECIMAL(4,2)\n" +
                ",\"COL_656\" DATE\n" +
                ",\"COL_657\" DATE\n" +
                ",\"COL_658\" DECIMAL(4,2)\n" +
                ",\"COL_659\" VARCHAR(30)\n" +
                ",\"COL_660\" DECIMAL(4,2)\n" +
                ",\"COL_661\" CHAR(10)\n" +
                ",\"COL_662\" BIGINT\n" +
                ",\"COL_663\" DATE\n" +
                ",\"COL_664\" VARCHAR(30)\n" +
                ",\"COL_665\" INTEGER\n" +
                ",\"COL_666\" DATE\n" +
                ",\"COL_667\" INTEGER\n" +
                ",\"COL_668\" DATE\n" +
                ",\"COL_669\" CHAR(10)\n" +
                ",\"COL_670\" INTEGER\n" +
                ",\"COL_671\" DECIMAL(4,2)\n" +
                ",\"COL_672\" DECIMAL(4,2)\n" +
                ",\"COL_673\" VARCHAR(30)\n" +
                ",\"COL_674\" VARCHAR(30)\n" +
                ",\"COL_675\" CHAR(10)\n" +
                ",\"COL_676\" CHAR(10)\n" +
                ",\"COL_677\" DATE\n" +
                ",\"COL_678\" DATE\n" +
                ",\"COL_679\" VARCHAR(30)\n" +
                ",\"COL_680\" VARCHAR(30)\n" +
                ",\"COL_681\" VARCHAR(30)\n" +
                ",\"COL_682\" VARCHAR(30)\n" +
                ",\"COL_683\" CHAR(10)\n" +
                ",\"COL_684\" BIGINT\n" +
                ",\"COL_685\" DECIMAL(4,2)\n" +
                ",\"COL_686\" VARCHAR(30)\n" +
                ",\"COL_687\" CHAR(10)\n" +
                ",\"COL_688\" DATE\n" +
                ",\"COL_689\" BIGINT\n" +
                ",\"COL_690\" DECIMAL(4,2)\n" +
                ",\"COL_691\" BIGINT\n" +
                ",\"COL_692\" INTEGER\n" +
                ",\"COL_693\" CHAR(10)\n" +
                ",\"COL_694\" DATE\n" +
                ",\"COL_695\" INTEGER\n" +
                ",\"COL_696\" INTEGER\n" +
                ",\"COL_697\" INTEGER\n" +
                ",\"COL_698\" VARCHAR(30)\n" +
                ",\"COL_699\" BIGINT\n" +
                ",\"COL_700\" VARCHAR(30)\n" +
                ",\"COL_701\" INTEGER\n" +
                ",\"COL_702\" BIGINT\n" +
                ",\"COL_703\" VARCHAR(30)\n" +
                ",\"COL_704\" INTEGER\n" +
                ",\"COL_705\" DATE\n" +
                ",\"COL_706\" DECIMAL(4,2)\n" +
                ",\"COL_707\" INTEGER\n" +
                ",\"COL_708\" BIGINT\n" +
                ",\"COL_709\" DATE\n" +
                ",\"COL_710\" DECIMAL(4,2)\n" +
                ",\"COL_711\" CHAR(10)\n" +
                ",\"COL_712\" VARCHAR(30)\n" +
                ",\"COL_713\" DATE\n" +
                ",\"COL_714\" CHAR(10)\n" +
                ",\"COL_715\" DATE\n" +
                ",\"COL_716\" BIGINT\n" +
                ",\"COL_717\" DECIMAL(4,2)\n" +
                ",\"COL_718\" DATE\n" +
                ",\"COL_719\" DECIMAL(4,2)\n" +
                ",\"COL_720\" DECIMAL(4,2)\n" +
                ",\"COL_721\" BIGINT\n" +
                ",\"COL_722\" VARCHAR(30)\n" +
                ",\"COL_723\" VARCHAR(30)\n" +
                ",\"COL_724\" DECIMAL(4,2)\n" +
                ",\"COL_725\" VARCHAR(30)\n" +
                ",\"COL_726\" DATE\n" +
                ",\"COL_727\" INTEGER\n" +
                ",\"COL_728\" BIGINT\n" +
                ",\"COL_729\" DATE\n" +
                ",\"COL_730\" DATE\n" +
                ",\"COL_731\" INTEGER\n" +
                ",\"COL_732\" INTEGER\n" +
                ",\"COL_733\" INTEGER\n" +
                ",\"COL_734\" CHAR(10)\n" +
                ",\"COL_735\" DATE\n" +
                ",\"COL_736\" DECIMAL(4,2)\n" +
                ",\"COL_737\" VARCHAR(30)\n" +
                ",\"COL_738\" DATE\n" +
                ",\"COL_739\" INTEGER\n" +
                ",\"COL_740\" CHAR(10)\n" +
                ",\"COL_741\" DATE\n" +
                ",\"COL_742\" BIGINT\n" +
                ",\"COL_743\" DECIMAL(4,2)\n" +
                ",\"COL_744\" VARCHAR(30)\n" +
                ",\"COL_745\" BIGINT\n" +
                ",\"COL_746\" BIGINT\n" +
                ",\"COL_747\" DATE\n" +
                ",\"COL_748\" BIGINT\n" +
                ",\"COL_749\" DECIMAL(4,2)\n" +
                ",\"COL_750\" INTEGER\n" +
                ",\"COL_751\" DECIMAL(4,2)\n" +
                ",\"COL_752\" BIGINT\n" +
                ",\"COL_753\" DECIMAL(4,2)\n" +
                ",\"COL_754\" DECIMAL(4,2)\n" +
                ",\"COL_755\" BIGINT\n" +
                ",\"COL_756\" DECIMAL(4,2)\n" +
                ",\"COL_757\" CHAR(10)\n" +
                ",\"COL_758\" BIGINT\n" +
                ",\"COL_759\" DATE\n" +
                ",\"COL_760\" INTEGER\n" +
                ",\"COL_761\" BIGINT\n" +
                ",\"COL_762\" INTEGER\n" +
                ",\"COL_763\" INTEGER\n" +
                ",\"COL_764\" DATE\n" +
                ",\"COL_765\" INTEGER\n" +
                ",\"COL_766\" CHAR(10)\n" +
                ",\"COL_767\" CHAR(10)\n" +
                ",\"COL_768\" CHAR(10)\n" +
                ",\"COL_769\" BIGINT\n" +
                ",\"COL_770\" VARCHAR(30)\n" +
                ",\"COL_771\" CHAR(10)\n" +
                ",\"COL_772\" VARCHAR(30)\n" +
                ",\"COL_773\" BIGINT\n" +
                ",\"COL_774\" DECIMAL(4,2)\n" +
                ",\"COL_775\" CHAR(10)\n" +
                ",\"COL_776\" CHAR(10)\n" +
                ",\"COL_777\" DATE\n" +
                ",\"COL_778\" BIGINT\n" +
                ",\"COL_779\" DECIMAL(4,2)\n" +
                ",\"COL_780\" CHAR(10)\n" +
                ",\"COL_781\" INTEGER\n" +
                ",\"COL_782\" DATE\n" +
                ",\"COL_783\" BIGINT\n" +
                ",\"COL_784\" DATE\n" +
                ",\"COL_785\" BIGINT\n" +
                ",\"COL_786\" VARCHAR(30)\n" +
                ",\"COL_787\" DATE\n" +
                ",\"COL_788\" BIGINT\n" +
                ",\"COL_789\" INTEGER\n" +
                ",\"COL_790\" VARCHAR(30)\n" +
                ",\"COL_791\" INTEGER\n" +
                ",\"COL_792\" VARCHAR(30)\n" +
                ",\"COL_793\" BIGINT\n" +
                ",\"COL_794\" INTEGER\n" +
                ",\"COL_795\" CHAR(10)\n" +
                ",\"COL_796\" CHAR(10)\n" +
                ",\"COL_797\" CHAR(10)\n" +
                ",\"COL_798\" BIGINT\n" +
                ",\"COL_799\" DECIMAL(4,2)\n" +
                ",\"COL_800\" VARCHAR(30)\n" +
                ",\"COL_801\" BIGINT\n" +
                ",\"COL_802\" VARCHAR(30)\n" +
                ",\"COL_803\" CHAR(10)\n" +
                ",\"COL_804\" INTEGER\n" +
                ",\"COL_805\" DATE\n" +
                ",\"COL_806\" CHAR(10)\n" +
                ",\"COL_807\" BIGINT\n" +
                ",\"COL_808\" DATE\n" +
                ",\"COL_809\" INTEGER\n" +
                ",\"COL_810\" VARCHAR(30)\n" +
                ",\"COL_811\" DECIMAL(4,2)\n" +
                ",\"COL_812\" INTEGER\n" +
                ",\"COL_813\" CHAR(10)\n" +
                ",\"COL_814\" INTEGER\n" +
                ",\"COL_815\" VARCHAR(30)\n" +
                ",\"COL_816\" BIGINT\n" +
                ",\"COL_817\" VARCHAR(30)\n" +
                ",\"COL_818\" INTEGER\n" +
                ",\"COL_819\" BIGINT\n" +
                ",\"COL_820\" CHAR(10)\n" +
                ",\"COL_821\" VARCHAR(30)\n" +
                ",\"COL_822\" CHAR(10)\n" +
                ",\"COL_823\" VARCHAR(30)\n" +
                ",\"COL_824\" CHAR(10)\n" +
                ",\"COL_825\" INTEGER\n" +
                ",\"COL_826\" CHAR(10)\n" +
                ",\"COL_827\" DATE\n" +
                ",\"COL_828\" BIGINT\n" +
                ",\"COL_829\" CHAR(10)\n" +
                ",\"COL_830\" DECIMAL(4,2)\n" +
                ",\"COL_831\" DATE\n" +
                ",\"COL_832\" INTEGER\n" +
                ",\"COL_833\" DECIMAL(4,2)\n" +
                ",\"COL_834\" INTEGER\n" +
                ",\"COL_835\" DECIMAL(4,2)\n" +
                ",\"COL_836\" DATE\n" +
                ",\"COL_837\" VARCHAR(30)\n" +
                ",\"COL_838\" DECIMAL(4,2)\n" +
                ",\"COL_839\" DECIMAL(4,2)\n" +
                ",\"COL_840\" INTEGER\n" +
                ",\"COL_841\" CHAR(10)\n" +
                ",\"COL_842\" VARCHAR(30)\n" +
                ",\"COL_843\" INTEGER\n" +
                ",\"COL_844\" VARCHAR(30)\n" +
                ",\"COL_845\" INTEGER\n" +
                ",\"COL_846\" DATE\n" +
                ",\"COL_847\" DATE\n" +
                ",\"COL_848\" BIGINT\n" +
                ",\"COL_849\" CHAR(10)\n" +
                ",\"COL_850\" DECIMAL(4,2)\n" +
                ",\"COL_851\" INTEGER\n" +
                ",\"COL_852\" DECIMAL(4,2)\n" +
                ",\"COL_853\" CHAR(10)\n" +
                ",\"COL_854\" INTEGER\n" +
                ",\"COL_855\" VARCHAR(30)\n" +
                ",\"COL_856\" INTEGER\n" +
                ",\"COL_857\" DECIMAL(4,2)\n" +
                ",\"COL_858\" VARCHAR(30)\n" +
                ",\"COL_859\" DECIMAL(4,2)\n" +
                ",\"COL_860\" VARCHAR(30)\n" +
                ",\"COL_861\" CHAR(10)\n" +
                ",\"COL_862\" VARCHAR(30)\n" +
                ",\"COL_863\" CHAR(10)\n" +
                ",\"COL_864\" DECIMAL(4,2)\n" +
                ",\"COL_865\" CHAR(10)\n" +
                ",\"COL_866\" VARCHAR(30)\n" +
                ",\"COL_867\" VARCHAR(30)\n" +
                ",\"COL_868\" DECIMAL(4,2)\n" +
                ",\"COL_869\" BIGINT\n" +
                ",\"COL_870\" DATE\n" +
                ",\"COL_871\" VARCHAR(30)\n" +
                ",\"COL_872\" VARCHAR(30)\n" +
                ",\"COL_873\" BIGINT\n" +
                ",\"COL_874\" DECIMAL(4,2)\n" +
                ",\"COL_875\" CHAR(10)\n" +
                ",\"COL_876\" INTEGER\n" +
                ",\"COL_877\" DECIMAL(4,2)\n" +
                ",\"COL_878\" VARCHAR(30)\n" +
                ",\"COL_879\" DATE\n" +
                ",\"COL_880\" BIGINT\n" +
                ",\"COL_881\" VARCHAR(30)\n" +
                ",\"COL_882\" CHAR(10)\n" +
                ",\"COL_883\" BIGINT\n" +
                ",\"COL_884\" CHAR(10)\n" +
                ",\"COL_885\" VARCHAR(30)\n" +
                ",\"COL_886\" VARCHAR(30)\n" +
                ",\"COL_887\" INTEGER\n" +
                ",\"COL_888\" DATE\n" +
                ",\"COL_889\" VARCHAR(30)\n" +
                ",\"COL_890\" INTEGER\n" +
                ",\"COL_891\" DECIMAL(4,2)\n" +
                ",\"COL_892\" DATE\n" +
                ",\"COL_893\" DECIMAL(4,2)\n" +
                ",\"COL_894\" VARCHAR(30)\n" +
                ",\"COL_895\" INTEGER\n" +
                ",\"COL_896\" DATE\n" +
                ",\"COL_897\" BIGINT\n" +
                ",\"COL_898\" INTEGER\n" +
                ",\"COL_899\" VARCHAR(30)\n" +
                ",\"COL_900\" BIGINT\n" +
                ",\"COL_901\" CHAR(10)\n" +
                ",\"COL_902\" DATE\n" +
                ",\"COL_903\" BIGINT\n" +
                ",\"COL_904\" CHAR(10)\n" +
                ",\"COL_905\" VARCHAR(30)\n" +
                ",\"COL_906\" INTEGER\n" +
                ",\"COL_907\" CHAR(10)\n" +
                ",\"COL_908\" DECIMAL(4,2)\n" +
                ",\"COL_909\" DECIMAL(4,2)\n" +
                ",\"COL_910\" BIGINT\n" +
                ",\"COL_911\" BIGINT\n" +
                ",\"COL_912\" VARCHAR(30)\n" +
                ",\"COL_913\" DECIMAL(4,2)\n" +
                ",\"COL_914\" CHAR(10)\n" +
                ",\"COL_915\" CHAR(10)\n" +
                ",\"COL_916\" VARCHAR(30)\n" +
                ",\"COL_917\" DATE\n" +
                ",\"COL_918\" DATE\n" +
                ",\"COL_919\" DECIMAL(4,2)\n" +
                ",\"COL_920\" VARCHAR(30)\n" +
                ",\"COL_921\" INTEGER\n" +
                ",\"COL_922\" BIGINT\n" +
                ",\"COL_923\" BIGINT\n" +
                ",\"COL_924\" DECIMAL(4,2)\n" +
                ",\"COL_925\" DATE\n" +
                ",\"COL_926\" INTEGER\n" +
                ",\"COL_927\" DECIMAL(4,2)\n" +
                ",\"COL_928\" INTEGER\n" +
                ",\"COL_929\" BIGINT\n" +
                ",\"COL_930\" DATE\n" +
                ",\"COL_931\" DECIMAL(4,2)\n" +
                ",\"COL_932\" BIGINT\n" +
                ",\"COL_933\" DATE\n" +
                ",\"COL_934\" CHAR(10)\n" +
                ",\"COL_935\" VARCHAR(30)\n" +
                ",\"COL_936\" CHAR(10)\n" +
                ",\"COL_937\" VARCHAR(30)\n" +
                ",\"COL_938\" INTEGER\n" +
                ",\"COL_939\" INTEGER\n" +
                ",\"COL_940\" CHAR(10)\n" +
                ",\"COL_941\" BIGINT\n" +
                ",\"COL_942\" CHAR(10)\n" +
                ",\"COL_943\" BIGINT\n" +
                ",\"COL_944\" CHAR(10)\n" +
                ",\"COL_945\" BIGINT\n" +
                ",\"COL_946\" CHAR(10)\n" +
                ",\"COL_947\" INTEGER\n" +
                ",\"COL_948\" DECIMAL(4,2)\n" +
                ",\"COL_949\" DECIMAL(4,2)\n" +
                ",\"COL_950\" CHAR(10)\n" +
                ",\"COL_951\" BIGINT\n" +
                ",\"COL_952\" DECIMAL(4,2)\n" +
                ",\"COL_953\" VARCHAR(30)\n" +
                ",\"COL_954\" DATE\n" +
                ",\"COL_955\" DECIMAL(4,2)\n" +
                ",\"COL_956\" DECIMAL(4,2)\n" +
                ",\"COL_957\" DATE\n" +
                ",\"COL_958\" BIGINT\n" +
                ",\"COL_959\" CHAR(10)\n" +
                ",\"COL_960\" INTEGER\n" +
                ",\"COL_961\" DECIMAL(4,2)\n" +
                ",\"COL_962\" BIGINT\n" +
                ",\"COL_963\" DATE\n" +
                ",\"COL_964\" DATE\n" +
                ",\"COL_965\" INTEGER\n" +
                ",\"COL_966\" CHAR(10)\n" +
                ",\"COL_967\" INTEGER\n" +
                ",\"COL_968\" VARCHAR(30)\n" +
                ",\"COL_969\" DATE\n" +
                ",\"COL_970\" INTEGER\n" +
                ",\"COL_971\" VARCHAR(30)\n" +
                ",\"COL_972\" BIGINT\n" +
                ",\"COL_973\" DATE\n" +
                ",\"COL_974\" CHAR(10)\n" +
                ",\"COL_975\" BIGINT\n" +
                ",\"COL_976\" VARCHAR(30)\n" +
                ",\"COL_977\" DATE\n" +
                ",\"COL_978\" DATE\n" +
                ",\"COL_979\" DECIMAL(4,2)\n" +
                ",\"COL_980\" DECIMAL(4,2)\n" +
                ",\"COL_981\" DATE\n" +
                ",\"COL_982\" CHAR(10)\n" +
                ",\"COL_983\" DECIMAL(4,2)\n" +
                ",\"COL_984\" INTEGER\n" +
                ",\"COL_985\" VARCHAR(30)\n" +
                ",\"COL_986\" BIGINT\n" +
                ",\"COL_987\" DECIMAL(4,2)\n" +
                ",\"COL_988\" BIGINT\n" +
                ",\"COL_989\" VARCHAR(30)\n" +
                ",\"COL_990\" DATE\n" +
                ",\"COL_991\" VARCHAR(30)\n" +
                ",\"COL_992\" VARCHAR(30)\n" +
                ",\"COL_993\" CHAR(10)\n" +
                ",\"COL_994\" INTEGER\n" +
                ",\"COL_995\" BIGINT\n" +
                ",\"COL_996\" INTEGER\n" +
                ",\"COL_997\" DATE\n" +
                ",\"COL_998\" DECIMAL(4,2)\n" +
                ",\"COL_999\" BIGINT\n" +
                ",\"COL_1000\" DATE\n" +
                ",\"COL_1001\" CHAR(10)\n" +
                ",\"COL_1002\" CHAR(10)\n" +
                ",\"COL_1003\" CHAR(10)\n" +
                ",\"COL_1004\" DECIMAL(4,2)\n" +
                ",\"COL_1005\" INTEGER\n" +
                ",\"COL_1006\" CHAR(10)\n" +
                ",\"COL_1007\" CHAR(10)\n" +
                ",\"COL_1008\" INTEGER\n" +
                ",\"COL_1009\" CHAR(10)\n" +
                ",\"COL_1010\" CHAR(10)\n" +
                ",\"COL_1011\" VARCHAR(30)\n" +
                ",\"COL_1012\" VARCHAR(30)\n" +
                ",\"COL_1013\" DECIMAL(4,2)\n" +
                ",\"COL_1014\" VARCHAR(30)\n" +
                ",\"COL_1015\" DECIMAL(4,2)\n" +
                ",\"COL_1016\" INTEGER\n" +
                ",\"COL_1017\" DECIMAL(4,2)\n" +
                ",\"COL_1018\" CHAR(10)\n" +
                ",\"COL_1019\" DATE\n" +
                ",\"COL_1020\" CHAR(10)\n" +
                ",\"COL_1021\" DECIMAL(4,2)\n" +
                ",\"COL_1022\" INTEGER\n" +
                ",\"COL_1023\" DATE\n" +
                ",\"COL_1024\" DECIMAL(4,2)\n" +
                ",\"COL_1025\" INTEGER\n" +
                ",\"COL_1026\" DATE\n" +
                ",\"COL_1027\" INTEGER\n" +
                ",\"COL_1028\" CHAR(10)\n" +
                ",\"COL_1029\" INTEGER\n" +
                ",\"COL_1030\" INTEGER\n" +
                ",\"COL_1031\" BIGINT\n" +
                ",\"COL_1032\" VARCHAR(30)\n" +
                ",\"COL_1033\" VARCHAR(30)\n" +
                ",\"COL_1034\" DECIMAL(4,2)\n" +
                ",\"COL_1035\" VARCHAR(30)\n" +
                ",\"COL_1036\" BIGINT\n" +
                ",\"COL_1037\" INTEGER\n" +
                ",\"COL_1038\" INTEGER\n" +
                ",\"COL_1039\" VARCHAR(30)\n" +
                ",\"COL_1040\" BIGINT\n" +
                ",\"COL_1041\" BIGINT\n" +
                ",\"COL_1042\" DECIMAL(4,2)\n" +
                ",\"COL_1043\" CHAR(10)\n" +
                ",\"COL_1044\" CHAR(10)\n" +
                ",\"COL_1045\" DECIMAL(4,2)\n" +
                ",\"COL_1046\" DATE\n" +
                ",\"COL_1047\" INTEGER\n" +
                ",\"COL_1048\" CHAR(10)\n" +
                ",\"COL_1049\" VARCHAR(30)\n" +
                ",\"COL_1050\" DATE\n" +
                ",\"COL_1051\" DECIMAL(4,2)\n" +
                ",\"COL_1052\" CHAR(10)\n" +
                ",\"COL_1053\" BIGINT\n" +
                ",\"COL_1054\" BIGINT\n" +
                ",\"COL_1055\" DATE\n" +
                ",\"COL_1056\" DATE\n" +
                ",\"COL_1057\" INTEGER\n" +
                ",\"COL_1058\" VARCHAR(30)\n" +
                ",\"COL_1059\" DECIMAL(4,2)\n" +
                ",\"COL_1060\" VARCHAR(30)\n" +
                ",\"COL_1061\" DATE\n" +
                ",\"COL_1062\" BIGINT\n" +
                ",\"COL_1063\" INTEGER\n" +
                ",\"COL_1064\" INTEGER\n" +
                ",\"COL_1065\" INTEGER\n" +
                ",\"COL_1066\" VARCHAR(30)\n" +
                ",\"COL_1067\" DECIMAL(4,2)\n" +
                ",\"COL_1068\" BIGINT\n" +
                ",\"COL_1069\" CHAR(10)\n" +
                ",\"COL_1070\" VARCHAR(30)\n" +
                ",\"COL_1071\" VARCHAR(30)\n" +
                ",\"COL_1072\" DECIMAL(4,2)\n" +
                ",\"COL_1073\" CHAR(10)\n" +
                ",\"COL_1074\" INTEGER\n" +
                ",\"COL_1075\" INTEGER\n" +
                ",\"COL_1076\" CHAR(10)\n" +
                ",\"COL_1077\" VARCHAR(30)\n" +
                ",\"COL_1078\" VARCHAR(30)\n" +
                ",\"COL_1079\" DATE\n" +
                ",\"COL_1080\" INTEGER\n" +
                ",\"COL_1081\" VARCHAR(30)\n" +
                ",\"COL_1082\" BIGINT\n" +
                ",\"COL_1083\" CHAR(10)\n" +
                ",\"COL_1084\" INTEGER\n" +
                ",\"COL_1085\" INTEGER\n" +
                ",\"COL_1086\" DECIMAL(4,2)\n" +
                ",\"COL_1087\" INTEGER\n" +
                ",\"COL_1088\" VARCHAR(30)\n" +
                ",\"COL_1089\" INTEGER\n" +
                ",\"COL_1090\" CHAR(10)\n" +
                ",\"COL_1091\" CHAR(10)\n" +
                ",\"COL_1092\" VARCHAR(30)\n" +
                ",\"COL_1093\" INTEGER\n" +
                ",\"COL_1094\" VARCHAR(30)\n" +
                ",\"COL_1095\" DECIMAL(4,2)\n" +
                ",\"COL_1096\" DATE\n" +
                ",\"COL_1097\" BIGINT\n" +
                ",\"COL_1098\" VARCHAR(30)\n" +
                ",\"COL_1099\" CHAR(10)\n" +
                ",\"COL_1100\" DECIMAL(4,2)\n" +
                ",\"COL_1101\" INTEGER\n" +
                ",\"COL_1102\" BIGINT\n" +
                ",\"COL_1103\" INTEGER\n" +
                ",\"COL_1104\" DECIMAL(4,2)\n" +
                ",\"COL_1105\" BIGINT\n" +
                ",\"COL_1106\" DECIMAL(4,2)\n" +
                ",\"COL_1107\" BIGINT\n" +
                ",\"COL_1108\" DECIMAL(4,2)\n" +
                ",\"COL_1109\" DECIMAL(4,2)\n" +
                ",\"COL_1110\" CHAR(10)\n" +
                ",\"COL_1111\" CHAR(10)\n" +
                ",\"COL_1112\" BIGINT\n" +
                ",\"COL_1113\" BIGINT\n" +
                ",\"COL_1114\" INTEGER\n" +
                ",\"COL_1115\" CHAR(10)\n" +
                ",\"COL_1116\" INTEGER\n" +
                ",\"COL_1117\" BIGINT\n" +
                ",\"COL_1118\" DATE\n" +
                ",\"COL_1119\" VARCHAR(30)\n" +
                ",\"COL_1120\" CHAR(10)\n" +
                ",\"COL_1121\" DECIMAL(4,2)\n" +
                ",\"COL_1122\" BIGINT\n" +
                ",\"COL_1123\" DECIMAL(4,2)\n" +
                ",\"COL_1124\" BIGINT\n" +
                ",\"COL_1125\" INTEGER\n" +
                ",\"COL_1126\" DATE\n" +
                ",\"COL_1127\" DECIMAL(4,2)\n" +
                ",\"COL_1128\" BIGINT\n" +
                ",\"COL_1129\" CHAR(10)\n" +
                ",\"COL_1130\" INTEGER\n" +
                ",\"COL_1131\" VARCHAR(30)\n" +
                ",\"COL_1132\" DECIMAL(4,2)\n" +
                ",\"COL_1133\" CHAR(10)\n" +
                ",\"COL_1134\" CHAR(10)\n" +
                ",\"COL_1135\" CHAR(10)\n" +
                ",\"COL_1136\" CHAR(10)\n" +
                ",\"COL_1137\" DECIMAL(4,2)\n" +
                ",\"COL_1138\" INTEGER\n" +
                ",\"COL_1139\" INTEGER\n" +
                ",\"COL_1140\" DATE\n" +
                ",\"COL_1141\" BIGINT\n" +
                ",\"COL_1142\" DECIMAL(4,2)\n" +
                ",\"COL_1143\" CHAR(10)\n" +
                ",\"COL_1144\" DECIMAL(4,2)\n" +
                ",\"COL_1145\" DATE\n" +
                ",\"COL_1146\" DECIMAL(4,2)\n" +
                ",\"COL_1147\" BIGINT\n" +
                ",\"COL_1148\" DATE\n" +
                ",\"COL_1149\" BIGINT\n" +
                ",\"COL_1150\" DATE\n" +
                ",\"COL_1151\" INTEGER\n" +
                ",\"COL_1152\" CHAR(10)\n" +
                ",\"COL_1153\" CHAR(10)\n" +
                ",\"COL_1154\" DECIMAL(4,2)\n" +
                ",\"COL_1155\" BIGINT\n" +
                ",\"COL_1156\" CHAR(10)\n" +
                ",\"COL_1157\" CHAR(10)\n" +
                ",\"COL_1158\" DECIMAL(4,2)\n" +
                ",\"COL_1159\" VARCHAR(30)\n" +
                ",\"COL_1160\" DECIMAL(4,2)\n" +
                ",\"COL_1161\" DATE\n" +
                ",\"COL_1162\" BIGINT\n" +
                ",\"COL_1163\" VARCHAR(30)\n" +
                ",\"COL_1164\" DECIMAL(4,2)\n" +
                ",\"COL_1165\" VARCHAR(30)\n" +
                ",\"COL_1166\" INTEGER\n" +
                ",\"COL_1167\" DATE\n" +
                ",\"COL_1168\" DATE\n" +
                ",\"COL_1169\" INTEGER\n" +
                ",\"COL_1170\" CHAR(10)\n" +
                ",\"COL_1171\" BIGINT\n" +
                ",\"COL_1172\" CHAR(10)\n" +
                ",\"COL_1173\" VARCHAR(30)\n" +
                ",\"COL_1174\" INTEGER\n" +
                ",\"COL_1175\" DECIMAL(4,2)\n" +
                ",\"COL_1176\" DECIMAL(4,2)\n" +
                ",\"COL_1177\" BIGINT\n" +
                ",\"COL_1178\" DECIMAL(4,2)\n" +
                ",\"COL_1179\" DATE\n" +
                ",\"COL_1180\" CHAR(10)\n" +
                ",\"COL_1181\" VARCHAR(30)\n" +
                ",\"COL_1182\" INTEGER\n" +
                ",\"COL_1183\" INTEGER\n" +
                ",\"COL_1184\" DECIMAL(4,2)\n" +
                ",\"COL_1185\" VARCHAR(30)\n" +
                ",\"COL_1186\" BIGINT\n" +
                ",\"COL_1187\" DECIMAL(4,2)\n" +
                ",\"COL_1188\" DECIMAL(4,2)\n" +
                ",\"COL_1189\" BIGINT\n" +
                ",\"COL_1190\" DECIMAL(4,2)\n" +
                ",\"COL_1191\" DECIMAL(4,2)\n" +
                ",\"COL_1192\" INTEGER\n" +
                ",\"COL_1193\" INTEGER\n" +
                ",\"COL_1194\" INTEGER\n" +
                ",\"COL_1195\" DECIMAL(4,2)\n" +
                ",\"COL_1196\" CHAR(10)\n" +
                ",\"COL_1197\" DATE\n" +
                ",\"COL_1198\" BIGINT\n" +
                ",\"COL_1199\" INTEGER\n" +
                ",\"COL_1200\" BIGINT\n" +
                ",\"COL_1201\" CHAR(10)\n" +
                ",\"COL_1202\" DATE\n" +
                ",\"COL_1203\" INTEGER\n" +
                ",\"COL_1204\" CHAR(10)\n" +
                ",\"COL_1205\" DATE\n" +
                ",\"COL_1206\" BIGINT\n" +
                ",\"COL_1207\" BIGINT\n" +
                ",\"COL_1208\" INTEGER\n" +
                ",\"COL_1209\" CHAR(10)\n" +
                ",\"COL_1210\" DECIMAL(4,2)\n" +
                ",\"COL_1211\" DATE\n" +
                ",\"COL_1212\" DATE\n" +
                ",\"COL_1213\" INTEGER\n" +
                ",\"COL_1214\" DECIMAL(4,2)\n" +
                ",\"COL_1215\" INTEGER\n" +
                ",\"COL_1216\" BIGINT\n" +
                ",\"COL_1217\" BIGINT\n" +
                ",\"COL_1218\" DATE\n" +
                ",\"COL_1219\" VARCHAR(30)\n" +
                ",\"COL_1220\" INTEGER\n" +
                ",\"COL_1221\" VARCHAR(30)\n" +
                ",\"COL_1222\" INTEGER\n" +
                ",\"COL_1223\" INTEGER\n" +
                ",\"COL_1224\" DECIMAL(4,2)\n" +
                ",\"COL_1225\" CHAR(10)\n" +
                ",\"COL_1226\" INTEGER\n" +
                ",\"COL_1227\" BIGINT\n" +
                ",\"COL_1228\" CHAR(10)\n" +
                ",\"COL_1229\" DATE\n" +
                ",\"COL_1230\" BIGINT\n" +
                ",\"COL_1231\" DECIMAL(4,2)\n" +
                ",\"COL_1232\" DECIMAL(4,2)\n" +
                ",\"COL_1233\" CHAR(10)\n" +
                ",\"COL_1234\" DATE\n" +
                ",\"COL_1235\" DECIMAL(4,2)\n" +
                ",\"COL_1236\" DATE\n" +
                ",\"COL_1237\" CHAR(10)\n" +
                ",\"COL_1238\" DECIMAL(4,2)\n" +
                ",\"COL_1239\" CHAR(10)\n" +
                ",\"COL_1240\" CHAR(10)\n" +
                ",\"COL_1241\" DECIMAL(4,2)\n" +
                ",\"COL_1242\" DECIMAL(4,2)\n" +
                ",\"COL_1243\" CHAR(10)\n" +
                ",\"COL_1244\" DATE\n" +
                ",\"COL_1245\" DECIMAL(4,2)\n" +
                ",\"COL_1246\" DATE\n" +
                ",\"COL_1247\" DECIMAL(4,2)\n" +
                ",\"COL_1248\" DATE\n" +
                ",\"COL_1249\" DATE\n" +
                ",\"COL_1250\" INTEGER\n" +
                ",\"COL_1251\" DECIMAL(4,2)\n" +
                ",\"COL_1252\" CHAR(10)\n" +
                ",\"COL_1253\" DATE\n" +
                ",\"COL_1254\" VARCHAR(30)\n" +
                ",\"COL_1255\" DECIMAL(4,2)\n" +
                ",\"COL_1256\" BIGINT\n" +
                ",\"COL_1257\" DECIMAL(4,2)\n" +
                ",\"COL_1258\" BIGINT\n" +
                ",\"COL_1259\" DECIMAL(4,2)\n" +
                ",\"COL_1260\" VARCHAR(30)\n" +
                ",\"COL_1261\" CHAR(10)\n" +
                ",\"COL_1262\" VARCHAR(30)\n" +
                ",\"COL_1263\" CHAR(10)\n" +
                ",\"COL_1264\" VARCHAR(30)\n" +
                ",\"COL_1265\" DATE\n" +
                ",\"COL_1266\" DECIMAL(4,2)\n" +
                ",\"COL_1267\" DATE\n" +
                ",\"COL_1268\" VARCHAR(30)\n" +
                ",\"COL_1269\" DECIMAL(4,2)\n" +
                ",\"COL_1270\" DECIMAL(4,2)\n" +
                ",\"COL_1271\" DECIMAL(4,2)\n" +
                ",\"COL_1272\" DECIMAL(4,2)\n" +
                ",\"COL_1273\" BIGINT\n" +
                ",\"COL_1274\" DECIMAL(4,2)\n" +
                ",\"COL_1275\" BIGINT\n" +
                ",\"COL_1276\" INTEGER\n" +
                ",\"COL_1277\" DECIMAL(4,2)\n" +
                ",\"COL_1278\" DATE\n" +
                ",\"COL_1279\" CHAR(10)\n" +
                ",\"COL_1280\" CHAR(10)\n" +
                ",\"COL_1281\" DATE\n" +
                ",\"COL_1282\" BIGINT\n" +
                ",\"COL_1283\" VARCHAR(30)\n" +
                ",\"COL_1284\" DATE\n" +
                ",\"COL_1285\" DATE\n" +
                ",\"COL_1286\" DECIMAL(4,2)\n" +
                ",\"COL_1287\" CHAR(10)\n" +
                ",\"COL_1288\" DECIMAL(4,2)\n" +
                ",\"COL_1289\" DATE\n" +
                ",\"COL_1290\" BIGINT\n" +
                ",\"COL_1291\" INTEGER\n" +
                ",\"COL_1292\" BIGINT\n" +
                ",\"COL_1293\" VARCHAR(30)\n" +
                ",\"COL_1294\" CHAR(10)\n" +
                ",\"COL_1295\" CHAR(10)\n" +
                ",\"COL_1296\" CHAR(10)\n" +
                ",\"COL_1297\" BIGINT\n" +
                ",\"COL_1298\" BIGINT\n" +
                ",\"COL_1299\" BIGINT\n" +
                ",\"COL_1300\" VARCHAR(30)\n" +
                ",\"COL_1301\" BIGINT\n" +
                ",\"COL_1302\" VARCHAR(30)\n" +
                ",\"COL_1303\" CHAR(10)\n" +
                ",\"COL_1304\" INTEGER\n" +
                ",\"COL_1305\" BIGINT\n" +
                ",\"COL_1306\" CHAR(10)\n" +
                ",\"COL_1307\" INTEGER\n" +
                ",\"COL_1308\" INTEGER\n" +
                ",\"COL_1309\" CHAR(10)\n" +
                ",\"COL_1310\" VARCHAR(30)\n" +
                ",\"COL_1311\" VARCHAR(30)\n" +
                ",\"COL_1312\" VARCHAR(30)\n" +
                ",\"COL_1313\" CHAR(10)\n" +
                ",\"COL_1314\" BIGINT\n" +
                ",\"COL_1315\" BIGINT\n" +
                ",\"COL_1316\" BIGINT\n" +
                ",\"COL_1317\" INTEGER\n" +
                ",\"COL_1318\" INTEGER\n" +
                ",\"COL_1319\" VARCHAR(30)\n" +
                ",\"COL_1320\" BIGINT\n" +
                ",\"COL_1321\" DATE\n" +
                ",\"COL_1322\" DECIMAL(4,2)\n" +
                ",\"COL_1323\" INTEGER\n" +
                ",\"COL_1324\" VARCHAR(30)\n" +
                ",\"COL_1325\" BIGINT\n" +
                ",\"COL_1326\" CHAR(10)\n" +
                ",\"COL_1327\" DECIMAL(4,2)\n" +
                ",\"COL_1328\" VARCHAR(30)\n" +
                ",\"COL_1329\" DECIMAL(4,2)\n" +
                ",\"COL_1330\" VARCHAR(30)\n" +
                ",\"COL_1331\" CHAR(10)\n" +
                ",\"COL_1332\" CHAR(10)\n" +
                ",\"COL_1333\" DECIMAL(4,2)\n" +
                ",\"COL_1334\" VARCHAR(30)\n" +
                ",\"COL_1335\" INTEGER\n" +
                ",\"COL_1336\" INTEGER\n" +
                ",\"COL_1337\" BIGINT\n" +
                ",\"COL_1338\" DATE\n" +
                ",\"COL_1339\" BIGINT\n" +
                ",\"COL_1340\" VARCHAR(30)\n" +
                ",\"COL_1341\" DECIMAL(4,2)\n" +
                ",\"COL_1342\" DATE\n" +
                ",\"COL_1343\" CHAR(10)\n" +
                ",\"COL_1344\" CHAR(10)\n" +
                ",\"COL_1345\" INTEGER\n" +
                ",\"COL_1346\" VARCHAR(30)\n" +
                ",\"COL_1347\" DATE\n" +
                ",\"COL_1348\" VARCHAR(30)\n" +
                ",\"COL_1349\" VARCHAR(30)\n" +
                ",\"COL_1350\" DECIMAL(4,2)\n" +
                ",\"COL_1351\" DECIMAL(4,2)\n" +
                ",\"COL_1352\" INTEGER\n" +
                ",\"COL_1353\" INTEGER\n" +
                ",\"COL_1354\" DECIMAL(4,2)\n" +
                ",\"COL_1355\" DATE\n" +
                ",\"COL_1356\" VARCHAR(30)\n" +
                ",\"COL_1357\" DATE\n" +
                ",\"COL_1358\" CHAR(10)\n" +
                ",\"COL_1359\" DECIMAL(4,2)\n" +
                ",\"COL_1360\" DECIMAL(4,2)\n" +
                ",\"COL_1361\" INTEGER\n" +
                ",\"COL_1362\" CHAR(10)\n" +
                ",\"COL_1363\" DATE\n" +
                ",\"COL_1364\" DECIMAL(4,2)\n" +
                ",\"COL_1365\" INTEGER\n" +
                ",\"COL_1366\" VARCHAR(30)\n" +
                ",\"COL_1367\" INTEGER\n" +
                ",\"COL_1368\" INTEGER\n" +
                ",\"COL_1369\" DATE\n" +
                ",\"COL_1370\" INTEGER\n" +
                ",\"COL_1371\" CHAR(10)\n" +
                ",\"COL_1372\" INTEGER\n" +
                ",\"COL_1373\" CHAR(10)\n" +
                ",\"COL_1374\" VARCHAR(30)\n" +
                ",\"COL_1375\" VARCHAR(30)\n" +
                ",\"COL_1376\" DATE\n" +
                ",\"COL_1377\" CHAR(10)\n" +
                ",\"COL_1378\" BIGINT\n" +
                ",\"COL_1379\" VARCHAR(30)\n" +
                ",\"COL_1380\" CHAR(10)\n" +
                ",\"COL_1381\" CHAR(10)\n" +
                ",\"COL_1382\" DECIMAL(4,2)\n" +
                ",\"COL_1383\" BIGINT\n" +
                ",\"COL_1384\" VARCHAR(30)\n" +
                ",\"COL_1385\" CHAR(10)\n" +
                ",\"COL_1386\" INTEGER\n" +
                ",\"COL_1387\" INTEGER\n" +
                ",\"COL_1388\" DECIMAL(4,2)\n" +
                ",\"COL_1389\" VARCHAR(30)\n" +
                ",\"COL_1390\" INTEGER\n" +
                ",\"COL_1391\" CHAR(10)\n" +
                ",\"COL_1392\" VARCHAR(30)\n" +
                ",\"COL_1393\" BIGINT\n" +
                ",\"COL_1394\" DATE\n" +
                ",\"COL_1395\" VARCHAR(30)\n" +
                ",\"COL_1396\" CHAR(10)\n" +
                ",\"COL_1397\" BIGINT\n" +
                ",\"COL_1398\" DECIMAL(4,2)\n" +
                ",\"COL_1399\" DATE\n" +
                ",\"COL_1400\" DECIMAL(4,2)\n" +
                ",\"COL_1401\" INTEGER\n" +
                ",\"COL_1402\" DECIMAL(4,2)\n" +
                ",\"COL_1403\" CHAR(10)\n" +
                ",\"COL_1404\" DATE\n" +
                ",\"COL_1405\" CHAR(10)\n" +
                ",\"COL_1406\" CHAR(10)\n" +
                ",\"COL_1407\" VARCHAR(30)\n" +
                ",\"COL_1408\" VARCHAR(30)\n" +
                ",\"COL_1409\" DECIMAL(4,2)\n" +
                ",\"COL_1410\" BIGINT\n" +
                ",\"COL_1411\" CHAR(10)\n" +
                ",\"COL_1412\" DECIMAL(4,2)\n" +
                ",\"COL_1413\" DECIMAL(4,2)\n" +
                ",\"COL_1414\" DECIMAL(4,2)\n" +
                ",\"COL_1415\" VARCHAR(30)\n" +
                ",\"COL_1416\" DATE\n" +
                ",\"COL_1417\" CHAR(10)\n" +
                ",\"COL_1418\" INTEGER\n" +
                ",\"COL_1419\" CHAR(10)\n" +
                ",\"COL_1420\" BIGINT\n" +
                ",\"COL_1421\" CHAR(10)\n" +
                ",\"COL_1422\" DECIMAL(4,2)\n" +
                ",\"COL_1423\" CHAR(10)\n" +
                ",\"COL_1424\" BIGINT\n" +
                ",\"COL_1425\" VARCHAR(30)\n" +
                ",\"COL_1426\" INTEGER\n" +
                ",\"COL_1427\" DECIMAL(4,2)\n" +
                ",\"COL_1428\" VARCHAR(30)\n" +
                ",\"COL_1429\" DECIMAL(4,2)\n" +
                ",\"COL_1430\" BIGINT\n" +
                ",\"COL_1431\" VARCHAR(30)\n" +
                ",\"COL_1432\" CHAR(10)\n" +
                ",\"COL_1433\" DATE\n" +
                ",\"COL_1434\" DATE\n" +
                ",\"COL_1435\" INTEGER\n" +
                ",\"COL_1436\" CHAR(10)\n" +
                ",\"COL_1437\" CHAR(10)\n" +
                ",\"COL_1438\" DECIMAL(4,2)\n" +
                ",\"COL_1439\" DATE\n" +
                ",\"COL_1440\" DATE\n" +
                ",\"COL_1441\" DATE\n" +
                ",\"COL_1442\" CHAR(10)\n" +
                ",\"COL_1443\" BIGINT\n" +
                ",\"COL_1444\" CHAR(10)\n" +
                ",\"COL_1445\" CHAR(10)\n" +
                ",\"COL_1446\" DATE\n" +
                ",\"COL_1447\" VARCHAR(30)\n" +
                ",\"COL_1448\" VARCHAR(30)\n" +
                ",\"COL_1449\" VARCHAR(30)\n" +
                ",\"COL_1450\" CHAR(10)\n" +
                ",\"COL_1451\" VARCHAR(30)\n" +
                ",\"COL_1452\" DECIMAL(4,2)\n" +
                ",\"COL_1453\" BIGINT\n" +
                ",\"COL_1454\" VARCHAR(30)\n" +
                ",\"COL_1455\" VARCHAR(30)\n" +
                ",\"COL_1456\" INTEGER\n" +
                ",\"COL_1457\" CHAR(10)\n" +
                ",\"COL_1458\" DECIMAL(4,2)\n" +
                ",\"COL_1459\" CHAR(10)\n" +
                ",\"COL_1460\" VARCHAR(30)\n" +
                ",\"COL_1461\" DATE\n" +
                ",\"COL_1462\" DATE\n" +
                ",\"COL_1463\" DECIMAL(4,2)\n" +
                ",\"COL_1464\" DECIMAL(4,2)\n" +
                ",\"COL_1465\" DECIMAL(4,2)\n" +
                ",\"COL_1466\" CHAR(10)\n" +
                ",\"COL_1467\" DATE\n" +
                ",\"COL_1468\" DECIMAL(4,2)\n" +
                ",\"COL_1469\" INTEGER\n" +
                ",\"COL_1470\" DECIMAL(4,2)\n" +
                ",\"COL_1471\" DATE\n" +
                ",\"COL_1472\" DATE\n" +
                ",\"COL_1473\" VARCHAR(30)\n" +
                ",\"COL_1474\" VARCHAR(30)\n" +
                ",\"COL_1475\" DATE\n" +
                ",\"COL_1476\" DATE\n" +
                ",\"COL_1477\" CHAR(10)\n" +
                ",\"COL_1478\" DATE\n" +
                ",\"COL_1479\" DATE\n" +
                ",\"COL_1480\" BIGINT\n" +
                ",\"COL_1481\" CHAR(10)\n" +
                ",\"COL_1482\" DECIMAL(4,2)\n" +
                ",\"COL_1483\" INTEGER\n" +
                ",\"COL_1484\" VARCHAR(30)\n" +
                ",\"COL_1485\" CHAR(10)\n" +
                ",\"COL_1486\" INTEGER\n" +
                ",\"COL_1487\" DECIMAL(4,2)\n" +
                ",\"COL_1488\" DECIMAL(4,2)\n" +
                ",\"COL_1489\" CHAR(10)\n" +
                ",\"COL_1490\" VARCHAR(30)\n" +
                ",\"COL_1491\" DECIMAL(4,2)\n" +
                ",\"COL_1492\" BIGINT\n" +
                ",\"COL_1493\" VARCHAR(30)\n" +
                ",\"COL_1494\" CHAR(10)\n" +
                ",\"COL_1495\" INTEGER\n" +
                ",\"COL_1496\" BIGINT\n" +
                ",\"COL_1497\" DECIMAL(4,2)\n" +
                ",\"COL_1498\" DECIMAL(4,2)\n" +
                ",\"COL_1499\" BIGINT\n" +
                ",\"COL_1500\" VARCHAR(30)\n" +
                ",\"COL_1501\" VARCHAR(30)\n" +
                ",\"COL_1502\" BIGINT\n" +
                ",\"COL_1503\" BIGINT\n" +
                ",\"COL_1504\" INTEGER\n" +
                ",\"COL_1505\" DECIMAL(4,2)\n" +
                ",\"COL_1506\" BIGINT\n" +
                ",\"COL_1507\" VARCHAR(30)\n" +
                ",\"COL_1508\" BIGINT\n" +
                ",\"COL_1509\" DATE\n" +
                ",\"COL_1510\" CHAR(10)\n" +
                ",\"COL_1511\" DATE\n" +
                ",\"COL_1512\" CHAR(10)\n" +
                ",\"COL_1513\" DATE\n" +
                ",\"COL_1514\" INTEGER\n" +
                ",\"COL_1515\" VARCHAR(30)\n" +
                ",\"COL_1516\" BIGINT\n" +
                ",\"COL_1517\" BIGINT\n" +
                ",\"COL_1518\" VARCHAR(30)\n" +
                ",\"COL_1519\" CHAR(10)\n" +
                ",\"COL_1520\" DECIMAL(4,2)\n" +
                ",\"COL_1521\" CHAR(10)\n" +
                ",\"COL_1522\" VARCHAR(30)\n" +
                ",\"COL_1523\" DECIMAL(4,2)\n" +
                ",\"COL_1524\" INTEGER\n" +
                ",\"COL_1525\" VARCHAR(30)\n" +
                ",\"COL_1526\" BIGINT\n" +
                ",\"COL_1527\" DECIMAL(4,2)\n" +
                ",\"COL_1528\" BIGINT\n" +
                ",\"COL_1529\" BIGINT\n" +
                ",\"COL_1530\" CHAR(10)\n" +
                ",\"COL_1531\" INTEGER\n" +
                ",\"COL_1532\" INTEGER\n" +
                ",\"COL_1533\" BIGINT\n" +
                ",\"COL_1534\" VARCHAR(30)\n" +
                ",\"COL_1535\" CHAR(10)\n" +
                ",\"COL_1536\" CHAR(10)\n" +
                ",\"COL_1537\" CHAR(10)\n" +
                ",\"COL_1538\" INTEGER\n" +
                ",\"COL_1539\" BIGINT\n" +
                ",\"COL_1540\" BIGINT\n" +
                ",\"COL_1541\" CHAR(10)\n" +
                ",\"COL_1542\" BIGINT\n" +
                ",\"COL_1543\" BIGINT\n" +
                ",\"COL_1544\" DECIMAL(4,2)\n" +
                ",\"COL_1545\" BIGINT\n" +
                ",\"COL_1546\" VARCHAR(30)\n" +
                ",\"COL_1547\" DECIMAL(4,2)\n" +
                ",\"COL_1548\" INTEGER\n" +
                ",\"COL_1549\" DECIMAL(4,2)\n" +
                ",\"COL_1550\" INTEGER\n" +
                ",\"COL_1551\" VARCHAR(30)\n" +
                ",\"COL_1552\" CHAR(10)\n" +
                ",\"COL_1553\" VARCHAR(30)\n" +
                ",\"COL_1554\" BIGINT\n" +
                ",\"COL_1555\" VARCHAR(30)\n" +
                ",\"COL_1556\" DECIMAL(4,2)\n" +
                ",\"COL_1557\" DATE\n" +
                ",\"COL_1558\" DECIMAL(4,2)\n" +
                ",\"COL_1559\" DATE\n" +
                ",\"COL_1560\" CHAR(10)\n" +
                ",\"COL_1561\" BIGINT\n" +
                ",\"COL_1562\" INTEGER\n" +
                ",\"COL_1563\" VARCHAR(30)\n" +
                ",\"COL_1564\" INTEGER\n" +
                ",\"COL_1565\" CHAR(10)\n" +
                ",\"COL_1566\" INTEGER\n" +
                ",\"COL_1567\" VARCHAR(30)\n" +
                ",\"COL_1568\" BIGINT\n" +
                ",\"COL_1569\" CHAR(10)\n" +
                ",\"COL_1570\" VARCHAR(30)\n" +
                ",\"COL_1571\" INTEGER\n" +
                ",\"COL_1572\" DATE\n" +
                ",\"COL_1573\" DECIMAL(4,2)\n" +
                ",\"COL_1574\" INTEGER\n" +
                ",\"COL_1575\" DECIMAL(4,2)\n" +
                ",\"COL_1576\" VARCHAR(30)\n" +
                ",\"COL_1577\" DATE\n" +
                ",\"COL_1578\" INTEGER\n" +
                ",\"COL_1579\" INTEGER\n" +
                ",\"COL_1580\" BIGINT\n" +
                ",\"COL_1581\" VARCHAR(30)\n" +
                ",\"COL_1582\" DATE\n" +
                ",\"COL_1583\" BIGINT\n" +
                ",\"COL_1584\" DECIMAL(4,2)\n" +
                ",\"COL_1585\" CHAR(10)\n" +
                ",\"COL_1586\" INTEGER\n" +
                ",\"COL_1587\" CHAR(10)\n" +
                ",\"COL_1588\" BIGINT\n" +
                ",\"COL_1589\" BIGINT\n" +
                ",\"COL_1590\" VARCHAR(30)\n" +
                ",\"COL_1591\" BIGINT\n" +
                ",\"COL_1592\" VARCHAR(30)\n" +
                ",\"COL_1593\" BIGINT\n" +
                ",\"COL_1594\" DECIMAL(4,2)\n" +
                ",\"COL_1595\" DECIMAL(4,2)\n" +
                ",\"COL_1596\" VARCHAR(30)\n" +
                ",\"COL_1597\" CHAR(10)\n" +
                ",\"COL_1598\" DATE\n" +
                ",\"COL_1599\" DECIMAL(4,2)\n" +
                ",\"COL_1600\" DECIMAL(4,2)\n" +
                ",\"COL_1601\" DECIMAL(4,2)\n" +
                ",\"COL_1602\" DATE\n" +
                ",\"COL_1603\" INTEGER\n" +
                ",\"COL_1604\" BIGINT\n" +
                ",\"COL_1605\" DECIMAL(4,2)\n" +
                ",\"COL_1606\" DATE\n" +
                ",\"COL_1607\" DECIMAL(4,2)\n" +
                ",\"COL_1608\" CHAR(10)\n" +
                ",\"COL_1609\" DECIMAL(4,2)\n" +
                ",\"COL_1610\" VARCHAR(30)\n" +
                ",\"COL_1611\" CHAR(10)\n" +
                ",\"COL_1612\" DATE\n" +
                ",\"COL_1613\" DATE\n" +
                ",\"COL_1614\" CHAR(10)\n" +
                ",\"COL_1615\" CHAR(10)\n" +
                ",\"COL_1616\" INTEGER\n" +
                ",\"COL_1617\" CHAR(10)\n" +
                ",\"COL_1618\" BIGINT\n" +
                ",\"COL_1619\" DECIMAL(4,2)\n" +
                ",\"COL_1620\" CHAR(10)\n" +
                ",\"COL_1621\" VARCHAR(30)\n" +
                ",\"COL_1622\" CHAR(10)\n" +
                ",\"COL_1623\" INTEGER\n" +
                ",\"COL_1624\" INTEGER\n" +
                ",\"COL_1625\" CHAR(10)\n" +
                ",\"COL_1626\" VARCHAR(30)\n" +
                ",\"COL_1627\" CHAR(10)\n" +
                ",\"COL_1628\" DATE\n" +
                ",\"COL_1629\" BIGINT\n" +
                ",\"COL_1630\" INTEGER\n" +
                ",\"COL_1631\" INTEGER\n" +
                ",\"COL_1632\" CHAR(10)\n" +
                ",\"COL_1633\" DATE\n" +
                ",\"COL_1634\" VARCHAR(30)\n" +
                ",\"COL_1635\" DECIMAL(4,2)\n" +
                ",\"COL_1636\" DECIMAL(4,2)\n" +
                ",\"COL_1637\" DATE\n" +
                ",\"COL_1638\" DECIMAL(4,2)\n" +
                ",\"COL_1639\" DATE\n" +
                ",\"COL_1640\" DATE\n" +
                ",\"COL_1641\" VARCHAR(30)\n" +
                ",\"COL_1642\" CHAR(10)\n" +
                ",\"COL_1643\" VARCHAR(30)\n" +
                ",\"COL_1644\" BIGINT\n" +
                ",\"COL_1645\" INTEGER\n" +
                ",\"COL_1646\" DECIMAL(4,2)\n" +
                ",\"COL_1647\" INTEGER\n" +
                ",\"COL_1648\" DATE\n" +
                ",\"COL_1649\" BIGINT\n" +
                ",\"COL_1650\" INTEGER\n" +
                ",\"COL_1651\" CHAR(10)\n" +
                ",\"COL_1652\" DATE\n" +
                ",\"COL_1653\" CHAR(10)\n" +
                ",\"COL_1654\" DATE\n" +
                ",\"COL_1655\" INTEGER\n" +
                ",\"COL_1656\" BIGINT\n" +
                ",\"COL_1657\" BIGINT\n" +
                ",\"COL_1658\" BIGINT\n" +
                ",\"COL_1659\" BIGINT\n" +
                ",\"COL_1660\" DATE\n" +
                ",\"COL_1661\" INTEGER\n" +
                ",\"COL_1662\" CHAR(10)\n" +
                ",\"COL_1663\" DECIMAL(4,2)\n" +
                ",\"COL_1664\" INTEGER\n" +
                ",\"COL_1665\" VARCHAR(30)\n" +
                ",\"COL_1666\" INTEGER\n" +
                ",\"COL_1667\" INTEGER\n" +
                ",\"COL_1668\" DECIMAL(4,2)\n" +
                ",\"COL_1669\" CHAR(10)\n" +
                ",\"COL_1670\" VARCHAR(30)\n" +
                ",\"COL_1671\" BIGINT\n" +
                ",\"COL_1672\" DATE\n" +
                ",\"COL_1673\" DATE\n" +
                ",\"COL_1674\" CHAR(10)\n" +
                ",\"COL_1675\" DATE\n" +
                ",\"COL_1676\" CHAR(10)\n" +
                ",\"COL_1677\" INTEGER\n" +
                ",\"COL_1678\" DECIMAL(4,2)\n" +
                ",\"COL_1679\" DATE\n" +
                ",\"COL_1680\" DECIMAL(4,2)\n" +
                ",\"COL_1681\" BIGINT\n" +
                ",\"COL_1682\" CHAR(10)\n" +
                ",\"COL_1683\" DECIMAL(4,2)\n" +
                ",\"COL_1684\" INTEGER\n" +
                ",\"COL_1685\" DECIMAL(4,2)\n" +
                ",\"COL_1686\" CHAR(10)\n" +
                ",\"COL_1687\" DECIMAL(4,2)\n" +
                ",\"COL_1688\" BIGINT\n" +
                ",\"COL_1689\" INTEGER\n" +
                ",\"COL_1690\" DECIMAL(4,2)\n" +
                ",\"COL_1691\" CHAR(10)\n" +
                ",\"COL_1692\" DECIMAL(4,2)\n" +
                ",\"COL_1693\" INTEGER\n" +
                ",\"COL_1694\" BIGINT\n" +
                ",\"COL_1695\" VARCHAR(30)\n" +
                ",\"COL_1696\" BIGINT\n" +
                ",\"COL_1697\" INTEGER\n" +
                ",\"COL_1698\" DECIMAL(4,2)\n" +
                ",\"COL_1699\" DECIMAL(4,2)\n" +
                ",\"COL_1700\" BIGINT\n" +
                ",\"COL_1701\" INTEGER\n" +
                ",\"COL_1702\" DECIMAL(4,2)\n" +
                ",\"COL_1703\" VARCHAR(30)\n" +
                ",\"COL_1704\" DECIMAL(4,2)\n" +
                ",\"COL_1705\" BIGINT\n" +
                ",\"COL_1706\" CHAR(10)\n" +
                ",\"COL_1707\" BIGINT\n" +
                ",\"COL_1708\" CHAR(10)\n" +
                ",\"COL_1709\" VARCHAR(30)\n" +
                ",\"COL_1710\" VARCHAR(30)\n" +
                ",\"COL_1711\" CHAR(10)\n" +
                ",\"COL_1712\" VARCHAR(30)\n" +
                ",\"COL_1713\" INTEGER\n" +
                ",\"COL_1714\" DATE\n" +
                ",\"COL_1715\" INTEGER\n" +
                ",\"COL_1716\" DECIMAL(4,2)\n" +
                ",\"COL_1717\" DATE\n" +
                ",\"COL_1718\" DATE\n" +
                ",\"COL_1719\" DATE\n" +
                ",\"COL_1720\" BIGINT\n" +
                ",\"COL_1721\" DATE\n" +
                ",\"COL_1722\" CHAR(10)\n" +
                ",\"COL_1723\" VARCHAR(30)\n" +
                ",\"COL_1724\" BIGINT\n" +
                ",\"COL_1725\" VARCHAR(30)\n" +
                ",\"COL_1726\" DECIMAL(4,2)\n" +
                ",\"COL_1727\" CHAR(10)\n" +
                ",\"COL_1728\" BIGINT\n" +
                ",\"COL_1729\" BIGINT\n" +
                ",\"COL_1730\" BIGINT\n" +
                ",\"COL_1731\" BIGINT\n" +
                ",\"COL_1732\" BIGINT\n" +
                ",\"COL_1733\" CHAR(10)\n" +
                ",\"COL_1734\" VARCHAR(30)\n" +
                ",\"COL_1735\" BIGINT\n" +
                ",\"COL_1736\" INTEGER\n" +
                ",\"COL_1737\" DATE\n" +
                ",\"COL_1738\" DECIMAL(4,2)\n" +
                ",\"COL_1739\" CHAR(10)\n" +
                ",\"COL_1740\" DATE\n" +
                ",\"COL_1741\" INTEGER\n" +
                ",\"COL_1742\" CHAR(10)\n" +
                ",\"COL_1743\" BIGINT\n" +
                ",\"COL_1744\" VARCHAR(30)\n" +
                ",\"COL_1745\" INTEGER\n" +
                ",\"COL_1746\" BIGINT\n" +
                ",\"COL_1747\" DATE\n" +
                ",\"COL_1748\" CHAR(10)\n" +
                ",\"COL_1749\" BIGINT\n" +
                ",\"COL_1750\" DATE\n" +
                ",\"COL_1751\" DECIMAL(4,2)\n" +
                ",\"COL_1752\" BIGINT\n" +
                ",\"COL_1753\" CHAR(10)\n" +
                ",\"COL_1754\" BIGINT\n" +
                ",\"COL_1755\" INTEGER\n" +
                ",\"COL_1756\" VARCHAR(30)\n" +
                ",\"COL_1757\" VARCHAR(30)\n" +
                ",\"COL_1758\" VARCHAR(30)\n" +
                ",\"COL_1759\" BIGINT\n" +
                ",\"COL_1760\" DATE\n" +
                ",\"COL_1761\" BIGINT\n" +
                ",\"COL_1762\" VARCHAR(30)\n" +
                ",\"COL_1763\" INTEGER\n" +
                ",\"COL_1764\" INTEGER\n" +
                ",\"COL_1765\" BIGINT\n" +
                ",\"COL_1766\" DATE\n" +
                ",\"COL_1767\" DECIMAL(4,2)\n" +
                ",\"COL_1768\" VARCHAR(30)\n" +
                ",\"COL_1769\" DECIMAL(4,2)\n" +
                ",\"COL_1770\" VARCHAR(30)\n" +
                ",\"COL_1771\" BIGINT\n" +
                ",\"COL_1772\" CHAR(10)\n" +
                ",\"COL_1773\" CHAR(10)\n" +
                ",\"COL_1774\" DATE\n" +
                ",\"COL_1775\" BIGINT\n" +
                ",\"COL_1776\" CHAR(10)\n" +
                ",\"COL_1777\" CHAR(10)\n" +
                ",\"COL_1778\" CHAR(10)\n" +
                ",\"COL_1779\" DECIMAL(4,2)\n" +
                ",\"COL_1780\" INTEGER\n" +
                ",\"COL_1781\" VARCHAR(30)\n" +
                ",\"COL_1782\" INTEGER\n" +
                ",\"COL_1783\" CHAR(10)\n" +
                ",\"COL_1784\" VARCHAR(30)\n" +
                ",\"COL_1785\" DATE\n" +
                ",\"COL_1786\" DECIMAL(4,2)\n" +
                ",\"COL_1787\" DATE\n" +
                ",\"COL_1788\" CHAR(10)\n" +
                ",\"COL_1789\" DECIMAL(4,2)\n" +
                ",\"COL_1790\" DECIMAL(4,2)\n" +
                ",\"COL_1791\" DECIMAL(4,2)\n" +
                ",\"COL_1792\" CHAR(10)\n" +
                ",\"COL_1793\" BIGINT\n" +
                ",\"COL_1794\" CHAR(10)\n" +
                ",\"COL_1795\" BIGINT\n" +
                ",\"COL_1796\" DATE\n" +
                ",\"COL_1797\" INTEGER\n" +
                ",\"COL_1798\" BIGINT\n" +
                ",\"COL_1799\" CHAR(10)\n" +
                ",\"COL_1800\" BIGINT\n" +
                ",\"COL_1801\" DECIMAL(4,2)\n" +
                ",\"COL_1802\" INTEGER\n" +
                ",\"COL_1803\" INTEGER\n" +
                ",\"COL_1804\" DATE\n" +
                ",\"COL_1805\" DATE\n" +
                ",\"COL_1806\" BIGINT\n" +
                ",\"COL_1807\" BIGINT\n" +
                ",\"COL_1808\" VARCHAR(30)\n" +
                ",\"COL_1809\" VARCHAR(30)\n" +
                ",\"COL_1810\" BIGINT\n" +
                ",\"COL_1811\" INTEGER\n" +
                ",\"COL_1812\" DATE\n" +
                ",\"COL_1813\" BIGINT\n" +
                ",\"COL_1814\" BIGINT\n" +
                ",\"COL_1815\" DECIMAL(4,2)\n" +
                ",\"COL_1816\" BIGINT\n" +
                ",\"COL_1817\" BIGINT\n" +
                ",\"COL_1818\" DECIMAL(4,2)\n" +
                ",\"COL_1819\" CHAR(10)\n" +
                ",\"COL_1820\" DECIMAL(4,2)\n" +
                ",\"COL_1821\" DATE\n" +
                ",\"COL_1822\" CHAR(10)\n" +
                ",\"COL_1823\" VARCHAR(30)\n" +
                ",\"COL_1824\" VARCHAR(30)\n" +
                ",\"COL_1825\" BIGINT\n" +
                ",\"COL_1826\" DECIMAL(4,2)\n" +
                ",\"COL_1827\" BIGINT\n" +
                ",\"COL_1828\" VARCHAR(30)\n" +
                ",\"COL_1829\" BIGINT\n" +
                ",\"COL_1830\" BIGINT\n" +
                ",\"COL_1831\" DECIMAL(4,2)\n" +
                ",\"COL_1832\" DECIMAL(4,2)\n" +
                ",\"COL_1833\" DECIMAL(4,2)\n" +
                ",\"COL_1834\" DECIMAL(4,2)\n" +
                ",\"COL_1835\" DATE\n" +
                ",\"COL_1836\" BIGINT\n" +
                ",\"COL_1837\" CHAR(10)\n" +
                ",\"COL_1838\" DATE\n" +
                ",\"COL_1839\" DECIMAL(4,2)\n" +
                ",\"COL_1840\" INTEGER\n" +
                ",\"COL_1841\" DECIMAL(4,2)\n" +
                ",\"COL_1842\" DATE\n" +
                ",\"COL_1843\" BIGINT\n" +
                ",\"COL_1844\" VARCHAR(30)\n" +
                ",\"COL_1845\" BIGINT\n" +
                ",\"COL_1846\" BIGINT\n" +
                ",\"COL_1847\" INTEGER\n" +
                ",\"COL_1848\" DATE\n" +
                ",\"COL_1849\" CHAR(10)\n" +
                ",\"COL_1850\" BIGINT\n" +
                ",\"COL_1851\" BIGINT\n" +
                ",\"COL_1852\" DECIMAL(4,2)\n" +
                ",\"COL_1853\" BIGINT\n" +
                ",\"COL_1854\" VARCHAR(30)\n" +
                ",\"COL_1855\" VARCHAR(30)\n" +
                ",\"COL_1856\" INTEGER\n" +
                ",\"COL_1857\" INTEGER\n" +
                ",\"COL_1858\" CHAR(10)\n" +
                ",\"COL_1859\" VARCHAR(30)\n" +
                ",\"COL_1860\" CHAR(10)\n" +
                ",\"COL_1861\" BIGINT\n" +
                ",\"COL_1862\" BIGINT\n" +
                ",\"COL_1863\" DATE\n" +
                ",\"COL_1864\" DATE\n" +
                ",\"COL_1865\" BIGINT\n" +
                ",\"COL_1866\" DATE\n" +
                ",\"COL_1867\" INTEGER\n" +
                ",\"COL_1868\" BIGINT\n" +
                ",\"COL_1869\" CHAR(10)\n" +
                ",\"COL_1870\" BIGINT\n" +
                ",\"COL_1871\" VARCHAR(30)\n" +
                ",\"COL_1872\" CHAR(10)\n" +
                ",\"COL_1873\" CHAR(10)\n" +
                ",\"COL_1874\" DECIMAL(4,2)\n" +
                ",\"COL_1875\" INTEGER\n" +
                ",\"COL_1876\" INTEGER\n" +
                ",\"COL_1877\" INTEGER\n" +
                ",\"COL_1878\" CHAR(10)\n" +
                ",\"COL_1879\" DECIMAL(4,2)\n" +
                ",\"COL_1880\" DATE\n" +
                ",\"COL_1881\" DATE\n" +
                ",\"COL_1882\" VARCHAR(30)\n" +
                ",\"COL_1883\" DATE\n" +
                ",\"COL_1884\" INTEGER\n" +
                ",\"COL_1885\" BIGINT\n" +
                ",\"COL_1886\" CHAR(10)\n" +
                ",\"COL_1887\" INTEGER\n" +
                ",\"COL_1888\" DATE\n" +
                ",\"COL_1889\" DATE\n" +
                ",\"COL_1890\" DATE\n" +
                ",\"COL_1891\" DATE\n" +
                ",\"COL_1892\" DATE\n" +
                ",\"COL_1893\" BIGINT\n" +
                ",\"COL_1894\" DATE\n" +
                ",\"COL_1895\" DECIMAL(4,2)\n" +
                ",\"COL_1896\" BIGINT\n" +
                ",\"COL_1897\" INTEGER\n" +
                ",\"COL_1898\" DECIMAL(4,2)\n" +
                ",\"COL_1899\" BIGINT\n" +
                ",\"COL_1900\" DATE\n" +
                ",\"COL_1901\" BIGINT\n" +
                ",\"COL_1902\" BIGINT\n" +
                ",\"COL_1903\" VARCHAR(30)\n" +
                ",\"COL_1904\" BIGINT\n" +
                ",\"COL_1905\" VARCHAR(30)\n" +
                ",\"COL_1906\" VARCHAR(30)\n" +
                ",\"COL_1907\" INTEGER\n" +
                ",\"COL_1908\" BIGINT\n" +
                ",\"COL_1909\" DATE\n" +
                ",\"COL_1910\" VARCHAR(30)\n" +
                ",\"COL_1911\" CHAR(10)\n" +
                ",\"COL_1912\" DECIMAL(4,2)\n" +
                ",\"COL_1913\" DECIMAL(4,2)\n" +
                ",\"COL_1914\" VARCHAR(30)\n" +
                ",\"COL_1915\" INTEGER\n" +
                ",\"COL_1916\" INTEGER\n" +
                ",\"COL_1917\" BIGINT\n" +
                ",\"COL_1918\" BIGINT\n" +
                ",\"COL_1919\" DECIMAL(4,2)\n" +
                ",\"COL_1920\" DECIMAL(4,2)\n" +
                ",\"COL_1921\" BIGINT\n" +
                ",\"COL_1922\" DATE\n" +
                ",\"COL_1923\" INTEGER\n" +
                ",\"COL_1924\" INTEGER\n" +
                ",\"COL_1925\" DATE\n" +
                ",\"COL_1926\" DATE\n" +
                ",\"COL_1927\" CHAR(10)\n" +
                ",\"COL_1928\" INTEGER\n" +
                ",\"COL_1929\" VARCHAR(30)\n" +
                ",\"COL_1930\" CHAR(10)\n" +
                ",\"COL_1931\" DECIMAL(4,2)\n" +
                ",\"COL_1932\" DATE\n" +
                ",\"COL_1933\" DECIMAL(4,2)\n" +
                ",\"COL_1934\" BIGINT\n" +
                ",\"COL_1935\" DECIMAL(4,2)\n" +
                ",\"COL_1936\" DECIMAL(4,2)\n" +
                ",\"COL_1937\" DATE\n" +
                ",\"COL_1938\" INTEGER\n" +
                ",\"COL_1939\" DECIMAL(4,2)\n" +
                ",\"COL_1940\" CHAR(10)\n" +
                ",\"COL_1941\" DECIMAL(4,2)\n" +
                ",\"COL_1942\" INTEGER\n" +
                ",\"COL_1943\" VARCHAR(30)\n" +
                ",\"COL_1944\" VARCHAR(30)\n" +
                ",\"COL_1945\" DATE\n" +
                ",\"COL_1946\" INTEGER\n" +
                ",\"COL_1947\" DATE\n" +
                ",\"COL_1948\" CHAR(10)\n" +
                ",\"COL_1949\" DECIMAL(4,2)\n" +
                ",\"COL_1950\" BIGINT\n" +
                ",\"COL_1951\" BIGINT\n" +
                ",\"COL_1952\" DECIMAL(4,2)\n" +
                ",\"COL_1953\" VARCHAR(30)\n" +
                ",\"COL_1954\" DECIMAL(4,2)\n" +
                ",\"COL_1955\" BIGINT\n" +
                ",\"COL_1956\" DATE\n" +
                ",\"COL_1957\" INTEGER\n" +
                ",\"COL_1958\" DECIMAL(4,2)\n" +
                ",\"COL_1959\" BIGINT\n" +
                ",\"COL_1960\" VARCHAR(30)\n" +
                ",\"COL_1961\" DECIMAL(4,2)\n" +
                ",\"COL_1962\" DATE\n" +
                ",\"COL_1963\" DECIMAL(4,2)\n" +
                ",\"COL_1964\" DATE\n" +
                ",\"COL_1965\" CHAR(10)\n" +
                ",\"COL_1966\" BIGINT\n" +
                ",\"COL_1967\" CHAR(10)\n" +
                ",\"COL_1968\" CHAR(10)\n" +
                ",\"COL_1969\" CHAR(10)\n" +
                ",\"COL_1970\" CHAR(10)\n" +
                ",\"COL_1971\" INTEGER\n" +
                ",\"COL_1972\" BIGINT\n" +
                ",\"COL_1973\" VARCHAR(30)\n" +
                ",\"COL_1974\" INTEGER\n" +
                ",\"COL_1975\" INTEGER\n" +
                ",\"COL_1976\" VARCHAR(30)\n" +
                ",\"COL_1977\" CHAR(10)\n" +
                ",\"COL_1978\" BIGINT\n" +
                ",\"COL_1979\" VARCHAR(30)\n" +
                ",\"COL_1980\" DATE\n" +
                ",\"COL_1981\" VARCHAR(30)\n" +
                ",\"COL_1982\" CHAR(10)\n" +
                ",\"COL_1983\" VARCHAR(30)\n" +
                ",\"COL_1984\" DECIMAL(4,2)\n" +
                ",\"COL_1985\" DECIMAL(4,2)\n" +
                ",\"COL_1986\" VARCHAR(30)\n" +
                ",\"COL_1987\" BIGINT\n" +
                ",\"COL_1988\" DECIMAL(4,2)\n" +
                ",\"COL_1989\" INTEGER\n" +
                ",\"COL_1990\" DECIMAL(4,2)\n" +
                ",\"COL_1991\" VARCHAR(30)\n" +
                ",\"COL_1992\" DATE\n" +
                ",\"COL_1993\" DECIMAL(4,2)\n" +
                ",\"COL_1994\" DECIMAL(4,2)\n" +
                ",\"COL_1995\" DATE\n" +
                ",\"COL_1996\" BIGINT\n" +
                ",\"COL_1997\" DATE\n" +
                ",\"COL_1998\" DATE\n" +
                ",\"COL_1999\" INTEGER\n" +
                ",\"COL_2000\" BIGINT\n" +
                ",\"COL_2001\" VARCHAR(30)\n" +
                ",\"COL_2002\" DATE\n" +
                ",\"COL_2003\" CHAR(10)\n" +
                ",\"COL_2004\" DECIMAL(4,2)\n" +
                ",\"COL_2005\" INTEGER\n" +
                ",\"COL_2006\" CHAR(10)\n" +
                ",\"COL_2007\" DECIMAL(4,2)\n" +
                ",\"COL_2008\" CHAR(10)\n" +
                ",\"COL_2009\" INTEGER\n" +
                ",\"COL_2010\" VARCHAR(30)\n" +
                ",\"COL_2011\" INTEGER\n" +
                ",\"COL_2012\" DATE\n" +
                ",\"COL_2013\" DATE\n" +
                ",\"COL_2014\" INTEGER\n" +
                ",\"COL_2015\" DATE\n" +
                ",\"COL_2016\" DATE\n" +
                ",\"COL_2017\" BIGINT\n" +
                ",\"COL_2018\" INTEGER\n" +
                ",\"COL_2019\" BIGINT\n" +
                ",\"COL_2020\" BIGINT\n" +
                ",\"COL_2021\" BIGINT\n" +
                ",\"COL_2022\" VARCHAR(30)\n" +
                ",\"COL_2023\" DATE\n" +
                ",\"COL_2024\" VARCHAR(30)\n" +
                ",\"COL_2025\" DECIMAL(4,2)\n" +
                ",\"COL_2026\" BIGINT\n" +
                ",\"COL_2027\" DECIMAL(4,2)\n" +
                ",\"COL_2028\" INTEGER\n" +
                ",\"COL_2029\" CHAR(10)\n" +
                ",\"COL_2030\" DECIMAL(4,2)\n" +
                ",\"COL_2031\" DECIMAL(4,2)\n" +
                ",\"COL_2032\" BIGINT\n" +
                ",\"COL_2033\" VARCHAR(30)\n" +
                ",\"COL_2034\" CHAR(10)\n" +
                ",\"COL_2035\" DATE\n" +
                ",\"COL_2036\" BIGINT\n" +
                ",\"COL_2037\" DECIMAL(4,2)\n" +
                ",\"COL_2038\" INTEGER\n" +
                ",\"COL_2039\" INTEGER\n" +
                ",\"COL_2040\" DECIMAL(4,2)\n" +
                ",\"COL_2041\" DECIMAL(4,2)\n" +
                ",\"COL_2042\" DECIMAL(4,2)\n" +
                ",\"COL_2043\" DECIMAL(4,2)\n" +
                ",\"COL_2044\" CHAR(10)\n" +
                ",\"COL_2045\" DECIMAL(4,2)\n" +
                ",\"COL_2046\" INTEGER\n" +
                ",\"COL_2047\" CHAR(10)\n" +
                ",\"COL_2048\" DECIMAL(4,2)\n" +
                ",\"COL_2049\" DECIMAL(4,2)\n" +
                ",\"COL_2050\" BIGINT\n" +
                ",\"COL_2051\" CHAR(10)\n" +
                ",\"COL_2052\" BIGINT\n" +
                ",\"COL_2053\" DECIMAL(4,2)\n" +
                ",\"COL_2054\" VARCHAR(30)\n" +
                ",\"COL_2055\" DECIMAL(4,2)\n" +
                ",\"COL_2056\" INTEGER\n" +
                ",\"COL_2057\" CHAR(10)\n" +
                ",\"COL_2058\" INTEGER\n" +
                ",\"COL_2059\" DATE\n" +
                ",\"COL_2060\" DECIMAL(4,2)\n" +
                ",\"COL_2061\" VARCHAR(30)\n" +
                ",\"COL_2062\" DECIMAL(4,2)\n" +
                ",\"COL_2063\" CHAR(10)\n" +
                ",\"COL_2064\" INTEGER\n" +
                ",\"COL_2065\" BIGINT\n" +
                ",\"COL_2066\" CHAR(10)\n" +
                ",\"COL_2067\" DECIMAL(4,2)\n" +
                ",\"COL_2068\" INTEGER\n" +
                ",\"COL_2069\" VARCHAR(30)\n" +
                ",\"COL_2070\" CHAR(10)\n" +
                ",\"COL_2071\" DECIMAL(4,2)\n" +
                ",\"COL_2072\" DECIMAL(4,2)\n" +
                ",\"COL_2073\" DECIMAL(4,2)\n" +
                ",\"COL_2074\" INTEGER\n" +
                ",\"COL_2075\" CHAR(10)\n" +
                ",\"COL_2076\" DATE\n" +
                ",\"COL_2077\" BIGINT\n" +
                ",\"COL_2078\" INTEGER\n" +
                ",\"COL_2079\" CHAR(10)\n" +
                ",\"COL_2080\" CHAR(10)\n" +
                ",\"COL_2081\" VARCHAR(30)\n" +
                ",\"COL_2082\" VARCHAR(30)\n" +
                ",\"COL_2083\" INTEGER\n" +
                ",\"COL_2084\" VARCHAR(30)\n" +
                ",\"COL_2085\" BIGINT\n" +
                ",\"COL_2086\" BIGINT\n" +
                ",\"COL_2087\" INTEGER\n" +
                ",\"COL_2088\" CHAR(10)\n" +
                ",\"COL_2089\" INTEGER\n" +
                ",\"COL_2090\" VARCHAR(30)\n" +
                ",\"COL_2091\" DECIMAL(4,2)\n" +
                ",\"COL_2092\" CHAR(10)\n" +
                ",\"COL_2093\" VARCHAR(30)\n" +
                ",\"COL_2094\" DATE\n" +
                ",\"COL_2095\" VARCHAR(30)\n" +
                ",\"COL_2096\" VARCHAR(30)\n" +
                ",\"COL_2097\" VARCHAR(30)\n" +
                ",\"COL_2098\" INTEGER\n" +
                ",\"COL_2099\" VARCHAR(30)\n" +
                ") ";

        methodWatcher.execute(ddl);

        try (ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','TEST_2100')")){
            rs.next();
            String result = rs.getString(1);
            Assert.assertEquals(ddl + ";", result);
        }
    }
}
