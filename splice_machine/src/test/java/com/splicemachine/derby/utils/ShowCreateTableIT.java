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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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


    }

    @AfterClass
    public static void cleanup()
    {
        classWatcher.closeAll();
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
                ", CONSTRAINT " + csName +" PRIMARY KEY(A1)) ;", ddl);
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
                ", CONSTRAINT " + csName + " PRIMARY KEY(A2,B2)) ;", ddl);
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
                ", CONSTRAINT " + csName + " PRIMARY KEY(B9,A9)) ;", ddl);
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
                ",CONSTRAINT T3_FK_1 FOREIGN KEY (A2,B2) REFERENCES T2(A2,B2) ON UPDATE NO ACTION ON DELETE NO ACTION) ;", rs.getString(1));
    }

    @Test
    public void testMultipleForeignKeys() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T4')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T4\" (\n" +
                "\"A4\" INTEGER\n" +
                ",\"B2\" INTEGER\n" +
                ",\"A2\" INTEGER\n" +
                ",\"A1\" INTEGER\n" +
                ",CONSTRAINT T4_FK_2 FOREIGN KEY (A1) REFERENCES T1(A1) ON UPDATE NO ACTION ON DELETE NO ACTION,CONSTRAINT T4_FK_1 FOREIGN KEY (A2,B2) REFERENCES T2(A2,B2) ON UPDATE NO ACTION ON DELETE NO ACTION) ;", rs.getString(1));
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
                ",CONSTRAINT T5_FK_1 FOREIGN KEY (A5F,B5F) REFERENCES T2(A2,B2) ON UPDATE NO ACTION ON DELETE NO ACTION) ;", rs.getString(1));
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
                ",CONSTRAINT T11_FK_1 FOREIGN KEY (A2,B2) REFERENCES T2(A2,B2) ON UPDATE RESTRICT ON DELETE RESTRICT) ;", rs.getString(1));
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
                ",CONSTRAINT T12_FK_1 FOREIGN KEY (A2,B2) REFERENCES T2(A2,B2) ON UPDATE NO ACTION ON DELETE NO ACTION) ;", rs.getString(1));
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
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T6\" (\n" +
                "\"A6\" INTEGER\n" +
                ",\"B6\" INTEGER\n" +
                ",\"C6\" INTEGER\n" +
                ",\"D6\" INTEGER\n" +
                ", CONSTRAINT " + csName + " UNIQUE (D6), CONSTRAINT U_T6_1 UNIQUE (A6), CONSTRAINT U_T6_2 UNIQUE (B6,C6)) ;", ddl);
    }

    @Test
    public void testCheck() throws Exception
    {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T7')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T7\" (\n" +
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
            Assert.assertEquals("42X05",sqlState);
        }
    }

    @Test
    public void testDateColumnHavingDefaultValue() throws Exception
    {
        String DDL = "CREATE TABLE \"SHOWCREATETABLEIT\".\"T13\"\n" +
                "(\n" +
                "\"I\" DATE DEFAULT '2019-01-01'\n" +
                ",\"J\" DATE DEFAULT CURRENT_DATE\n" +
                ")";
        methodWatcher.executeUpdate(DDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T13')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T13\" (\n" +
                "\"I\" DATE DEFAULT '2019-01-01'\n" +
                ",\"J\" DATE DEFAULT CURRENT_DATE\n" +
                ") ;", rs.getString(1));

    }

    @Test
    public void testTimeColumnHavingDefaultValue() throws Exception
    {
        String DDL = "CREATE TABLE \"SHOWCREATETABLEIT\".\"T14\"\n" +
                "(\n" +
                "\"I\" TIME DEFAULT '18:18:18'\n" +
                ",\"J\" TIME DEFAULT CURRENT_TIME\n" +
                ")";
        methodWatcher.executeUpdate(DDL);
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.SHOW_CREATE_TABLE('SHOWCREATETABLEIT','T14')");
        rs.next();
        Assert.assertEquals("CREATE TABLE \"SHOWCREATETABLEIT\".\"T14\" (\n" +
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
}
