/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by jyuan on 10/12/15.
 */
public class VTIOperationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = VTIOperationIT.class.getSimpleName().toUpperCase();
    private static final String TABLE_NAME="EMPLOYEE";

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    private Connection conn;

    @Before
    public void setUp() throws Exception{
        conn = spliceClassWatcher.getOrCreateConnection();
    }

    @After
    public void tearDown() throws Exception{
        conn.close();
    }

    @BeforeClass
    public static void setupClass() throws Exception {
        setup(spliceClassWatcher);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        try(Connection conn =spliceClassWatcher.getOrCreateConnection()){
            try(Statement s = conn.createStatement()){
                s.execute("drop function JDBCTableVTI ");
                s.execute("drop function JDBCSQLVTI ");
            }
        }
    }

    private static void setup(SpliceWatcher spliceClassWatcher) throws Exception {
        try(Connection conn = spliceClassWatcher.getOrCreateConnection()){
            new TableCreator(conn)
                    .withCreate("create table employee (name varchar(56),id bigint,salary numeric(9,2),ranking int)")
                    .withInsert("insert into employee values(?,?,?,?)")
                    .withRows(rows(
                            row("Andy",1,100000,1),
                            row("Billy",2,100000,2))).create();

            String tableVti="create function JDBCTableVTI(conn varchar(32672), s varchar(1024), t varchar(1024))\n"+
                    "returns table\n"+
                    "(\n"+
                    "   name varchar(56),\n"+
                    "   id bigint,\n"+
                    "   salary numeric(9,2),\n"+
                    "   ranking int\n"+
                    ")\n"+
                    "language java\n"+
                    "parameter style SPLICE_JDBC_RESULT_SET\n"+
                    "no sql\n"+
                    "external name 'com.splicemachine.derby.vti.SpliceJDBCVTI.getJDBCTableVTI'";

            String sqlVti="create function JDBCSQLVTI(conn varchar(32672), s varchar(32672))\n"+
                    "returns table\n"+
                    "(\n"+
                    "   name varchar(56),\n"+
                    "   id bigint,\n"+
                    "   salary numeric(9,2),\n"+
                    "   ranking int\n"+
                    ")\n"+
                    "language java\n"+
                    "parameter style SPLICE_JDBC_RESULT_SET\n"+
                    "no sql\n"+
                    "external name 'com.splicemachine.derby.vti.SpliceJDBCVTI.getJDBCSQLVTI'";
            try(Statement s = conn.createStatement()){
                s.execute(tableVti);

                s.execute(sqlVti);
            }
        }
    }

    @Test
    public void testJDBCSQLVTI() throws Exception {
        String sql = String.format("select * from table (JDBCSQLVTI('jdbc:splice://localhost:1527/splicedb;create=true;" +
                "user=splice;password=admin', " +
                "'select * from %s.%s'))a", CLASS_NAME, TABLE_NAME);
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery(sql)){
                int count=0;
                while(rs.next()){
                    count++;
                }
                Assert.assertEquals(2,count);
            }
        }
    }

    @Test
    public void testJDBCTableVTI() throws Exception {
        String sql = String.format("select * from table (JDBCTableVTI('jdbc:spliceClustered://localhost:1527/splicedb;create=true;user=splice;password=admin', '%s', '%s'))a", CLASS_NAME, TABLE_NAME);
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery(sql)){
                int count=0;
                while(rs.next()){
                    count++;
                }
                Assert.assertEquals(2,count);
            }
        }
    }

    @Test
    public void testJDBCTableVTIWithJoin() throws Exception {
        String sql = String.format("select * from table (JDBCTableVTI('jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin', '%s', '%s'))a" +
                ", employee where employee.id = a.id", CLASS_NAME, TABLE_NAME);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testFileVTI() throws Exception {
        String location = getResourceDirectory()+"importTest.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b (c1 varchar(128), c2 varchar(128), c3 int)", location);
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery(sql)){
                int count=0;
                while(rs.next()){
                    count++;
                }
                Assert.assertEquals(5,count);
            }
        }
    }


    @Test
    public void testFileVTIWithJoin() throws Exception {
        String location = getResourceDirectory()+"importTest.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b (c1 varchar(128), c2 varchar(128), c3 int)" +
                ", employee where employee.id + 25 = b.c3", location);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    @Ignore("DB-4641: failing when in Jenkins when run under the mem DB profile")
    public void testFileVTIExpectError() throws Exception {
        String location = getResourceDirectory()+"importTest.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b" +
                                       " (name varchar(10), title varchar(30), age int, something varchar(12), " +
                                       "date_hired timestamp, clock time)\n" +
                                       " where age < 40 and date_hired > TIMESTAMP('2015-08-21', '08:09:08') order" +
                                       " by name", location);
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery(sql)){
                fail("Expected: java.sql.SQLException: Number of columns in column definition, 6, differ from those found in import file 3.");
            }catch(SQLException e){
                // expected: "Number of columns in column definition, 6, differ from those found in import file 3. "
                assertEquals("XIE0A",e.getSQLState());
                return;
            }
        }
        fail("Expected: java.sql.SQLException: Number of columns in column definition, 6, differ from those found in import file 3.");
    }

    @Test
    public void testFileVTITypes() throws Exception {
        String location = getResourceDirectory()+"vtiConversion.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b" +
                                       " (name varchar(10), title varchar(30), age int, something varchar(12), " +
                                       "date_hired timestamp, clock time)\n" +
                                       " where age < 40 and date_hired > TIMESTAMP('2015-08-21', '08:09:08') order" +
                                       " by name", location);
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery(sql)){
                String expected=
                        "NAME  |          TITLE          | AGE | SOMETHING  |     DATE_HIRED       |  CLOCK  |\n"+
                                "--------------------------------------------------------------------------------------\n"+
                                "jzhang | How The West Won Texas  | 34  |08-23X-2015 |2015-08-22 08:12:08.0 |11:08:08 |\n"+
                                "sfines |Senior Software Engineer | 27  |08X-27-2015 |2015-08-27 08:08:08.0 |06:08:08 |";
                assertEquals("\n"+sql+"\n",expected,TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            }
        }
    }

    @Test
    public void testVTIConversion() throws Exception {
        String location = getResourceDirectory()+"vtiConversion.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') " +
                "as b (c1 varchar(128), c2 varchar(128), c3 varchar(128), c4 varchar(128), c5 varchar(128), " +
                "c6 varchar(128))", location);
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery(sql)){
                int count=0;
                while(rs.next()){
                    count++;
                }
                Assert.assertEquals(5,count);
            }
        }
    }
}
