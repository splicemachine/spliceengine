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

package com.splicemachine.derby.impl.sql.execute.operations;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.format;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Tests around creating tables with no data and with data calls.
 * <p/>
 * e.g. sql that looks like "create table as ... with [no] data".
 *
 * @author Scott Fines
 *         Date: 12/18/13
 */
public class CreateTableWithDataIT{
    protected static SpliceWatcher spliceClassWatcher=new SpliceWatcher();

    protected static SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(CreateTableWithDataIT.class.getSimpleName());
    protected static SpliceTableWatcher baseTable=new SpliceTableWatcher("T",spliceSchemaWatcher.schemaName,"(a int, b int)");
    protected static SpliceTableWatcher rightTable=new SpliceTableWatcher("R",spliceSchemaWatcher.schemaName,"(b int, c int)");
    protected static SpliceTableWatcher decimalTable=new SpliceTableWatcher("D",spliceSchemaWatcher.schemaName,"(d decimal(15, 2))");

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(baseTable)
            .around(decimalTable)
            .around(rightTable).around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(PreparedStatement ps=spliceClassWatcher.prepareStatement(String.format("insert into %s (a,b) values (?,?)",baseTable))){
                        for(int i=0;i<10;i++){
                            ps.setInt(1,i);
                            ps.setInt(2,2*i);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }

                    try(PreparedStatement ps=spliceClassWatcher.prepareStatement(String.format("insert into %s (b,c) values (?,?)",rightTable))){
                        for(int i=0;i<10;i++){
                            ps.setInt(1,2*i);
                            ps.setInt(2,i);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher();

    private Connection conn;

    @Before
    public void setUp() throws Exception{
        conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();

    }

    @Test
    public void testCreateTableWithNoDataHasNoData() throws Exception{
        //confirmation test that we don't break anything that derby does correctly
        try(PreparedStatement ps=conn.prepareStatement(String.format("create table %s.t2 as select * from %s with no data",spliceSchemaWatcher.schemaName,baseTable))){
            int numRows=ps.executeUpdate();
            Assert.assertEquals("It claims to have updated rows!",0,numRows);

        }
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select * from "+spliceSchemaWatcher.schemaName+".t2")){
                Assert.assertFalse("Rows returned by no data!",rs.next());
            }
        }
    }


    @Test
    public void testCreateTableWithData() throws Exception{
        try(PreparedStatement ps=methodWatcher.prepareStatement(String.format("create table %s.t3 as select * from %s with data",spliceSchemaWatcher.schemaName,baseTable))){
            int numRows=ps.executeUpdate();
            Assert.assertEquals("It does not claim to have updated rows!",10,numRows);

        }

        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select * from "+spliceSchemaWatcher.schemaName+".t3")){
                int count=0;
                while(rs.next()){
                    int first=rs.getInt(1);
                    int second=rs.getInt(2);
                    Assert.assertEquals("Incorrect row: ("+first+","+second+")",first*2,second);
                    count++;
                }
                Assert.assertEquals("Incorrect row count",10,count);
            }
        }
    }

    @Test
    public void testCTASQuotedName() throws Exception {
        try(PreparedStatement ps=methodWatcher.prepareStatement(String.format("create table %s.\"t3\" as select * from %s with data",spliceSchemaWatcher.schemaName,baseTable))){
            int numRows=ps.executeUpdate();
            Assert.assertEquals("It does not claim to have updated rows!",10,numRows);

        }

        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select * from "+spliceSchemaWatcher.schemaName+".\"t3\"")){
                int count=0;
                while(rs.next()){
                    int first=rs.getInt(1);
                    int second=rs.getInt(2);
                    Assert.assertEquals("Incorrect row: ("+first+","+second+")",first*2,second);
                    count++;
                }
                Assert.assertEquals("Incorrect row count",10,count);
            }
        }
    }


    @Test
    public void testCreateTableWithData2() throws Exception{
        try(PreparedStatement ps=conn.prepareStatement(String.format("create table %s.t4 as select t1.a, t2.c from %s t1, %s t2 where t1.b = t2.b with data",spliceSchemaWatcher.schemaName,baseTable,rightTable))){
            int numRows=ps.executeUpdate();
            Assert.assertEquals("It claims to have updated rows!",10,numRows);

        }
        try(Statement s= conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select * from "+spliceSchemaWatcher.schemaName+".t4")){
                int count=0;
                while(rs.next()){
                    int first=rs.getInt(1);
                    int second=rs.getInt(2);
                    Assert.assertEquals("Incorrect row: ("+first+","+second+")",first,second);
                    count++;
                }
                Assert.assertEquals("Incorrect row count",10,count);
            }
        }
    }

    @Test
    public void testCreateTableWithoutWithDataClause() throws Exception{
        try(PreparedStatement ps=conn.prepareStatement(String.format("create table %s.t10 as select t1.a, t2.c from %s t1, %s t2 where t1.b = t2.b",spliceSchemaWatcher.schemaName,baseTable,rightTable))){
            int numRows=ps.executeUpdate();
            Assert.assertEquals("It claims to have updated rows!",10,numRows);

        }
        try(Statement s= conn.createStatement()){
            try(ResultSet rs=s.executeQuery("select * from "+spliceSchemaWatcher.schemaName+".t10")){
                int count=0;
                while(rs.next()){
                    int first=rs.getInt(1);
                    int second=rs.getInt(2);
                    Assert.assertEquals("Incorrect row: ("+first+","+second+")",first,second);
                    count++;
                }
                Assert.assertEquals("Incorrect row count",10,count);
            }
        }
    }

    // DB-1170
    @Test
    public void testCreateTableWithNoDataDerivedDecimal() throws Exception{
        try(Statement s = conn.createStatement()){
            s.executeUpdate(String.format("create table %s.t5 as select (d * (1 - d)) as volume from %s with no data",spliceSchemaWatcher.schemaName,decimalTable));
            s.executeUpdate("drop table "+spliceSchemaWatcher.schemaName+".t5");
        }
    }

    private void testCommentsInCTAS(String commentString, String viewRef) throws Exception {
        String depsalTable = "depsal";
        String depsalTableRef = spliceSchemaWatcher.schemaName + "." + depsalTable;
        String depsalTableDef = format("create table %s as " +
                "select dept, salary, ssn from %s %s where dept < 3 with data", depsalTableRef, viewRef, commentString);
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(spliceSchemaWatcher.schemaName, depsalTable);

        methodWatcher.executeUpdate(depsalTableDef);
        String sqlText = format("select * from %s order by dept, salary", depsalTableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "DEPT |SALARY |   SSN   |\n" +
                "------------------------\n" +
                "  1  | 75000 |11199222 |\n" +
                "  2  | 52000 |22200555 |\n" +
                "  2  | 78000 |88844777 |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        try(PreparedStatement ps=methodWatcher.prepareStatement(String.format("drop table %s",depsalTableRef))){
            ps.executeUpdate();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void createTableWithViewJoins() throws Exception {
        // DB-4170: create table with data didn't work with more than one join (the view defn is executed)
        String nameTable = "names";
        String nameTableRef = spliceSchemaWatcher.schemaName + "." + nameTable;
        String nameTableDef = "(id int, fname varchar(10), lname varchar(10))";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(spliceSchemaWatcher.schemaName, nameTable);

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(format("create table %s %s", nameTableRef, nameTableDef))
                .withInsert(format("insert into %s values (?,?,?)", nameTableRef))
                .withRows(rows(
                        row(20, "Joe", "Blow"),
                        row(70, "Fred", "Ziffle"),
                        row(60, "Floyd", "Jones"),
                        row(40, "Janice", "Jones")
                ))
                .create();

        String empTable = "emptab";
        String empTableRef = spliceSchemaWatcher.schemaName + "." + empTable;
        String empTableDef = "(empnum int, dept int, salary int)";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(spliceSchemaWatcher.schemaName, empTable);

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(format("create table %s %s", empTableRef, empTableDef))
                .withInsert(format("insert into %s values (?,?,?)", empTableRef))
                .withRows(rows(
                        row(20, 1, 75000),
                        row(70, 3, 76000),
                        row(60, 2, 78000),
                        row(40, 2, 52000)
                ))
                .create();

        String ssnTable = "ssn";
        String ssnTableRef = spliceSchemaWatcher.schemaName + "." + ssnTable;
        String ssnTableDef = "(id int, ssn int)";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(spliceSchemaWatcher.schemaName, ssnTable);

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate(format("create table %s %s", ssnTableRef, ssnTableDef))
                .withInsert(format("insert into %s values (?,?)", ssnTableRef))
                .withRows(rows(
                        row(20, 11199222),
                        row(70, 33366777),
                        row(60, 88844777),
                        row(40, 22200555)
                ))
                .create();

        String viewName = "empsal";
        String viewRef = spliceSchemaWatcher.schemaName + "." + viewName;
        String viewDef = format("create view %s as select distinct " +
                        "A.ID, A.LNAME, A.FNAME, " +
                        "B.DEPT, B.SALARY, " +
                        "C.SSN " +
                        "FROM %s A " +
                        "LEFT OUTER JOIN %s B ON A.ID = B.EMPNUM " +
                        "LEFT OUTER JOIN %s C ON A.ID = C.ID ", viewRef, nameTableRef, empTableRef,
                ssnTableRef);

        methodWatcher.execute(viewDef);

        testCommentsInCTAS("--comment hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello hello\n", viewRef);
        testCommentsInCTAS("-- WITH and AS in a comment \n", viewRef);
        testCommentsInCTAS("-- /* C-style comment inside a '--' comment */ \n", viewRef);
        testCommentsInCTAS("/* A C-style comment */", viewRef);
        testCommentsInCTAS("/* WITH and AS in a C-style comment */", viewRef);
        testCommentsInCTAS("/* A C-style comment followed be newline */\n", viewRef);
        testCommentsInCTAS("/* A C-style comment \n spanning \n multiple \n lines */", viewRef);
        testCommentsInCTAS("/* -- dash dash comment inside a C-style comment */", viewRef);
        testCommentsInCTAS("/* --splice-properties useSpark=true  hint inside comment should be OK */", viewRef);
        // A splice properties hint should work
        testCommentsInCTAS("--splice-properties useSpark=true\n", viewRef);
        // Mixed case hints should work too.
        testCommentsInCTAS("--spLice-prOperties useSpark=true\n", viewRef);
    }
}

