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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.*;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @author Scott Fines
 *         Created on: 10/23/13
 */
public class TernaryOperationIT {

    private static final String CLASS_NAME = TernaryOperationIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static final SpliceTableWatcher tableWatcher = new SpliceTableWatcher(
        "A",schemaWatcher.schemaName,"(c varchar(20),a int, b int)");

    // Table for 'replace' testing.
    private static final SpliceTableWatcher tableWatcherB = new SpliceTableWatcher(
    	"B", schemaWatcher.schemaName, "(a int, b varchar(30), c varchar(30), d varchar(30), e varchar(30))");

    // Table for 'timestampadd' and 'timestampdiff testing.
    private static final SpliceTableWatcher tableWatcherC = new SpliceTableWatcher(
            "C", schemaWatcher.schemaName, "(a int, b timestamp, c timestamp)");

    // Table for 'trim' testing.
    private static final SpliceTableWatcher tableWatcherD = new SpliceTableWatcher(
            "D", schemaWatcher.schemaName, "(a int, b varchar(30), c varchar(30), d varchar(30))");


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcher)
            .around(tableWatcherB)
            .around(tableWatcherC)
            .around(tableWatcherD)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps = classWatcher.prepareStatement("insert into "+ tableWatcher+" (a, c) values (?, ?)");
                        ps.setInt(1,1);
                        ps.setString(2,"this world is crazy");
                        ps.execute();

                        ps.setInt(1,2);
                        ps.setString(2,"nada");
                        ps.execute();

                        ps = classWatcher.prepareStatement("insert into "+ tableWatcher +"(a, b) values (?, ?)");
                        ps.setInt(1,3);
                        ps.setInt(2,3);
                        ps.execute();

                        ps = classWatcher.prepareStatement("insert into "+ tableWatcher +"(a,b,c) values (?, ?, ?)");
                        ps.setInt(1,4);
                        ps.setInt(2, 2);
                        ps.setString(3,"hello world");
                        ps.execute();


                        // Each of the following inserted rows represent individual test units,
                        // including expected result (column 'e'), for less test code
                        // testReplaceFunction method.
                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherB + " (a, b, c, d, e) values (?, ?, ?, ?, ?)");
                        ps.setInt   (1,1); // row 1
                        ps.setObject(2,"having fun yet?");
                        ps.setString(3,"fun");
                        ps.setString(4,"craziness");
                        ps.setString(5,"having craziness yet?");
                        ps.execute();

                        ps.setInt   (1,2); // row 2
                        ps.setObject(2,"one yep two yep");
                        ps.setString(3,"yep");
                        ps.setString(4,"nope");
                        ps.setString(5,"one nope two nope");
                        ps.execute();

                        ps.setInt   (1,3); // row 3
                        ps.setObject(2,"one yep two yep");
                        ps.setString(3,"yep");
                        ps.setString(4,"");
                        ps.setString(5,"one  two ");
                        ps.execute();

                        ps.setInt   (1,4); // row 4
                        ps.setObject(2,"yep yep yep yep");
                        ps.setString(3,"gonk");
                        ps.setString(4,"nope");
                        ps.setString(5,"yep yep yep yep");
                        ps.execute();

                        ps.setInt   (1,5); // row 5
                        ps.setObject(2,null);
                        ps.setString(3,null);
                        ps.setString(4,"not null");
                        ps.setString(5,null);
                        ps.execute();

                        ps = classWatcher.prepareStatement("insert into " + tableWatcherC + " (a, b, c) values (?, ?, ?)");
                        ps.setInt(1, 1);
                        ps.setTimestamp(2, Timestamp.valueOf("2013-03-23 09:45:00"));
                        ps.setTimestamp(3, Timestamp.valueOf("2015-03-23 09:45:00"));
                        ps.execute();

                        ps = classWatcher.prepareStatement("insert into " + tableWatcherD + "(a, b, c, d) values (?, ?, ?, ?)");
                        ps.setInt   (1,1);
                        ps.setString(2,"welcome hello world");
                        ps.setString(3,"   hello world  ");
                        ps.setString(4,"hello");
                        ps.execute();


                    }catch (Exception e) {
                        throw new RuntimeException(e);
                    }finally{
                        classWatcher.closeAll();
                    }
                }
            });
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testLocateWorks() throws Exception {
        ResultSet rs =  methodWatcher.executeQuery("select locate('crazy',c) from " + tableWatcher + " where a < 4");
        int count = 0;
        boolean nullFound=false;
        boolean zeroFound=false;
        boolean positiveFound=false;
        while(rs.next()){
            int val = rs.getInt(1);
            if(rs.wasNull()){
                Assert.assertFalse("Null was seen twice!",nullFound);
                nullFound = true;
            } else if(val <0){
                Assert.fail("Should not have seen a negative number");
            }else if(val >0){
                Assert.assertFalse("Positive was seen twice!",positiveFound);
                positiveFound=true;
            }else{
                Assert.assertFalse("Zero was seen twice!",zeroFound);
                zeroFound=true;
            }
            count++;
        }
        Assert.assertTrue("Null not found!",nullFound);
        Assert.assertTrue("Negative not found!",zeroFound);
        Assert.assertTrue("Positive not found!",positiveFound);

        Assert.assertEquals("Incorrect row count returned!",3,count);
    }

    @Test
    public void testReplaceFunction() throws Exception {
        int count = 0;
	    String sCell1 = null;
	    String sCell2 = null;
	    ResultSet rs;

	    rs = methodWatcher.executeQuery("select replace(b, c, d), e from " + tableWatcherB);
	    count = 0;
	    while (rs.next()) {
    		sCell1 = rs.getString(1);
            sCell2 = rs.getString(2);
            Assert.assertEquals("Wrong result value", sCell1, sCell2);
            count++;
	    }
	    Assert.assertEquals("Incorrect row count", 5, count);
    }

    @Test
    public void testTRIM() throws Exception {
        ResultSet rs;
        int iCell = 0;
        String sql = "select a.a from a where exists ( select 1 from d where a.c = trim(d.c))";
        rs = methodWatcher.executeQuery(sql);
        while (rs.next()) {
            iCell = rs.getInt(1);
        }
        Assert.assertEquals("Wrong trim result", 4, iCell);
    }

    @Test
    public void testTRIMWithAlias() throws Exception {
        ResultSet rs;
        int iCell = 0;
        String sql = "select count(*) from ( select trim(b) from b ) tab";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        iCell = rs.getInt(1);
        Assert.assertEquals("Wrong trim result", 5, iCell);

        sql = "select count(*) from ( select trim (b.b) from b ) tab";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        iCell = rs.getInt(1);
        Assert.assertEquals("Wrong trim result", 5, iCell);

        sql = "select count(*) from ( select trim (b 'a' from c) from a b ) tab";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        iCell = rs.getInt(1);
        Assert.assertEquals("Wrong trim result", 4, iCell);

        sql = "select count(*) from ( select trim (b.c) from a b ) tab";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        iCell = rs.getInt(1);
        Assert.assertEquals("Wrong trim result", 4, iCell);

        sql = "select count(*) from ( select trim (l.c) from a l ) tab";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        iCell = rs.getInt(1);
        Assert.assertEquals("Wrong trim result", 4, iCell);

        sql = "select count(*) from ( select trim (t.c) from a t ) tab";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        iCell = rs.getInt(1);
        Assert.assertEquals("Wrong trim result", 4, iCell);

        sql = "select count(*) from ( select trim (\"leading\".c) from a as \"leading\" ) tab";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        iCell = rs.getInt(1);
        Assert.assertEquals("Wrong trim result", 4, iCell);
    }
    @Test
    public void testTRIMWithAliasNegative() throws Exception {
        try {
            String sql = "select trim (leading.c) from a as leading";
            methodWatcher.executeQuery(sql);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals(SQLState.LANG_SYNTAX_ERROR, e.getSQLState());
        }
    }

    @Test
    public void testSubString() throws Exception {
        ResultSet rs;
        String sCell1 = null;
        String sCell2 = null;
        String sql = "select d,substr(b,9,5) from d";
        rs = methodWatcher.executeQuery(sql);
        while (rs.next()) {
            sCell1 = rs.getString(1);
            sCell2 = rs.getString(2);
        }
        Assert.assertEquals("Wrong substr result", sCell1, sCell2 );
    }

    @Test
    public void testRight() throws Exception {
        ResultSet rs;

        methodWatcher.execute("create table ta(name VARCHAR(128), cut INT)");
        methodWatcher.execute(
                "insert into ta(name, cut) " +
                "values ('hello world', 5), ('hey dude', 20), (null, 10), ('cute string', null)"
        );

        String sql = "select right(name, cut) from ta";
        rs = methodWatcher.executeQuery(sql);

        methodWatcher.execute("drop table ta");

        List<String> expected = new ArrayList<>(2);
        expected.add("world");
        expected.add("hey dude");
        expected.add(null);
        expected.add(null);

        for (String s: expected) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(s, rs.getString(1));
        }
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testLeft() throws Exception {
        ResultSet rs;

        methodWatcher.execute("create table ta(name VARCHAR(128), cut INT)");
        methodWatcher.execute(
            "insert into ta(name, cut) " +
                "values ('hello world', 5), ('hey dude', 20), (null, 10), ('cute string', null)"
        );

        String sql = "select left(name, cut) from ta";
        rs = methodWatcher.executeQuery(sql);

        methodWatcher.execute("drop table ta");

        List<String> expected = new ArrayList<>(2);
        expected.add("hello");
        expected.add("hey dude");
        expected.add(null);
        expected.add(null);

        for (String s: expected) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(s, rs.getString(1));
        }
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testTIMESTAMPADD() throws Exception {
        ResultSet rs;
        Timestamp iCell1 = null;
        Timestamp iCell2 = null;
        String sql = "select c,TIMESTAMPADD(SQL_TSI_YEAR,2,b) from " + tableWatcherC;
        rs = methodWatcher.executeQuery(sql);
        while (rs.next()) {
            iCell1 = rs.getTimestamp(1);
            iCell2 = rs.getTimestamp(2);
        }
        Assert.assertEquals("Wrong timestampadd result", iCell1, iCell2);
    }

    @Test
    public void testTIMESTAMPDIFF() throws Exception {
        ResultSet rs;
        int iCell = 0;
        String sql = "select TIMESTAMPDIFF(SQL_TSI_YEAR,b,c) from " + tableWatcherC;
        rs = methodWatcher.executeQuery(sql);
        while (rs.next()) {
            iCell = rs.getInt(1);
        }
        Assert.assertEquals("Wrong timestampdiff result", 2, iCell);
    }

    @Test
    public void testSTRIP() throws Exception {
        ResultSet rs;
        int iCell = 0;
        String sql = "select a.a from a where exists ( select 1 from d where a.c = strip(d.c))";

        rs = methodWatcher.executeQuery(sql);
        while (rs.next()) {
            iCell = rs.getInt(1);
        }
        Assert.assertEquals("Wrong strip result", 4, iCell);

        sql  = "values strip('   space case   ')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","space case",rs.getString(1));

        sql  = "values strip('   space case   ',both)";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","space case",rs.getString(1));

        sql  = "values strip('   space case   ',leading)";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","space case   ",rs.getString(1));

        sql  = "values strip('   space case   ',trailing)";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","   space case",rs.getString(1));

        sql  = "values strip('aabbccaa',,'a')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","bbcc",rs.getString(1));

        sql  = "values strip('aabbccaa',both,'a')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","bbcc",rs.getString(1));

        sql  = "values strip('aabbccaa',leading,'a')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","bbccaa",rs.getString(1));

        sql  = "values strip('aabbccaa',trailing,'a')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","aabbcc",rs.getString(1));

        sql  = "values strip('12.7000',,'0')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","12.7",rs.getString(1));

        sql  = "values strip('0012.700',,'0')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","12.7",rs.getString(1));

        sql  = "values strip(' bb  ',)";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","bb",rs.getString(1));

    }

    @Test
    public void testSTRIPShortName() throws Exception {
        ResultSet rs;
        String sql  = "values strip('   space case   ',b)";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","space case",rs.getString(1));

        sql  = "values strip('   space case   ',l)";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","space case   ",rs.getString(1));

        sql  = "values strip('   space case   ',t)";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","   space case",rs.getString(1));

        sql  = "values strip('aabbccaa',b,'a')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","bbcc",rs.getString(1));

        sql  = "values strip('aabbccaa',l,'a')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","bbccaa",rs.getString(1));

        sql  = "values strip('aabbccaa',t,'a')";
        rs = methodWatcher.executeQuery(sql);
        rs.next();
        Assert.assertEquals("Wrong strip result","aabbcc",rs.getString(1));
    }

    @Test
    public void testSTRIPNegative() throws Exception {
        try {
            String sql = "values strip(1,2)";
            methodWatcher.executeQuery(sql);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals(SQLState.LANG_SYNTAX_ERROR, e.getSQLState());
        }

        try {
            String sql = "values strip('abc','bc')";
            methodWatcher.executeQuery(sql);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertTrue("Unexpected error code: " + e.getSQLState(),  SQLState.LANG_SYNTAX_ERROR.startsWith(e.getSQLState()));
        }
    }
}

