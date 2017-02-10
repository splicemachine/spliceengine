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

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

/**
 * Tests associated with the String functions as defined in
 * {@link SpliceStringFunctions}.
 * 
 * @author Walt Koetke
 */
public class SpliceStringFunctionsIT {
	
    private static final String CLASS_NAME = SpliceStringFunctionsIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Table for INSTR testing.
    private static final SpliceTableWatcher tableWatcherB = new SpliceTableWatcher(
    	"B", schemaWatcher.schemaName, "(a int, b varchar(30), c varchar(30), d int)");

    // Table for INITCAP testing.
    private static final SpliceTableWatcher tableWatcherC = new SpliceTableWatcher(
    	"C", schemaWatcher.schemaName, "(a varchar(30), b varchar(30))");

    // Table for CONCAT testing.
    private static final SpliceTableWatcher tableWatcherD = new SpliceTableWatcher(
    	"D", schemaWatcher.schemaName, "(a varchar(30), b varchar(30), c varchar(30))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherB)
            .around(tableWatcherC)
            .around(tableWatcherD)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps;
                        
                        // Each of the following inserted rows represents an individual test,
                        // including expected result (column 'd'), for less test code in the
                        // testInstr methods.

                        ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherB + " (a, b, c, d) values (?, ?, ?, ?)");
                        ps.setInt   (1, 1); // row 1
                        ps.setObject(2, "Fred Flintstone");
                        ps.setString(3, "Flint");
                        ps.setInt   (4, 6);
                        ps.execute();

                        ps.setInt   (1, 2);
                        ps.setObject(2, "Fred Flintstone");
                        ps.setString(3, "Fred");
                        ps.setInt   (4, 1);
                        ps.execute();

                        ps.setInt   (1, 3);
                        ps.setObject(2, "Fred Flintstone");
                        ps.setString(3, " F");
                        ps.setInt   (4, 5);
                        ps.execute();

                        ps.setInt   (1, 4);
                        ps.setObject(2, "Fred Flintstone");
                        ps.setString(3, "Flintstone");
                        ps.setInt   (4, 6);
                        ps.execute();

                        ps.setInt   (1, 5);
                        ps.setObject(2, "Fred Flintstone");
                        ps.setString(3, "stoner");
                        ps.setInt   (4, 0);
                        ps.execute();

                        ps.setInt   (1, 6);
                        ps.setObject(2, "Barney Rubble");
                        ps.setString(3, "Wilma");
                        ps.setInt   (4, 0);
                        ps.execute();

                        ps.setInt   (1, 7);
                        ps.setObject(2, "Bam Bam");
                        ps.setString(3, "Bam Bam Bam");
                        ps.setInt   (4, 0);
                        ps.execute();

                        ps.setInt   (1, 8);
                        ps.setObject(2, "Betty");
                        ps.setString(3, "");
                        ps.setInt   (4, 0);
                        ps.execute();

                        ps.setInt   (1, 9);
                        ps.setObject(2, null);
                        ps.setString(3, null);
                        ps.setInt   (4, 0);
                        ps.execute();

                        ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherC + " (a, b) values (?, ?)");
                        ps.setObject(1, "freDdy kruGeR");
                        ps.setString(2, "Freddy Kruger");
                        ps.execute();

                        ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherD + " (a, b, c) values (?, ?, ?)");
                        ps.setString(1, "AAA");
                        ps.setString(2, "BBB");
                        ps.setString(3, "AAABBB");
                        ps.execute();

                        ps.setString(1, "");
                        ps.setString(2, "BBB");
                        ps.setString(3, "BBB");
                        ps.execute();

                        ps.setString(1, "AAA");
                        ps.setString(2, "");
                        ps.setString(3, "AAA");
                        ps.execute();

                        ps.setString(1, "");
                        ps.setString(2, "");
                        ps.setString(3, "");
                        ps.execute();

                        ps.setString(1, null);
                        ps.setString(2, "BBB");
                        ps.setString(3, null);
                        ps.execute();

                        ps.setString(1, "AAA");
                        ps.setString(2, null);
                        ps.setString(3, null);
                        ps.execute();

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        classWatcher.closeAll();
                    }
                }
            });
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testInstrFunction() throws Exception {
        int count = 0;
	    String sCell1 = null;
	    String sCell2 = null;
	    ResultSet rs;
	    
	    rs = methodWatcher.executeQuery("SELECT INSTR(b, c), d from " + tableWatcherB);
	    count = 0;
	    while (rs.next()) {
    		sCell1 = rs.getString(1);
            sCell2 = rs.getString(2);
            Assert.assertEquals("Wrong result value", sCell1, sCell2);
            count++;
	    }
	    Assert.assertEquals("Incorrect row count", 9, count);
    }

    @Test
    public void testInitcapFunction() throws Exception {
	    String sCell1 = null;
	    String sCell2 = null;
	    ResultSet rs;
	    
	    rs = methodWatcher.executeQuery("SELECT INITCAP(a), b from " + tableWatcherC);
	    while (rs.next()) {
    		sCell1 = rs.getString(1);
            sCell2 = rs.getString(2);
            Assert.assertEquals("Wrong result value", sCell2, sCell1);
	    }
    }

    @Test
    public void testConcatFunction() throws Exception {
	    String sCell1 = null;
	    String sCell2 = null;
	    ResultSet rs;
	    
	    rs = methodWatcher.executeQuery("SELECT CONCAT(a, b), c from " + tableWatcherD);
	    while (rs.next()) {
    		sCell1 = rs.getString(1);
            sCell2 = rs.getString(2);
            Assert.assertEquals("Wrong result value", sCell2, sCell1);
	    }
    }
}
