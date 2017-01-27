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

package com.splicemachine.derby.impl.sql.execute.operations;

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

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcher)
            .around(tableWatcherB)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps = classWatcher.prepareStatement("insert into "+ tableWatcher+" (c) values (?)");
                        ps.setString(1,"this world is crazy");
                        ps.execute();

                        ps.setString(1,"nada");
                        ps.execute();

                        ps = classWatcher.prepareStatement("insert into "+ tableWatcher +"(b) values (?)");
                        ps.setInt(1,3);
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
        ResultSet rs =  methodWatcher.executeQuery("select locate('crazy',c) from " + tableWatcher);
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
}

