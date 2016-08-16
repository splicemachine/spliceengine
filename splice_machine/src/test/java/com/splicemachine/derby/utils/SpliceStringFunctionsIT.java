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

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.scalatest.tools.PresentAlertProvided;

import static com.splicemachine.triggers.Trigger_Exec_Stored_Proc_IT.methodWatcher;

/**
 * Tests associated with the String functions as defined in
 * {@link SpliceStringFunctions}.
 * 
 * @author Walt Koetke
 */
public class SpliceStringFunctionsIT {

    private static final TestConnectionPool connPool = new TestConnectionPool();
    private static final String SCHEMA_NAME = SpliceStringFunctionsIT.class.getSimpleName().toUpperCase();

    private static RuledConnection conn = new RuledConnection(connPool,true);

    private static TableRule b = new TableRule(conn,"B","(a int, b varchar(30), c varchar(30), d int)");
    private static TableRule c = new TableRule(conn,"C","(a varchar(30),b varchar(30))");
    private static TableRule d = new TableRule(conn,"D","(a varchar(30),b varchar(30),c varchar(30))");

    @ClassRule public static TestRule rules = RuleChain.outerRule(conn)
            .around(new SchemaRule(conn,SCHEMA_NAME))
            .around(b)
            .around(c)
            .around(d);

    @BeforeClass
    public static void loadData() throws Exception{

        try( PreparedStatement ps = conn.prepareStatement( "insert into "+b+"(a,b,c,d) values (?,?,?,?)") ){

            // Each of the following inserted rows represents an individual test,
            // including expected result (column 'd'), for less test code in the
            // testInstr methods.

            ps.setInt(1,1);
            ps.setObject(2,"Fred Flintstone");
            ps.setString(3,"Flint");
            ps.setInt(4,6);
            ps.execute();

            ps.setInt(1,2);
            ps.setObject(2,"Fred Flintstone");
            ps.setString(3,"Fred");
            ps.setInt(4,1);
            ps.execute();

            ps.setInt(1,3);
            ps.setObject(2,"Fred Flintstone");
            ps.setString(3," F");
            ps.setInt(4,5);
            ps.execute();

            ps.setInt(1,4);
            ps.setObject(2,"Fred Flintstone");
            ps.setString(3,"Flintstone");
            ps.setInt(4,6);
            ps.execute();

            ps.setInt(1,5);
            ps.setObject(2,"Fred Flintstone");
            ps.setString(3,"stoner");
            ps.setInt(4,0);
            ps.execute();

            ps.setInt(1,6);
            ps.setObject(2,"Barney Rubble");
            ps.setString(3,"Wilma");
            ps.setInt(4,0);
            ps.execute();

            ps.setInt(1,7);
            ps.setObject(2,"Bam Bam");
            ps.setString(3,"Bam Bam Bam");
            ps.setInt(4,0);
            ps.execute();

            ps.setInt(1,8);
            ps.setObject(2,"Betty");
            ps.setString(3,"");
            ps.setInt(4,0);
            ps.execute();

            ps.setInt(1,9);
            ps.setObject(2,null);
            ps.setString(3,null);
            ps.setInt(4,0);
            ps.execute();
        }

        try(PreparedStatement ps = conn.prepareStatement("insert into "+c+"(a,b) values (?,?)")){
            ps.setObject(1,"freDdy kruGeR");
            ps.setString(2,"Freddy Kruger");
            ps.execute();
        }

        try(PreparedStatement ps = conn.prepareStatement("insert into "+d+"(a,b,c) values (?,?,?)")){
            ps.setString(1,"AAA");
            ps.setString(2,"BBB");
            ps.setString(3,"AAABBB");
            ps.execute();

            ps.setString(1,"");
            ps.setString(2,"BBB");
            ps.setString(3,"BBB");
            ps.execute();

            ps.setString(1,"AAA");
            ps.setString(2,"");
            ps.setString(3,"AAA");
            ps.execute();

            ps.setString(1,"");
            ps.setString(2,"");
            ps.setString(3,"");
            ps.execute();

            ps.setString(1,null);
            ps.setString(2,"BBB");
            ps.setString(3,null);
            ps.execute();

            ps.setString(1,"AAA");
            ps.setString(2,null);
            ps.setString(3,null);
            ps.execute();
        }
    }


    //    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

//    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Table for INSTR testing.
//    private static final SpliceTableWatcher tableWatcherB = new SpliceTableWatcher(
//    	"B", schemaWatcher.schemaName, "(a int, b varchar(30), c varchar(30), d int)");

    // Table for INITCAP testing.
//    private static final SpliceTableWatcher tableWatcherC = new SpliceTableWatcher(
//    	"C", schemaWatcher.schemaName, "(a varchar(30), b varchar(30))");

    // Table for CONCAT testing.
//    private static final SpliceTableWatcher tableWatcherD = new SpliceTableWatcher(
//    	"D", schemaWatcher.schemaName, "(a varchar(30), b varchar(30), c varchar(30))");

//    @ClassRule
//    public static TestRule chain = RuleChain.outerRule(classWatcher)
//            .around(schemaWatcher)
//            .around(tableWatcherB)
//            .around(tableWatcherC)
//            .around(tableWatcherD)
//            .around(new SpliceDataWatcher() {
//                @Override
//                protected void starting(Description description) {
//
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    } finally {
//                        classWatcher.closeAll();
//                    }
//                }
//            });
//    @Rule
//    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testInstrFunction() throws Exception {
        int count;
	    String sCell1;
	    String sCell2;

        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("SELECT INSTR(b, c), d from "+b)){
                count=0;
                while(rs.next()){
                    sCell1=rs.getString(1);
                    sCell2=rs.getString(2);
                    Assert.assertEquals("Wrong result value",sCell1,sCell2);
                    count++;
                }
                Assert.assertEquals("Incorrect row count",9,count);
            }
        }
    }

    @Test
    public void testInitcapFunction() throws Exception {
	    String sCell1;
	    String sCell2;

        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("SELECT INITCAP(a), b from "+c)){
                while(rs.next()){
                    sCell1=rs.getString(1);
                    sCell2=rs.getString(2);
                    Assert.assertEquals("Wrong result value",sCell2,sCell1);
                }
            }
        }
    }

    @Test
    public void testConcatFunction() throws Exception {
	    String sCell1;
	    String sCell2;

        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("SELECT CONCAT(a, b), c from "+d)){
                while(rs.next()){
                    sCell1=rs.getString(1);
                    sCell2=rs.getString(2);
                    Assert.assertEquals("Wrong result value",sCell2,sCell1);
                }
            }
        }
    }
}
