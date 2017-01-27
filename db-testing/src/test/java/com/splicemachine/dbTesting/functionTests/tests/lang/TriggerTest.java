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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.Test;

import com.splicemachine.dbTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest;
import com.splicemachine.dbTesting.functionTests.util.streams.ByteAlphabet;
import com.splicemachine.dbTesting.functionTests.util.streams.CharAlphabet;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetReader;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetStream;
import com.splicemachine.dbTesting.functionTests.util.streams.ReadOnceByteArrayInputStream;
import com.splicemachine.dbTesting.functionTests.util.streams.StringReaderWithLength;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.XML;

/**
 * Test triggers.
 *
 */
public class TriggerTest extends BaseJDBCTestCase {
   
    /**
     * Thread local that a trigger can access to
     * allow recording information about the firing.
     */
    private static ThreadLocal TRIGGER_INFO = new ThreadLocal();

    public TriggerTest(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }
    
    /**
     * Run only in embedded as TRIGGERs are server side logic.
     * Also the use of a ThreadLocal to check state requires
     * embedded. 
     */
    public static Test suite() {
        return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(TriggerTest.class));
        
    }
    
    protected void initializeConnection(Connection conn) throws SQLException
    {
        conn.setAutoCommit(false);
    }
    
    protected void setUp() throws Exception
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE PROCEDURE TRIGGER_LOG_INFO(" +
                "O VARCHAR(255)) " +
                "NO SQL PARAMETER STYLE JAVA LANGUAGE JAVA " +
                "EXTERNAL NAME " +
                "'" + getClass().getName() + ".logTriggerInfo'");
        s.close();

    }
    
    protected void tearDown() throws Exception
    {
        TRIGGER_INFO.set(null);
        JDBC.dropSchema(getConnection().getMetaData(),
                getTestConfiguration().getUserName());

        super.tearDown();
    }
    
    /**
     * Altering the column length should regenerate the trigger
     * action plan which is saved in SYSSTATEMENTS. DERBY-4874
     * 
     * @throws SQLException 
     * 
     */
    public void testAlerColumnLength() throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE TestAlterTable( " +
        		"element_id INTEGER NOT NULL, "+
        		"altered_id VARCHAR(30) NOT NULL, "+
        		"counter SMALLINT NOT NULL DEFAULT 0, "+
        		"timets TIMESTAMP NOT NULL)");
        s.executeUpdate("CREATE TRIGGER mytrig "+
        		"AFTER UPDATE ON TestAlterTable "+
        		"REFERENCING NEW AS newt OLD AS oldt "+
        		"FOR EACH ROW MODE DB2SQL "+
        		"  UPDATE TestAlterTable set "+
        		"  TestAlterTable.counter = CASE WHEN "+
        		"  (oldt.counter < 32767) THEN (oldt.counter + 1) ELSE 1 END "+
        		"  WHERE ((newt.counter is null) or "+
        		"  (oldt.counter = newt.counter)) " +
        		"  AND newt.element_id = TestAlterTable.element_id "+
        		"  AND newt.altered_id = TestAlterTable.altered_id");
        s.executeUpdate("ALTER TABLE TestAlterTable ALTER altered_id "+
        		"SET DATA TYPE VARCHAR(64)");
        s.executeUpdate("insert into TestAlterTable values (99, "+
        		"'012345678901234567890123456789001234567890',"+
        		"1,CURRENT_TIMESTAMP)");

        ResultSet rs = s.executeQuery("SELECT element_id, counter "+
        		"FROM TestAlterTable");
        JDBC.assertFullResultSet(rs, 
        		new String[][] {{"99", "1"}});
        
        s.executeUpdate("update TestAlterTable "+
        		"set timets = CURRENT_TIMESTAMP "+
        		"where ELEMENT_ID = 99");
        rs = s.executeQuery("SELECT element_id, counter "+
        		"FROM TestAlterTable");
        JDBC.assertFullResultSet(rs, 
        		new String[][] {{"99", "2"}});

        s.executeUpdate("DROP TABLE TestAlterTable");
    }
    
    /**
     * Test the firing order of triggers. Should be:
     * 
     * Before operations
     * after operations
     * 
     * For multiple triggers within the same group (before or after)
     * firing order is determined by create order.
     * @throws SQLException 
     *
     */
    public void testFiringOrder() throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE T(ID INT)");
        
        int triggerCount = createRandomTriggers()[0];
        
        List info = new ArrayList();
        TRIGGER_INFO.set(info);
        
        // Check ordering with a single row.
        s.execute("INSERT INTO T VALUES 1");
        commit();
        int fireCount = assertFiringOrder("INSERT", 1);
        info.clear();
        
        s.execute("UPDATE T SET ID = 2");
        commit();
        fireCount += assertFiringOrder("UPDATE", 1);
        info.clear();
        
        s.execute("DELETE FROM T");
        commit();
        fireCount += assertFiringOrder("DELETE", 1);
        info.clear();
           
        assertEquals("All triggers fired?", triggerCount, fireCount);

        // and now with multiple rows
        s.execute("INSERT INTO T VALUES 1,2,3");
        commit();
        fireCount = assertFiringOrder("INSERT", 3);
        info.clear();
        
        s.execute("UPDATE T SET ID = 2");
        commit();
        fireCount += assertFiringOrder("UPDATE", 3);
        info.clear();
        
        s.execute("DELETE FROM T");
        commit();
        fireCount += assertFiringOrder("DELETE", 3);
        info.clear();
        
        // cannot assume row triggers were created so can only
        // say that at least all the triggers were fired.
        assertTrue("Sufficient triggers fired?", fireCount >= triggerCount);
        
        
        // and then with no rows
        assertTableRowCount("T", 0);
        s.execute("INSERT INTO T SELECT ID FROM T");
        commit();
        fireCount = assertFiringOrder("INSERT", 0);
        info.clear();
        
        s.execute("UPDATE T SET ID = 2");
        commit();
        fireCount += assertFiringOrder("UPDATE", 0);
        info.clear();
        
        s.execute("DELETE FROM T");
        commit();
        fireCount += assertFiringOrder("DELETE", 0);
        info.clear();
        
        // can't assert anthing about fireCount, could be all row triggers.
            
        s.close();

    }
    
    private int[] createRandomTriggers() throws SQLException
    {
        Statement s = createStatement();
        
        int beforeCount = 0;
        int afterCount = 0;
        
        Random r = new Random();
        // Randomly generate a number of triggers.
        // There are 12 types (B/A, I/U/D, R,S)
        // so pick enough triggers to get some
        // distribution across all 12.
        int triggerCount = r.nextInt(45) + 45;
        for (int i = 0; i < triggerCount; i++)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("CREATE TRIGGER TR");
            sb.append(i);
            sb.append(" ");
            
            String before;
            if (r.nextInt(2) == 0) {
                before = "NO CASCADE BEFORE";
                beforeCount++;
            } else {
                before = "AFTER";
                afterCount++;
            }
            sb.append(before);
            sb.append(" ");
            
            int type = r.nextInt(3);
            String iud;
            if (type == 0)
                iud = "INSERT";
            else if (type == 1)
                iud = "UPDATE";
            else
                iud = "DELETE";
            sb.append(iud);
            
            sb.append(" ON T FOR EACH ");
            
            String row;
            if (r.nextInt(2) == 0)
                row = "ROW";
            else
                row = "STATEMENT";
            sb.append(row);
            sb.append(" ");
            
            sb.append("CALL TRIGGER_LOG_INFO('");
            sb.append(i);
            sb.append(",");
            sb.append(before);
            sb.append(",");
            sb.append(iud);
            sb.append(",");
            sb.append(row);
            sb.append("')");

            s.execute(sb.toString());
        }
        commit();
        s.close();
        return new int[] {triggerCount, beforeCount, afterCount};
    }
    
    
    /**
     * Test that a order of firing is before triggers,
     * constraint checking and after triggers.
     * @throws SQLException 
     *
     */
    public void testFiringConstraintOrder() throws SQLException
    {
        Statement s = createStatement();
        s.execute("CREATE TABLE T (I INT PRIMARY KEY," +
                "U INT NOT NULL UNIQUE, C INT CHECK (C < 20))");
        s.execute("INSERT INTO T VALUES(1,5,10)");
        s.execute("INSERT INTO T VALUES(11,19,3)");
        s.execute("CREATE TABLE TCHILD (I INT, FOREIGN KEY (I) REFERENCES T)");
        s.execute("INSERT INTO TCHILD VALUES 1");
        commit();
        
        int beforeCount = createRandomTriggers()[1];
        
        List info = new ArrayList();
        TRIGGER_INFO.set(info);
        
        // constraint violation on primary key
        assertStatementError("23505", s, "INSERT INTO T VALUES (1,6,10)");
        assertFiringOrder("INSERT", 1, true);        
        info.clear();
        assertStatementError("23505", s, "UPDATE T SET I=1 WHERE I = 11");
        assertFiringOrder("UPDATE", 1, true);        
        info.clear();
        rollback();
        
        // constraint violation on unique key
        assertStatementError("23505", s, "INSERT INTO T VALUES (2,5,10)");
        assertFiringOrder("INSERT", 1, true);        
        info.clear();
        assertStatementError("23505", s, "UPDATE T SET U=5 WHERE I = 11");
        assertFiringOrder("UPDATE", 1, true);        
        info.clear();
        rollback();
        
        // check constraint
        assertStatementError("23513", s, "INSERT INTO T VALUES (2,6,22)");
        assertFiringOrder("INSERT", 1, true);        
        info.clear();
        assertStatementError("23513", s, "UPDATE T SET C=C+40 WHERE I = 11");
        assertFiringOrder("UPDATE", 1, true);        
        info.clear();
        rollback();
        
        // Foreign key constraint
        assertStatementError("23503", s, "DELETE FROM T WHERE I = 1");
        assertFiringOrder("DELETE", 1, true);        
        
        s.close();
        commit();
    }
    
    /**
     * Look at the ordered information in the thread local
     * and ensure it reflects correct sequenceing of
     * triggers created in testFiringOrder.
     * @param iud
     * @return the number of triggers checked
     */
    private int assertFiringOrder(String iud, int modifiedRowCount)
    {
        return assertFiringOrder(iud, modifiedRowCount, false);
    }
    private int assertFiringOrder(String iud, int modifiedRowCount,
            boolean noAfter)
    {
        List fires = (List) TRIGGER_INFO.get();
        
        int lastOrder = -1;
        String lastBefore = null;
        for (Iterator i = fires.iterator(); i.hasNext(); )
        {
            String info = i.next().toString();
            StringTokenizer st = new StringTokenizer(info, ",");
            assertEquals(4, st.countTokens());
            st.hasMoreTokens();
            int order = Integer.valueOf(st.nextToken()).intValue();
            st.hasMoreTokens();
            String before = st.nextToken();
            st.hasMoreTokens();
            String fiud = st.nextToken();
            st.hasMoreTokens();
            String row = st.nextToken();
            
            assertEquals("Incorrect trigger firing:"+info, iud, fiud);
            if (modifiedRowCount == 0)
               assertEquals("Row trigger firing on no rows",
                       "STATEMENT", row);
            if (noAfter)
                assertFalse("No AFTER triggers", "AFTER".equals(before));
            
            // First trigger.
            if (lastOrder == -1)
            {
                lastOrder = order;
                lastBefore = before;
                continue;
            }
            
            // Same trigger as last one.
            if (lastBefore.equals(before))
            {
                // for multiple rows the trigger can match the previous one.
                boolean orderOk =
                    modifiedRowCount > 1 ? (order >= lastOrder) :
                        (order > lastOrder);
                assertTrue("matching triggers need to be fired in order creation:"
                        +info, orderOk);
                lastOrder = order;
                continue;
            }
            
            
            // switching from a before trigger to an after trigger.
            assertEquals("BEFORE before AFTER:"+info,
                    "NO CASCADE BEFORE", lastBefore);
            assertEquals("then AFTER:"+info,
                    "AFTER", before);
            
            lastBefore = before;
            lastOrder = order;
            
        }
        
        return fires.size();
    }
    
    /**
     * Record the trigger information in the thread local.
     * Called as a SQL procedure.
     * @param info trigger information
      */
    public static void logTriggerInfo(String info)
    {
        ((List) TRIGGER_INFO.get()).add(info);  
    }

    /** 
     * Test for DERBY-3718 NPE when a trigger is fired
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testNPEinTriggerFire() throws SQLException
    {
        Statement s = createStatement();
        
    	String sql = " CREATE TABLE TRADE(ID INT PRIMARY KEY GENERATED "+
    	"BY DEFAULT AS IDENTITY (START WITH 1000), BUYID INT NOT NULL," +
    	"QTY FLOAT(2) NOT NULL)";
        s.executeUpdate(sql);

        sql = "CREATE TABLE TOTAL(BUYID INT NOT NULL, TOTALQTY FLOAT(2) NOT NULL)";
        s.executeUpdate(sql);
        
        sql = "CREATE TRIGGER TRADE_INSERT AFTER INSERT ON TRADE REFERENCING "+ 
        "NEW AS NEWROW FOR EACH ROW MODE DB2SQL UPDATE TOTAL SET TOTALQTY "+
        "= NEWROW.QTY WHERE BUYID = NEWROW.BUYID"; 
        s.executeUpdate(sql);
        
        s.executeUpdate("INSERT INTO TOTAL VALUES (1, 0)");
        //Before DERBY-3718 was fixed, following would cause NPE in 10.4 and 
        //trunk. This happened because starting 10.4, rather than saving the
        //TypeId of the DataTypeDescriptor (in writeExternal method), we rely
        //on reconstructing TypeId (in readExternal) by using the Types.xxx 
        //information(DERBY-2917 revision r619995). This approach does not
        //work for internal datatype REF, because we use Types.OTHER for REF
        //datatypes. Types.OTHER is not enough to know that the type to be 
        //constructed is REF. 
        //To get around the problem, for reconstructing TypeId, we will
        //use the type name rather than Types.xxx. Since we have the correct
        //type name for internal datatype REF, we can successfully reconstruct
        //REF datatype. 
        s.executeUpdate("INSERT INTO TRADE VALUES(1, 1, 10)");
        commit();      
    }

    //DERBY-1482
    public void testReadRequiredColumnsOnlyFromTriggerTable() throws SQLException, IOException {
        Statement s = createStatement();

        s.executeUpdate("CREATE TABLE table1 (c11 int, c12 int, c13 int, c14 int, c15 int)");
        s.executeUpdate("INSERT INTO table1 VALUES(1,2,3,4,5)");
        s.executeUpdate("CREATE TABLE table2 (c21 int, c22 int, c23 int, c24 int, c25 int)");
        s.executeUpdate("INSERT INTO table2 VALUES(2,2,3,-1,5)");
        //Notice that following trigger references columns from trigger table
        //randomly ie columns c12 and c14 are not the 1st 2 columns in trigger
        //table but they will be the first 2 columns in the resultset generated
        //for the trigger. The internal code generation for CreateTriggerNode
        //has been written to handle this mismatch of column numbering
        //between trigger table and trigger runtime resultset
        s.executeUpdate("CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 " +
        		" REFERENCING OLD AS oldt NEW AS newt" +
        		" FOR EACH ROW UPDATE table2 SET c24=oldt.c14");
        commit();      

        s.executeUpdate("update table1 set c12 = -9 where c11=1");
        ResultSet rs = s.executeQuery("SELECT * FROM table2");
        String[][] result = {
                {"2","2","3","4","5"},
            };
        JDBC.assertFullResultSet(rs, result);
            
        //couple negative test
        //give invalid column in trigger column
        String triggerStmt = "CREATE TRIGGER tr1 AFTER UPDATE OF c12xxx ON table1 " +
        		" REFERENCING OLD AS oldt NEW AS newt" +
        		" FOR EACH ROW UPDATE table2 SET c24=oldt.c14";
        assertStatementError("42X14", s, triggerStmt);
        
        //give invalid column in trigger action
        triggerStmt = "CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 " +
		" REFERENCING OLD AS oldt NEW AS newt" +
		" FOR EACH ROW UPDATE table2 SET c24=oldt.c14xxx";
        assertStatementError("42X04", s, triggerStmt);
        
        //Test case involving before and after values of LOB columns
        s.executeUpdate("create table derby1482_lob1 (str1 Varchar(80), " +
        		"c_lob CLOB(50M))");
        s.executeUpdate("create table derby1482_lob1_log(oldvalue CLOB(50M), " +
        		"newvalue  CLOB(50M), " +
        		"chng_time timestamp default current_timestamp)");
        s.executeUpdate("create trigger tr1_derby1482_lob1 after update of c_lob " +
        		"on derby1482_lob1 REFERENCING OLD AS old NEW AS new " +
        		"FOR EACH ROW MODE DB2SQL "+
        		"insert into derby1482_lob1_log(oldvalue, newvalue) values " +
        		"(old.c_lob, new.c_lob)");
        s.executeUpdate("INSERT INTO derby1482_lob1 VALUES ('1',null)");
        s.executeUpdate("update derby1482_lob1 set c_lob = null");
        rs = s.executeQuery("SELECT oldvalue, newvalue FROM derby1482_lob1_log");
        result = new String [][] {{null, null}};
        JDBC.assertFullResultSet(rs, result);
        
        //Test case involving a trigger which updates the trigger table
        s.executeUpdate("create table derby1482_selfUpdate (i int, j int)");
        s.executeUpdate("insert into derby1482_selfUpdate values (1,10)");
        s.executeUpdate("create trigger tr_derby1482_selfUpdate " + 
        		"after update of i on derby1482_selfUpdate " +
        		"referencing old as old for each row " +
        		"update derby1482_selfUpdate set j = old.j+1");
        s.executeUpdate("update derby1482_selfUpdate set i=i+1");
        rs = s.executeQuery("SELECT * FROM derby1482_selfUpdate");
        result = new String [][] {{"2","11"}};
        JDBC.assertFullResultSet(rs, result);
        
        //Test case where trigger definition uses REFERENCING clause but does
        //not use those columns in trigger action
        s.executeUpdate("create table t1_noTriggerActionColumn "+
        		"(id int, status smallint)");
        s.executeUpdate("insert into t1_noTriggerActionColumn values(11,1)");
        s.executeUpdate("create table t2_noTriggerActionColumn " +
        		"(id int, updates int default 0)");
        s.executeUpdate("insert into t2_noTriggerActionColumn values(1,1)");
        s.executeUpdate("create trigger tr1_noTriggerActionColumn " +
        		"after update of status on t1_noTriggerActionColumn " +
        		"referencing new as n_row for each row " +
        		"update t2_noTriggerActionColumn set " +
        		"updates = updates + 1 " +
        		"where t2_noTriggerActionColumn.id = 1");
        s.executeUpdate("update t1_noTriggerActionColumn set status=-1");
        rs =s.executeQuery("SELECT * FROM t2_noTriggerActionColumn");
        result = new String [][] {{"1","2"}};
        JDBC.assertFullResultSet(rs, result);
    }
    
    public void testDERBY5121() throws SQLException
    {
        Statement s = createStatement();

        s.executeUpdate("CREATE TABLE T1 (A1 int)");
        s.executeUpdate("CREATE TABLE T2 (B1 int, B2 int, B3 int)");
        s.executeUpdate("CREATE TRIGGER t2UpdateTrigger "+
        		"after UPDATE of b1 on t2 " +
        		"referencing new row as nr for each ROW " +
        		"insert into t1 values ( nr.b3 ) ");
        s.executeUpdate("INSERT INTO T2 VALUES(0,0,0)");
        s.executeUpdate("update t2 set b1 = 100 , b2 = 1");
        ResultSet rs =s.executeQuery("SELECT * FROM T1");
        JDBC.assertFullResultSet(rs, new String[][] {{"0"}});

        s.executeUpdate("CREATE TABLE T3 (A1 int)");
        s.executeUpdate("CREATE TABLE T4 (B1 int, B2 int, B3 int)");
        s.executeUpdate("CREATE TRIGGER t4UpdateTrigger "+
        		"after UPDATE of b1 on t4 " +
        		"referencing new table as nt for each STATEMENT " +
        		"insert into t3 select b3 from nt");
        s.executeUpdate("INSERT INTO T4 VALUES(0,0,0)");
        s.executeUpdate("update t4 set b1 = 100 , b2 = 1");
        rs =s.executeQuery("SELECT * FROM T3");
        JDBC.assertFullResultSet(rs, new String[][] {{"0"}});

    }
    
    /** 
     * Test for DERBY-3238 trigger fails with IOException if triggering table has large lob.
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testClobInTriggerTable() throws SQLException, IOException
    {
    	testClobInTriggerTable(1024);
        testClobInTriggerTable(16384);
         
        testClobInTriggerTable(1024 *32 -1);
        testClobInTriggerTable(1024 *32);
        testClobInTriggerTable(1024 *32+1);
        testClobInTriggerTable(1024 *64 -1);
        testClobInTriggerTable(1024 *64);
        testClobInTriggerTable(1024 *64+1);
        
    }
   
    /**
     * Create a table with after update trigger on non-lob column.
     * Insert clob of size clobSize into table and perform update
     * on str1 column to fire trigger. Helper method called from 
     * testClobInTriggerTable
     * @param clobSize size of clob to test
     * @throws SQLException
     * @throws IOException
     */
    private void testClobInTriggerTable(int clobSize) throws SQLException, IOException {
        // Alphabet used when inserting a CLOB.
        CharAlphabet a1 = CharAlphabet.singleChar('a');
        // Alphabet used when updating a CLOB.
        CharAlphabet a2 = CharAlphabet.singleChar('b');
    	
    	// --- add a clob
    	String trig = " create trigger t_lob1 after update of str1 on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.str1, new.str1)";

        Statement s = createStatement();
        
        s.executeUpdate("create table LOB1 (str1 Varchar(80), c_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue varchar(80), newvalue varchar(80), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        PreparedStatement ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, clobSize +"");

        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(clobSize, a1), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        s.executeUpdate("update LOB1 set str1 = str1 || ' '");
       
        
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");

        // now referencing the lob column
        trig = " create trigger t_lob1 after update of c_lob on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.c_lob, new.c_lob)";

        s.executeUpdate("create table LOB1 (str1 Varchar(80), c_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue CLOB(50M), newvalue  CLOB(50M), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, clobSize +"");

        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(clobSize, a1), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        ps = prepareStatement("update LOB1 set c_lob = ?");
        ps.setCharacterStream(1,
                new LoopingAlphabetReader(clobSize, a2), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();        

        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
        
        //      now referencing the lob column twice
        trig = " create trigger t_lob1 after update of c_lob on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue, oldvalue_again, newvalue_again) values (old.c_lob, new.c_lob, old.c_lob, new.c_lob)";

        s.executeUpdate("create table LOB1 (str1 Varchar(80), c_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue CLOB(50M), newvalue  CLOB(50M), oldvalue_again CLOB(50M), newvalue_again CLOB(50M), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, clobSize +"");

        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(clobSize, a1), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        ps = prepareStatement("update LOB1 set c_lob = ?");
        ps.setCharacterStream(1,
                new LoopingAlphabetReader(clobSize, a2), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();
        
        // check log table.
        ResultSet rs = s.executeQuery("SELECT * from t_lob1_log");
        rs.next();
               
        assertEquals(new LoopingAlphabetReader(clobSize, a1),
                     rs.getCharacterStream(1));

        assertEquals(new LoopingAlphabetReader(clobSize, a2),
                     rs.getCharacterStream(2));

        assertEquals(new LoopingAlphabetReader(clobSize, a1),
                     rs.getCharacterStream(3));

        assertEquals(new LoopingAlphabetReader(clobSize, a2),
                     rs.getCharacterStream(4));

        rs.close();
        
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
        
       }

    /** 
     * Test for DERBY-3238 trigger fails with IOException if triggering table has large lob.
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testBlobInTriggerTable() throws SQLException, IOException
    {        
    	testBlobInTriggerTable(1024);
        testBlobInTriggerTable(16384);
         
        testBlobInTriggerTable(1024 *32 -1);
        testBlobInTriggerTable(1024 *32);
        testBlobInTriggerTable(1024 *32+1);
        testBlobInTriggerTable(1024 *64 -1);
        testBlobInTriggerTable(1024 *64);
        testBlobInTriggerTable(1024 *64+1);
        testBlobInTriggerTable(1024 *1024* 7);
    }
    
    
    /**
     * Create a table with after update trigger on non-lob column.
     * Insert two blobs of size blobSize into table and perform update
     * on str1 column to fire trigger. Helper method called from 
     * testBlobInTriggerTable
     * 
     * @param blobSize  size of blob to test.
     * @throws SQLException
     * @throws IOException
     */
    private  void testBlobInTriggerTable(int blobSize) throws SQLException, IOException {
        // Alphabet used when inserting a BLOB.
        ByteAlphabet a1 = ByteAlphabet.singleByte((byte) 8);
        // Alphabet used when updating a BLOB.
        ByteAlphabet a2 = ByteAlphabet.singleByte((byte) 9);

        String trig = " create trigger t_lob1 after update of str1 on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.str1, new.str1)";

        Statement s = createStatement();
        
        s.executeUpdate("create table LOB1 (str1 Varchar(80), b_lob BLOB(50M), b_lob2 BLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue varchar(80), newvalue varchar(80), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

    	// --- add a blob
        PreparedStatement ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?, ?)");
        
        ps.setString(1, blobSize +"");

        // - set the value of the input parameter to the input stream
        // use a couple blobs so we are sure it works with multiple lobs
        ps.setBinaryStream(2,
                new LoopingAlphabetStream(blobSize, a1), blobSize);
        ps.setBinaryStream(3,
                new LoopingAlphabetStream(blobSize, a1), blobSize);
        ps.execute();
        closeStatement(ps);
        
        commit();
        // Now executing update to fire trigger
        s.executeUpdate("update LOB1 set str1 = str1 || ' '");
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
   
        // now referencing the lob column
        trig = " create trigger t_lob1 after update of b_lob on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.b_lob, new.b_lob)";

        s.executeUpdate("create table LOB1 (str1 Varchar(80), b_lob BLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue BLOB(50M), newvalue  BLOB(50M), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, blobSize +"");


        // - set the value of the input parameter to the input stream
        ps.setBinaryStream(2,
                new LoopingAlphabetStream(blobSize, a1), blobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        ps = prepareStatement("update LOB1 set b_lob = ?");
        ps.setBinaryStream(1,
                new LoopingAlphabetStream(blobSize, a2), blobSize);
        ps.execute();
        closeStatement(ps);
        commit();        

        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
        
        //      now referencing the lob column twice
        trig = " create trigger t_lob1 after update of b_lob on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue, oldvalue_again, newvalue_again) values (old.b_lob, new.b_lob, old.b_lob, new.b_lob)";

        s.executeUpdate("create table LOB1 (str1 Varchar(80), b_lob BLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue BLOB(50M), newvalue  BLOB(50M), oldvalue_again BLOB(50M), newvalue_again BLOB(50M), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, blobSize +"");


        // - set the value of the input parameter to the input stream
        ps.setBinaryStream(2,
                new LoopingAlphabetStream(blobSize, a1), blobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        ps = prepareStatement("update LOB1 set b_lob = ?");
        ps.setBinaryStream(1,
                new LoopingAlphabetStream(blobSize, a2), blobSize);
        ps.execute();
        closeStatement(ps);
        commit();
        
        // check log table.
        ResultSet rs = s.executeQuery("SELECT * from t_lob1_log");
        rs.next();

        assertEquals(new LoopingAlphabetStream(blobSize, a1),
                     rs.getBinaryStream(1));

        assertEquals(new LoopingAlphabetStream(blobSize, a2),
                     rs.getBinaryStream(2));

        assertEquals(new LoopingAlphabetStream(blobSize, a1),
                     rs.getBinaryStream(3));

        assertEquals(new LoopingAlphabetStream(blobSize, a2),
                     rs.getBinaryStream(4));
        
        rs.close();

        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");

    }

    /* 
     * Test an update trigger on a Clob column
     * 
     */
    public void testUpdateTriggerOnClobColumn() throws SQLException, IOException
    {
        // Alphabet used when inserting a CLOB.
        CharAlphabet a1 = CharAlphabet.singleChar('a');
        // Alphabet used when updating a CLOB.
        CharAlphabet a2 = CharAlphabet.singleChar('b');

    	Connection conn = getConnection();
    	Statement s = createStatement();
    	String trig = " create trigger t_lob1 after update of str1 on lob1 ";
    	trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
    	trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.str1, new.str1)";
    	s.executeUpdate("create table LOB1 (str1 Varchar(80), C_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue varchar(80), newvalue varchar(80), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        conn.commit();
        PreparedStatement ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        int clobSize = 1024*64+1;
        ps.setString(1, clobSize +"");


        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(clobSize, a1), clobSize);
        ps.execute();
        conn.commit();


        PreparedStatement ps2 = prepareStatement("update LOB1 set c_lob = ? where str1 = '" + clobSize + "'");
        ps2.setCharacterStream(1,
                new LoopingAlphabetReader(clobSize, a2), clobSize);
        ps2.executeUpdate();
        conn.commit();
        // 	--- reading the clob make sure it was updated
        ResultSet rs = s.executeQuery("SELECT * FROM LOB1 where str1 = '" + clobSize + "'");
        rs.next();
        
        assertEquals(new LoopingAlphabetReader(clobSize, a2),
                     rs.getCharacterStream(2));
        rs.close();
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
        
	  
    }

    /**
     * Test that the action statement of a trigger
     * can work with all datatypes.
     * @throws SQLException
     * @throws IOException 
     */
    public void testTypesInActionStatement() throws SQLException, IOException
    {
        List types = DatabaseMetaDataTest.getSQLTypes(getConnection());
        
        if (!XML.classpathMeetsXMLReqs())
            types.remove("XML");
        
        // JSR 169 doesn't support DECIMAL in triggers.
        if (!JDBC.vmSupportsJDBC3())
        {           
            for (Iterator i = types.iterator(); i.hasNext(); )
            {
                String type = i.next().toString();
                if (type.startsWith("DECIMAL") || type.startsWith("NUMERIC"))
                    i.remove();
            }
        }
        
        for (Iterator i = types.iterator(); i.hasNext(); )
        {
            actionTypeTest(i.next().toString());
        }
    }
    
    /**
     * Test that the action statement of a trigger
     * can work with a specific datatype.
     * @param type SQL type to be tested
     * @throws SQLException 
     * @throws IOException 
     */
    private void actionTypeTest(String type) throws SQLException, IOException
    {
        println("actionTypeTest:"+type);
        Statement s = createStatement(); 
        
        actionTypesSetup(type);
        
        actionTypesInsertTest(type); 
        
        actionTypesUpdateTest(type);
        
        actionTypesDeleteTest(type);
               
        s.executeUpdate("DROP TABLE T_MAIN");
        s.executeUpdate("DROP TABLE T_ACTION_ROW");
        s.executeUpdate("DROP TABLE T_ACTION_STATEMENT");
        s.close();
        
        commit();
        

    }
   
    /**
     * Setup the tables and triggers for a single type for actionTypeTest
     */
    private void actionTypesSetup(String type) throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE T_MAIN(" +
                "ID INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
                "V " + type + " )");
        s.executeUpdate("CREATE TABLE T_ACTION_ROW(ID INT, A CHAR(1), " +
                "V1 " + type + ", V2 " + type + " )"); 
        s.executeUpdate("CREATE TABLE T_ACTION_STATEMENT(ID INT, A CHAR(1), " +
                "V1 " + type + ", V2 " + type + " )"); 
       
        // ON INSERT copy the typed value V into the action table.
        // Use V twice to ensure there are no issues with values
        // that can be streamed.
        // Two identical actions,  per row and per statement.
        s.executeUpdate("CREATE TRIGGER AIR " +
                "AFTER INSERT ON T_MAIN " +
                "REFERENCING NEW AS N " +
                "FOR EACH ROW " +      
                "INSERT INTO T_ACTION_ROW(A, V1, ID, V2) VALUES ('I', N.V, N.ID, N.V)");
        
        s.executeUpdate("CREATE TRIGGER AIS " +
                "AFTER INSERT ON T_MAIN " +
                "REFERENCING NEW TABLE AS N " +
                "FOR EACH STATEMENT " +      
                "INSERT INTO T_ACTION_STATEMENT(A, V1, ID, V2) " +
                "SELECT 'I', V, ID, V FROM N");
        
        // ON update copy the old and new value into the action table.
        // Two identical actions,  per row and per statement.
        s.executeUpdate("CREATE TRIGGER AUR " +
                "AFTER UPDATE OF V ON T_MAIN " +
                "REFERENCING NEW AS N OLD AS O " +
                "FOR EACH ROW " +      
                "INSERT INTO T_ACTION_ROW(A, V1, ID, V2) VALUES ('U', N.V, N.ID, O.V)");
        
        s.executeUpdate("CREATE TRIGGER AUS " +
                "AFTER UPDATE OF V ON T_MAIN " +
                "REFERENCING NEW TABLE AS N OLD TABLE AS O " +
                "FOR EACH STATEMENT " +      
                "INSERT INTO T_ACTION_STATEMENT(A, V1, ID, V2) " +
                "SELECT 'U', N.V, N.ID, O.V FROM N,O WHERE O.ID = N.ID");
        
        // ON DELETE copy the old value into the action table.
        // Two identical actions,  per row and per statement.
        s.executeUpdate("CREATE TRIGGER ADR " +
                "AFTER DELETE ON T_MAIN " +
                "REFERENCING OLD AS O " +
                "FOR EACH ROW " +      
                "INSERT INTO T_ACTION_ROW(A, V1, ID, V2) VALUES ('D', O.V, O.ID, O.V)");
        
        s.executeUpdate("CREATE TRIGGER ADS " +
                "AFTER DELETE ON T_MAIN " +
                "REFERENCING OLD TABLE AS O " +
                "FOR EACH STATEMENT " +      
                "INSERT INTO T_ACTION_STATEMENT(A, V1, ID, V2) " +
                "SELECT 'D', O.V, O.ID, O.V FROM O");        
        

        s.close();
        commit();
    }
    
    /**
     * Execute three insert statements.
     * NULL as the value for the type
     * one row insert with random value for the type
     * three row insert with random values for the type
     * 
     * Check that the data in the action table matches the main
     * table (see the after insert trigger definitions).
     * @param type
     * @throws SQLException
     * @throws IOException
     * 
     */
    private void actionTypesInsertTest(String type)
        throws SQLException, IOException
    {  
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO T_MAIN(V) VALUES NULL");
        s.close();
        actionTypesCompareMainToAction(1, type);

        int jdbcType = DatabaseMetaDataTest.getJDBCType(type);
        int precision = DatabaseMetaDataTest.getPrecision(jdbcType, type);

        // BUG DERBY-2349 - remove this check & return to see the issue.
        if (jdbcType == Types.BLOB)
            return; 
        
        Random r = new Random();
        
        String ins1 = "INSERT INTO T_MAIN(V) VALUES (?)";
        String ins3 = "INSERT INTO T_MAIN(V) VALUES (?), (?), (?)";
        
        // Can't directly insert into XML columns from JDBC.
        if (jdbcType == JDBC.SQLXML)
        {
            ins1 = "INSERT INTO T_MAIN(V) VALUES " +
                    "XMLPARSE (DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)";
            ins3 = "INSERT INTO T_MAIN(V) VALUES " +
                    "XMLPARSE (DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)," +
                    "XMLPARSE (DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)," +
                    "XMLPARSE (DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)";
        }
        
        PreparedStatement ps;
        ps = prepareStatement(ins1);
        setRandomValue(r, ps, 1, jdbcType, precision);
        ps.executeUpdate();
        ps.close();

        actionTypesCompareMainToAction(2, type);

        ps = prepareStatement(ins3);
        setRandomValue(r, ps, 1, jdbcType, precision);
        setRandomValue(r, ps, 2, jdbcType, precision);
        setRandomValue(r, ps, 3, jdbcType, precision);
        ps.executeUpdate();
        ps.close();

        actionTypesCompareMainToAction(5, type);    
    }
    
    /**
     * Test updates of the specified types in the action statement.
     * @param type
     * @throws SQLException
     * @throws IOException
     */
    private void actionTypesUpdateTest(String type)
        throws SQLException, IOException
    {
        int jdbcType = DatabaseMetaDataTest.getJDBCType(type);
        int precision = DatabaseMetaDataTest.getPrecision(jdbcType, type);

        // BUG DERBY-2349 - need insert case to work first
        if (jdbcType == Types.BLOB)
            return;
        
        Statement s = createStatement();
        s.executeUpdate("UPDATE T_MAIN SET V = NULL WHERE ID = 2");
        s.close();
        commit();
        actionTypesCompareMainToActionForUpdate(type, 2);

        Random r = new Random();
        
        PreparedStatement ps = prepareStatement(
            (jdbcType == JDBC.SQLXML
                ? "UPDATE T_MAIN SET V = " +
                  "XMLPARSE(DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)"
                : "UPDATE T_MAIN SET V = ?")
            + " WHERE ID >= ? AND ID <= ?");
        
        // Single row update of row 3
        setRandomValue(r, ps, 1, jdbcType, precision);
        ps.setInt(2, 3);
        ps.setInt(3, 3);
        assertUpdateCount(ps, 1);
        commit();
        actionTypesCompareMainToActionForUpdate(type, 3);
        
        // Bug DERBY-2358 - skip multi-row updates for streaming input.
        switch (jdbcType) {
        case Types.BLOB:
        case Types.CLOB:
        case Types.LONGVARBINARY:
        case Types.LONGVARCHAR:
            ps.close();
            return;
        }
        
        // multi-row update of 4,5
        setRandomValue(r, ps, 1, jdbcType, precision);
        ps.setInt(2, 4);
        ps.setInt(3, 5);
        assertUpdateCount(ps, 2);
        commit();
        actionTypesCompareMainToActionForUpdate(type, 4);
        actionTypesCompareMainToActionForUpdate(type, 5);

        ps.close();
        
    }
    
    /**
     * Compare the values for an update trigger.
     * @param type
     * @param id
     * @throws SQLException
     * @throws IOException
     */
    private void actionTypesCompareMainToActionForUpdate(String type,
            int id) throws SQLException, IOException {
        
        String sqlMain = "SELECT M.V, R.V1 FROM T_MAIN M, T_ACTION_ROW R " +
            "WHERE M.ID = ? AND R.A = 'I' AND M.ID = R.ID";
        String sqlRow = "SELECT V1, V2 FROM T_ACTION_ROW " +
            "WHERE A = 'U' AND ID = ?";
        String sqlStmt = "SELECT V1, V2 FROM T_ACTION_STATEMENT " +
            "WHERE A = 'U' AND ID = ?";
        
        if ("XML".equals(type)) {
            // XMLSERIALIZE(V AS CLOB)
            sqlMain = "SELECT XMLSERIALIZE(M.V AS CLOB), " +
                "XMLSERIALIZE(R.V1 AS CLOB) FROM T_MAIN M, T_ACTION_ROW R " +
                "WHERE M.ID = ? AND R.A = 'I' AND M.ID = R.ID";
            sqlRow = "SELECT XMLSERIALIZE(V1 AS CLOB), " +
                "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_ROW " +
                "WHERE A = 'U' AND ID = ?";
            sqlStmt = "SELECT XMLSERIALIZE(V1 AS CLOB), " +
                "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_STATEMENT " +
                "WHERE A = 'U' AND ID = ?";
        }
        
        // Get the new value from main and old from the action table 
        PreparedStatement psMain = prepareStatement(sqlMain);
              
        // new (V1) & old (V2) value as copied by the trigger
        PreparedStatement psActionRow = prepareStatement(sqlRow);
        PreparedStatement psActionStmt = prepareStatement(sqlStmt);
        
        psMain.setInt(1, id);
        psActionRow.setInt(1, id);
        psActionStmt.setInt(1, id);
        
        JDBC.assertSameContents(psMain.executeQuery(),
                psActionRow.executeQuery());
        JDBC.assertSameContents(psMain.executeQuery(),
                psActionStmt.executeQuery());
         
        psMain.close();
        psActionRow.close();
        psActionStmt.close();
        
        commit();
    }
    
    /**
     * Test deletes with the specified types in the action statement.
     * @param type
     * @throws SQLException
     * @throws IOException
     */
    private void actionTypesDeleteTest(String type)
        throws SQLException, IOException
    {
        int jdbcType = DatabaseMetaDataTest.getJDBCType(type);
        int precision = DatabaseMetaDataTest.getPrecision(jdbcType, type);

        // BUG DERBY-2349 - need insert case to work first
        if (jdbcType == Types.BLOB)
            return;
        
        Statement s = createStatement();
        // Single row delete
        assertUpdateCount(s, 1, "DELETE FROM T_MAIN WHERE ID = 3");
        commit();
        
        // multi-row delete
        assertUpdateCount(s, 4, "DELETE FROM T_MAIN");
        commit();
        
        s.close();
    }
    
    
    /**
     * Compare the contents of the main table to the action table.
     * See the trigger defintions for details.
     * @param actionCount
     * @param type
     * @throws SQLException
     * @throws IOException
     */
    private void actionTypesCompareMainToAction(int actionCount,
            String type) throws SQLException, IOException {
        
        Statement s1 = createStatement();
        Statement s2 = createStatement();
        
        String sqlMain = "SELECT ID, V, V FROM T_MAIN ORDER BY 1";
        String sqlActionRow = "SELECT ID, V1, V2 FROM T_ACTION_ROW ORDER BY 1";
        String sqlActionStatement = "SELECT ID, V1, V2 FROM T_ACTION_STATEMENT ORDER BY 1";
        
        // Derby does not (yet) allow a XML column in select list 
        if ("XML".equals(type)) {
            sqlMain = "SELECT ID, XMLSERIALIZE(V AS CLOB), " +
                    "XMLSERIALIZE(V AS CLOB) FROM T_MAIN ORDER BY 1";
            sqlActionRow = "SELECT ID, XMLSERIALIZE(V1 AS CLOB), " +
                    "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_ROW ORDER BY 1";
            sqlActionStatement = "SELECT ID, XMLSERIALIZE(V1 AS CLOB), " +
                "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_STATEMENT ORDER BY 1";
        }
        
        ResultSet rsMain = s1.executeQuery(sqlMain);
        ResultSet rsAction = s2.executeQuery(sqlActionRow);        
        JDBC.assertSameContents(rsMain, rsAction);
        
        rsMain = s1.executeQuery(sqlMain);
        rsAction = s2.executeQuery(sqlActionStatement);        
        JDBC.assertSameContents(rsMain, rsAction);
        
        
        assertTableRowCount("T_ACTION_ROW", actionCount);
        assertTableRowCount("T_ACTION_STATEMENT", actionCount);
        
        s1.close();
        s2.close();
    }
    
    public static void setRandomValue(Random r,
            PreparedStatement ps, int column, int jdbcType, int precision)
    throws SQLException, IOException
    {
        Object val = getRandomValue(r, jdbcType, precision);
        if (val instanceof StringReaderWithLength) {
            StringReaderWithLength rd = (StringReaderWithLength) val;
            ps.setCharacterStream(column, rd, rd.getLength());
        } else if (val instanceof InputStream) {
            InputStream in = (InputStream) val;
            ps.setBinaryStream(column, in, in.available());
        } else
            ps.setObject(column, val, jdbcType);
    }
    
    /**
     * Generate a random object (never null) for
     * a given JDBC type. Object is suitable for
     * PreparedStatement.setObject() either
     * with or without passing in jdbcType to setObject.
     * <BR>
     * For character types a String object or a
     * StringReaderWithLength is returned.
     * <BR>
     * For binary types a byte[] or an instance of InputStream
     * is returned. If an inputstream is returned then it can
     * only be read once and in.available() returns the total
     * number of bytes available to read.
     * For BLOB types a random value is returned up to
     * either the passed in precision or 256k. This is
     * to provide a general purpose value that can be
     * more than a page.
     * <P>
     * Caller should check the return type using instanceof
     * and use setCharacterStream() for Reader objects and
     * setBinaryStream for InputStreams.
     * (work in progress)
     * @throws IOException 
     */
    public static Object getRandomValue(Random r, int jdbcType, 
            int precision) throws IOException
    {
        switch (jdbcType)
        {
        case Types.SMALLINT:
            return new Integer((short) r.nextInt());
        case Types.INTEGER:
            return new Integer(r.nextInt());
            
        case Types.BIGINT:
            return new Long(r.nextLong());
            
        case Types.FLOAT:
        case Types.REAL:
            return new Float(r.nextFloat());
            
        case Types.DOUBLE:
            return new Double(r.nextDouble());

        case Types.DATE:
            long d = r.nextLong();
            if (d < 0)
                d = -d;
            d = d / (24L * 60L * 60L * 1000L);
            d = d % (4000L * 365L); // limit year to a reasonable value.
            d = d * (24L * 60L * 60L * 1000L);
            return new Date(d);
            
        case Types.TIME:
            long t = r.nextLong();
            if (t < 0)
                t = -t;
             return new Time(t % (24L * 60L * 60L * 1000L));
             
        case Types.TIMESTAMP:
            // limit year to a reasonable value
            long ts = r.nextLong();
            if (ts < 0)
                ts = -ts;
            ts = ts % (4000L * 365L * 24L * 60L * 60L * 1000L);
            return new Timestamp(ts);
            
        case Types.VARCHAR:
        case Types.CHAR:
            return randomString(r, r.nextInt(precision + 1));
            
        case Types.LONGVARCHAR:
            return new StringReaderWithLength(
                    randomString(r, r.nextInt(32700 + 1)));
            
        case Types.CLOB:
            if (precision > 256*1024)
                precision = 256*1024;
            return new StringReaderWithLength(
                    randomString(r, r.nextInt(precision)));

        case Types.BINARY:
        case Types.VARBINARY:
            return randomBinary(r, r.nextInt(precision + 1));

        case Types.LONGVARBINARY:
            return new ReadOnceByteArrayInputStream(
                    randomBinary(r, r.nextInt(32701)));
            
        case Types.BLOB:
            if (precision > 256*1024)
                precision = 256*1024;
            return new ReadOnceByteArrayInputStream(
                    randomBinary(r, r.nextInt(precision)));
            
        case JDBC.SQLXML:
            // Not random yet, but was blocked by DEBRY-2350
            // so just didn't put effort into generating 
            // a random size XML document.
            return new StringReaderWithLength("<a><b>text</b></a>");
            
             
       }
            
        // fail("unexpected JDBC Type " + jdbcType);
        return null;
    }
    
    private static byte[] randomBinary(Random r, int len)
    {
        byte[] bb = new byte[len];
        for (int i = 0; i < bb.length; i++)
            bb[i] = (byte) r.nextInt();
        return bb;
 
    }
    private static String randomString(Random r, int len)
    {
        char[] cb = new char[len];
        for (int i = 0; i < cb.length; i++)
            cb[i] = (char) r.nextInt(Character.MAX_VALUE);
              
        return new String(cb);
                
    }


    /**
     * Test that a nested loop join that accesses the 
     * TriggerOldTransitionRowsVTI can reopen the ResultSet properly 
     * when it re-executes.
     * @throws SQLException
     */
    public void testDerby4095OldTriggerRows() throws SQLException {
        Statement s = createStatement();
        
        s.executeUpdate("CREATE TABLE SPLICE.TAB (I INT)");
        s.executeUpdate("CREATE TABLE SPLICE.LOG (I INT, NAME VARCHAR(30), DELTIME TIMESTAMP)");
        s.executeUpdate("CREATE TABLE SPLICE.NAMES(ID INT, NAME VARCHAR(30))");

        
        s.executeUpdate("CREATE TRIGGER  SPLICE.MYTRIG AFTER DELETE ON SPLICE.TAB REFERENCING OLD_TABLE AS OLDROWS FOR EACH STATEMENT INSERT INTO SPLICE.LOG(i,name,deltime) SELECT OLDROWS.I, NAMES.NAME, CURRENT_TIMESTAMP FROM --DERBY-PROPERTIES joinOrder=FIXED\n NAMES, OLDROWS --DERBY-PROPERTIES joinStrategy = NESTEDLOOP\n WHERE (OLDROWS.i = NAMES.ID) AND (1 = 1)");
        
        s.executeUpdate("insert into SPLICE.tab values(1)");
        s.executeUpdate("insert into SPLICE.tab values(2)");
        s.executeUpdate("insert into SPLICE.tab values(3)");

        s.executeUpdate("insert into SPLICE.names values(1,'Charlie')");
        s.executeUpdate("insert into SPLICE.names values(2,'Hugh')");
        s.executeUpdate("insert into SPLICE.names values(3,'Alex')");

        // Now delete a row so we fire the trigger.
        s.executeUpdate("delete from tab where i = 1");
        // Check the log to make sure the trigger fired ok
        ResultSet loggedDeletes = s.executeQuery("SELECT * FROM SPLICE.LOG");
        JDBC.assertDrainResults(loggedDeletes, 1);
                 
        s.executeUpdate("DROP TABLE SPLICE.TAB");
        s.executeUpdate("DROP TABLE SPLICE.LOG");
        s.executeUpdate("DROP TABLE SPLICE.NAMES");
        
    }
    
    /**
     * Test that a nested loop join that accesses the 
     * TriggerNewTransitionRowsVTI can reopen the ResultSet properly 
     * when it re-executes.
     * @throws SQLException
     */
    public void testDerby4095NewTriggerRows() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE SPLICE.TAB (I INT)");
        s.executeUpdate("CREATE TABLE SPLICE.LOG (I INT, NAME VARCHAR(30), UPDTIME TIMESTAMP, NEWVALUE INT)");
        s.executeUpdate("CREATE TABLE SPLICE.NAMES(ID INT, NAME VARCHAR(30))");

        
        s.executeUpdate("CREATE TRIGGER  SPLICE.MYTRIG AFTER UPDATE ON SPLICE.TAB REFERENCING OLD_TABLE AS OLDROWS NEW_TABLE AS NEWROWS FOR EACH STATEMENT INSERT INTO SPLICE.LOG(i,name,updtime,newvalue) SELECT OLDROWS.I, NAMES.NAME, CURRENT_TIMESTAMP, NEWROWS.I  FROM --DERBY-PROPERTIES joinOrder=FIXED\n NAMES, NEWROWS --DERBY-PROPERTIES joinStrategy = NESTEDLOOP\n ,OLDROWS WHERE (NEWROWS.i = NAMES.ID) AND (1 = 1)");
        
        s.executeUpdate("insert into tab values(1)");
        s.executeUpdate("insert into tab values(2)");
        s.executeUpdate("insert into tab values(3)");

        s.executeUpdate("insert into names values(1,'Charlie')");
        s.executeUpdate("insert into names values(2,'Hugh')");
        s.executeUpdate("insert into names values(3,'Alex')");

        // Now update a row to fire the trigger
        s.executeUpdate("update tab set i=1 where i = 1");

        // Check the log to make sure the trigger fired ok
        ResultSet loggedUpdates = s.executeQuery("SELECT * FROM SPLICE.LOG");
        JDBC.assertDrainResults(loggedUpdates, 1);
        
        
        s.executeUpdate("DROP TABLE SPLICE.TAB");
        s.executeUpdate("DROP TABLE SPLICE.LOG");
        s.executeUpdate("DROP TABLE SPLICE.NAMES");
    }

    /**
     * Regression test case for DERBY-4610, where a DELETE statement failed
     * because a trigger used the wrong meta-data and mixed up the data types.
     */
    public void testDerby4610WrongDataType() throws SQLException {
        Statement s = createStatement();
        s.execute("create table testtable " +
                  "(id integer, name varchar(20), primary key(id))");
        s.execute("create table testchild (" +
                  "id integer constraint fk_id " +
                  "references testtable on delete cascade, " +
                  "ordernum int, primary key(id))");
        s.execute("create procedure testproc (str varchar(20)) " +
                  "PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME '" +
                  getClass().getName() + ".derby4610proc'");
        s.execute("create trigger testtabletrigger after delete on testtable " +
                  "referencing old as old " +
                  "for each row mode db2sql call testproc(char(old.id))");
        s.execute("create trigger testchildtrigger after delete on testchild " +
                  "referencing old as old " +
                  "for each row mode db2sql call testproc(char(old.ordernum))");
        s.execute("insert into testtable values (1, 'test1')");
        s.execute("insert into testchild values (1, 10)");

        // Used to fail with ERROR XCL12: An attempt was made to put a data
        // value of type 'java.lang.String' into a data value of type 'INTEGER'.
        assertUpdateCount(s, 1, "delete from testtable where id = 1");
    }

    /**
     * Procedure that does nothing. Called as a stored procedure in the
     * regression test case for DERBY-4610.
     */
    public static void derby4610proc(String str) {
        // do nothing
    }
}
