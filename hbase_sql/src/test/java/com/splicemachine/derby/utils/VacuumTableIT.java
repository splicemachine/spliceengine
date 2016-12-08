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
 *
 */

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.RuledConnection;
import com.splicemachine.derby.test.framework.SchemaRule;
import com.splicemachine.derby.test.framework.TableRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

/**
 * @author Scott Fines
 *         Date: 12/7/16
 */
public class VacuumTableIT{
    private final RuledConnection conn = new RuledConnection(null,false);

    private final TableRule t = new TableRule(conn,"T","(a int, b int)");

    @Rule
    public final TestRule rules = RuleChain.outerRule(conn)
            .around(new SchemaRule(conn,VacuumTableIT.class.getSimpleName()))
            .around(t);

    @Test
    public void vacuumTableDoesntBreakInserts() throws Exception{
        int[][] correct = new int[][]{
                new int[]{1,1},
                new int[]{2,2},
                new int[]{3,3},
                new int[]{4,4},
                new int[]{5,5}
        };
        loadData(correct);

        //load the data
        conn.commit();
        rollMAT();

        //vacuum the table
        vacuumTable();

        verifyData(correct);
    }

    @Test
    public void vacuumTableIgnoresDeletes() throws Exception{
        int[][] data = new int[][]{
                new int[]{1,1},
                new int[]{2,2},
                new int[]{3,3},
                new int[]{4,4},
                new int[]{5,5}
        };
        loadData(data);

        try(Statement s = conn.createStatement()){
            s.execute("delete from T where a = 3");
        }

        //load the data
        conn.commit();
        rollMAT();

        //vacuum the table
        vacuumTable();

        int[][] correctData = new int[][]{
                new int[]{1,1},
                new int[]{2,2},
                new int[]{4,4},
                new int[]{5,5}
        };
        verifyData(correctData);
    }

    @Test
    public void vacuumTableProcessesAntiTombstones() throws Exception{
        int[][] data = new int[][]{
                new int[]{1,1},
                new int[]{2,2},
                new int[]{3,3},
                new int[]{4,4},
                new int[]{5,5}
        };
        loadData(data);

        try(Statement s = conn.createStatement()){
            s.execute("delete from T where a = 3");
            s.execute("insert into T (a,b) values (3,4)");
        }

        //load the data
        conn.commit();
        rollMAT();

        //vacuum the table
        vacuumTable();

        int[][] correctData = new int[][]{
                new int[]{1,1},
                new int[]{2,2},
                new int[]{3,4},
                new int[]{4,4},
                new int[]{5,5}
        };
        verifyData(correctData);
    }

    @Test
    public void vacuumTableMergesUpdates() throws Exception{
        int[][] data = new int[][]{
                new int[]{1,1},
                new int[]{2,2},
                new int[]{3,3},
                new int[]{4,4},
                new int[]{5,5}
        };
        loadData(data);

        try(Statement s = conn.createStatement()){
            s.execute("update T set b = 4 where a = 3");
        }

        //load the data
        conn.commit();
        rollMAT();

        //vacuum the table
        vacuumTable();

        int[][] correctData = new int[][]{
                new int[]{1,1},
                new int[]{2,2},
                new int[]{3,4},
                new int[]{4,4},
                new int[]{5,5}
        };
        verifyData(correctData);
    }

    @Test
    public void vacuumTableIgnoresRolledBackDeletes() throws Exception{
        int[][] data = new int[][]{
                new int[]{1,1},
                new int[]{2,2},
                new int[]{3,3},
                new int[]{4,4},
                new int[]{5,5}
        };
        loadData(data);

        //load the data
        conn.commit();

        try(Statement s = conn.createStatement()){
            s.execute("delete from T where a = 3");
        }
        conn.rollback();
        rollMAT();

        //vacuum the table
        vacuumTable();

        int[][] correctData = new int[][]{
                new int[]{1,1},
                new int[]{2,2},
                new int[]{3,3},
                new int[]{4,4},
                new int[]{5,5}
        };
        verifyData(correctData);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void verifyData(int[][] correct) throws SQLException{
        try(Statement s = conn.createStatement()){
            boolean[] found = new boolean[correct.length];
            try(ResultSet rs = s.executeQuery("select * from T")){
                while(rs.next()){
                    int a = rs.getInt(1);
                    Assert.assertFalse("null a value!",rs.wasNull());
                    int b = rs.getInt(2);
                    Assert.assertFalse("null b value!",rs.wasNull());

                    boolean rowFound = false;
                    for(int i=0;i<correct.length;i++){
                        if(correct[i][0]==a){
                            Assert.assertFalse("Same row twice!",found[i]);
                            Assert.assertEquals("incorrect b value for a value <"+a+">",correct[i][1],b);
                            found[i] = rowFound = true;
                            break;
                        }
                    }
                    Assert.assertTrue("Did not find row ("+a+","+b+")",rowFound);
                }
            }
        }
    }

    private void vacuumTable() throws SQLException{
        try(CallableStatement vacuum = conn.prepareCall("call SYSCS_UTIL.VACUUM_TABLE(?,?)")){
            vacuum.setString(1,conn.getSchema());
            vacuum.setString(2,"T");

            vacuum.execute();
        }
    }

    private void rollMAT() throws SQLException{
        try(CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.SYSCS_GET_ACTIVE_TRANSACTION_COUNTS()")){
            /*
             * This has a nice side-effect of pushing the stored MAT up to the latest, as long as you
             * are only working with a single server. If you aren't, then this won't necessarily do it, but it's
             * worth doing just in case.
             */
            cs.execute();
        }
    }

    private void loadData(int[][] data) throws SQLException{
        try(PreparedStatement ps = conn.prepareStatement("insert into T(a,b) values (?,?)")){
            for(int[] row : data){
                ps.setInt(1,row[0]); ps.setInt(2,row[1]); ps.execute();
            }
        }
    }
}
