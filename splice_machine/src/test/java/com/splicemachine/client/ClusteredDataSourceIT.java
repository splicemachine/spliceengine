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

package com.splicemachine.client;

import com.splicemachine.db.client.cluster.*;
import com.splicemachine.db.client.net.NetConnection;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

/**
 * Integration tests for usage of the ClusteredDataSource.
 *
 * This is located here and not in db-client because it assumes a running Database is running.
 *
 * @author Scott Fines
 *         Date: 8/23/16
 */
public class ClusteredDataSourceIT{

    @Test
    public void testCanGetConnection() throws Exception{
        ConfiguredServerPoolFactory cspf = new ConfiguredServerPoolFactory("splicedb","splice","admin", new DeadlineFailureDetectorFactory(Long.MAX_VALUE),InfinitePoolSize.INSTANCE);
        ClusteredDataSource cds = ClusteredDataSource.newBuilder()
                .servers("localhost:1527")
                .serverPoolFactory(cspf)
                .connectionSelection(ConnectionStrategy.ROUND_ROBIN)
                .heartbeatPeriod(1000L)
                .discoveryWindow(10000L)
                .build();

        try(Connection conn = cds.getConnection()){
            try(Statement s = conn.createStatement()){
                queryValues(s);
            }
        }
    }


    @Test
    public void testCanGetConnectionThroughDriver() throws Exception{
        String url = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin";
//        Class.forName(ClusteredDriver.class.getCanonicalName());

        try(Connection conn =DriverManager.getConnection(url)){
            try(Statement s = conn.createStatement()){
                queryValues(s);
            }
        }
    }


    @Test
    public void repeatedDriver() throws Exception{
        for(int i=0;i<100;i++){
            System.out.println(i);
            testCanGetConnectionThroughDriver();
        }
    }

    @Test
    public void testCanDoStatementsAutoCommitOff() throws Exception{
        try(Connection conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb;user=splice;password=admin")){
            conn.setAutoCommit(false);
            for(int i=0;i<100;i++){
                try(Statement s=conn.createStatement()){
                    queryValues(s);
                }
            }
            conn.rollback();
        }
    }

    @Test
    public void testCreateAndDropTablesAutoCommittOff() throws Exception{
        try(Connection conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb;user=splice;password=admin")){
            conn.setAutoCommit(false);
            int initialTable = 1;
            int tables = 0;
            try(Statement s = conn.createStatement()){
                while(tables<5){
                    try{
                        s.execute("create table t"+initialTable+"(a int)");
                        tables++;
                    }catch(SQLException se){
                        if(!"X0Y32".equals(se.getSQLState())) throw se;
                        initialTable++;
                    }
                }
            }
            conn.rollback();
        }
    }

    @Test
    public void testDisablingClusteredConnectionRevertsToDerby() throws Exception{

        try(Connection conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;clustered=false")){
           Assert.assertTrue("Incorrect driver class!",NetConnection.class.isAssignableFrom(conn.getClass()));
        }

    }

    /* ****************************************************************************************************************/
    /*private helper methodss*/
    private void queryValues(Statement s) throws SQLException{
        try(ResultSet rs = s.executeQuery("values (1)")){
            Assert.assertTrue("No rows returned!",rs.next());
            Assert.assertEquals("Incorrect value returned!",1,rs.getInt(1));
            Assert.assertFalse("Too many rows returned!",rs.next());
        }
    }
}
