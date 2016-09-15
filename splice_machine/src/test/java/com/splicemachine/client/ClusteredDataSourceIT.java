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
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

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
                try(ResultSet rs = s.executeQuery("values (1)")){
                    Assert.assertTrue("No rows returned!",rs.next());
                    Assert.assertEquals("Incorrect value returned!",1,rs.getInt(1));
                    Assert.assertFalse("Too many rows returned!",rs.next());
                }
            }
        }
    }


    @Test
    public void testCanGetConnectionThroughDriver() throws Exception{
        String url = "jdbc:spliceClustered://localhost:1527/splicedb;user=splice;password=admin";
        Class.forName(ClusteredDriver.class.getCanonicalName());

        try(Connection conn =DriverManager.getDriver(url).connect(url,null)){
            try(Statement s = conn.createStatement()){
                try(ResultSet rs = s.executeQuery("values (1)")){
                    Assert.assertTrue("No rows returned!",rs.next());
                    Assert.assertEquals("Incorrect value returned!",1,rs.getInt(1));
                    Assert.assertFalse("Too many rows returned!",rs.next());
                }
            }
        }
    }

}
