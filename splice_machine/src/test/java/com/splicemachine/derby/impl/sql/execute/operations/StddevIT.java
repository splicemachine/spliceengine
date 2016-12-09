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

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class StddevIT extends SpliceUnitTest {
    public static final String CLASS_NAME = StddevIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME = "TAB";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT, D DOUBLE)";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try(Connection conn = spliceClassWatcher.getOrCreateConnection()){
                        try(PreparedStatement ps = conn.prepareStatement(String.format("insert into %s (i) values (?)", spliceTableWatcher))){
                            for(int i=0;i<10;i++){
                                ps.setInt(1,i);
                                for(int j=0;j<100;j++){
                                    ps.addBatch();
                                }
                                ps.executeBatch();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * This '@Before' method is ran before every '@Test' method
     */
    @Before
    public void setUp() throws Exception {
        try(ResultSet resultSet = methodWatcher.executeQuery(
                String.format("select * from %s", this.getTableReference(TABLE_NAME)))){
            Assert.assertEquals(1000,resultSetSize(resultSet));
        }
    }

    @Test
    public void test() throws Exception {
        try(Connection conn = methodWatcher.getOrCreateConnection()){
            try(Statement s = conn.createStatement()){
                try(ResultSet rs = s.executeQuery(String.format("select stddev_pop(i) from %s", this.getTableReference(TABLE_NAME)))){
                    while(rs.next()){
                        Assert.assertEquals((int)rs.getDouble(1),2);
                    }
                }
                try(ResultSet rs = methodWatcher.executeQuery(String.format("select stddev_samp(i) from %s", this.getTableReference(TABLE_NAME)))){
                    while(rs.next()) {
                        Assert.assertEquals((int)rs.getDouble(1), 2);
                    }
                }

                try(ResultSet rs = methodWatcher.executeQuery(String.format("select stddev_pop(d) from %s", this.getTableReference(TABLE_NAME)))){
                    while(rs.next()){
                        Assert.assertEquals((int)rs.getDouble(1), 0);
                    }
                }

                try(ResultSet rs = methodWatcher.executeQuery(String.format("select stddev_samp(d) from %s", this.getTableReference(TABLE_NAME)))){
                    while(rs.next()) {
                        Assert.assertEquals((int)rs.getDouble(1), 0);
                    }
                }
            }
        }
    }

    @Test
    public void testRepeated() throws Exception {
        for(int i=0;i<100;i++){
            test();
        }
    }
}
