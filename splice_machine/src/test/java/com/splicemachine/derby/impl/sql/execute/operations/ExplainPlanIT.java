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
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

import static java.lang.String.format;

/**
 * Created by jyuan on 10/7/14.
 */
public class ExplainPlanIT extends SpliceUnitTest  {

    public static final String CLASS_NAME = ExplainPlanIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME = "A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try{
                        PreparedStatement ps=spliceClassWatcher.prepareStatement(format("insert into %s.%s values (?)",CLASS_NAME,TABLE_NAME));
                        for(int i=0;i<10;i++){
                            ps.setInt(1, i);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        ps = spliceClassWatcher.prepareStatement(
                                format("insert into %s.%s select * from %s.%s", CLASS_NAME,TABLE_NAME, CLASS_NAME,TABLE_NAME));
                        for (int i = 0; i < 11; ++i) {
                            ps.execute();
                        }
                        ps = spliceClassWatcher.prepareStatement(format("analyze schema %s", CLASS_NAME));
                        ps.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }

            });
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testExplainSelect() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("explain select * from %s", this.getTableReference(TABLE_NAME)));

        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertTrue(count>0);
    }

    @Test
    public void testExplainUpdate() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("explain update %s set i = 0 where i = 1", this.getTableReference(TABLE_NAME)));

        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertTrue(count>0);
    }

    @Test
    public void testExplainDelete() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("explain delete from %s where i = 1", this.getTableReference(TABLE_NAME)));

        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertTrue(count>0);
    }

    @Test
    public void testExplainTwice() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("-- some comments \n explain\nupdate %s set i = 0 where i = 1", this.getTableReference(TABLE_NAME)));
        int count1 = 0;
        while (rs.next()) {
            ++count1;
        }
        rs.close();
        rs  = methodWatcher.executeQuery(
                String.format("-- some comments \n explain\nupdate %s set i = 0 where i = 1", this.getTableReference(TABLE_NAME)));
        int count2 = 0;
        while (rs.next()) {
            ++count2;
        }
        Assert.assertTrue(count1 == count2);
    }

    @Test
    public void testUseSpark() throws Exception {
        String sql = format("explain select * from %s.%s --SPLICE-PROPERTIES useSpark=false", CLASS_NAME, TABLE_NAME);
        ResultSet rs  = methodWatcher.executeQuery(sql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue("expect explain plan contains useSpark=false", rs.getString(1).contains("engine=control"));

        sql = format("explain select * from %s.%s", CLASS_NAME, TABLE_NAME);
        rs  = methodWatcher.executeQuery(sql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue("expect explain plan contains useSpark=true", rs.getString(1).contains("engine=Spark"));

    }

    @Test
    public void testSparkConnection() throws Exception {
        String url = "jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin;useSpark=true";
        Connection connection = DriverManager.getConnection(url, new Properties());
        connection.setSchema(CLASS_NAME.toUpperCase());
        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery("explain select * from A");
        Assert.assertTrue(rs.next());
        Assert.assertTrue("expect explain plan contains useSpark=false", rs.getString(1).contains("engine=Spark"));

    }
}
