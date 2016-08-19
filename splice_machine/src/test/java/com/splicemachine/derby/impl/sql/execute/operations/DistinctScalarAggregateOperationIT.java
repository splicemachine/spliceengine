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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.splicemachine.test.SlowTest;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class DistinctScalarAggregateOperationIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static Logger LOG = Logger.getLogger(DistinctScalarAggregateOperationIT.class);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(DistinctScalarAggregateOperationIT.class.getSimpleName());
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("ORDERSUMMARY",DistinctScalarAggregateOperationIT.class.getSimpleName(),"(oid int, catalog varchar(40), score float, brand varchar(40))");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("EMPTY_TABLE",DistinctScalarAggregateOperationIT.class.getSimpleName(),"(oid int, catalog varchar(40), score float, brand char(40))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        Statement s = spliceClassWatcher.getStatement();
                        s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(1, 'clothes', 2, 'zara')");
                        s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(2, 'clothes', 2, 'ann taylor')");
                        s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(2, 'clothes', 2, 'd&g')");
                        s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(2, 'showes', 3, 'zara')");
                        s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(3, 'showes', 3, 'burberry')");
                        s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(3, 'clothes', 2, 'd&g')");
                        s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(3, 'handbags', 5, 'd&g')");
                        s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(3, 'handbags', 5, 'd&g')");
                        s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(4, 'furniture', 6, 'ea')");
//                    spliceClassWatcher.splitTable("ordersummary", schema.schemaName);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @BeforeClass
    public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("CREATE TABLE DistinctScalarAggregateOperationIT.t (i DOUBLE, j int)")
                .withInsert("insert into DistinctScalarAggregateOperationIT.t (i,j) values(?, ?)")
                .withRows(rows(
                        row(null, 1),
                        row(0.2533141765143777, 1),
                        row(0.5948622082194885, 1),
                        row(0.30925968785729474, 1),
                        row(0.1619137273689144, 1),
                        row(null, null),
                        row(0.6232005241074771, 1),
                        row(0.08817195574227321, 1),
                        row(0.6470844263248035, 1),
                        row(0.1385595309690708, 1),
                        row(0.003724732605326908, 1),
                        row(0.17947900748689327, 1),
                        row(0.8705926880889353, 1),
                        row(null, null),
                        row(null, null),
                        row(0.3032862585487939, 1),
                        row(null, null),
                        row(0.12924566497115575, 1),
                        row(0.9670417796816004, 1),
                        row(0.9089245098281875, 1))).create();
    }

    @Test
    public void testDistinctScalarAggregate() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select sum(distinct score),max(distinct score),min(distinct score) from" + this.getPaddedTableReference("ORDERSUMMARY"));
        if (rs.next()) {
            LOG.info("sum of distinct="+rs.getInt(1));
            Assert.assertEquals("incorrect sum",16, rs.getInt(1));
            Assert.assertEquals("incorrect max",6,rs.getInt(2));
            Assert.assertEquals("incorrect min",2,rs.getInt(3));
        }else{
            Assert.fail("No results returned!");
        }
    }

    @Test
    @Category(SlowTest.class)
    public void testDistinctScalarAggregateRepeatedly() throws Exception {
        /*
         * This is a test to attempt to reproduce Bug 480. Under normal circumstances, this test
         * does nothing for us except take forever to run, so most of the time it should be ignored.
         */
        for(int i=0;i<100;i++){
            if(i%10==0)
                System.out.printf("Ran %d times without failure%n",i);
            testDistinctScalarAggregate();
        }
    }

    @Test
    public void testDistinctCountWithQualifiedPreparedStatement() throws Exception {
				/*Regression test for DB-2213*/
        PreparedStatement ps = methodWatcher.prepareStatement("select count(distinct score) from "+spliceTableWatcher+" where oid = ?");

        //set it to 2
        ps.setInt(1,2);
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue("Did not return any rows!",rs.next());
        Assert.assertEquals("Incorrect count!",2,rs.getInt(1));

        //now set it to something else and make sure it's correct
        ps.setInt(1,3);
        rs = ps.executeQuery();
        Assert.assertTrue("Did not return any rows!",rs.next());
        Assert.assertEquals("Incorrect count!",3,rs.getInt(1));
    }

    @Test
    public void testDistinctCount() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(distinct score) from" + this.getPaddedTableReference("ORDERSUMMARY"));
        if (rs.next()) {
            LOG.info("count of distinct="+rs.getInt(1));
            Assert.assertEquals("incorrect count",4,rs.getInt(1));
        }else{
            Assert.fail("No results returned!");
        }
    }

    @Test
    public void testDistinctCountOrderBy() throws Exception {
        Assert.assertEquals(4L,(long) methodWatcher.query("select count(distinct score) from " + this.getPaddedTableReference("ORDERSUMMARY") + "order by 1"));
    }

    @Test
    public void testDistinctScalarAggregateReturnsZeroOnEmptyTable() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(distinct brand),min(distinct brand),max(distinct brand) from "+ this.getPaddedTableReference("EMPTY_TABLE"));
        Assert.assertTrue("No rows returned!",rs.next());
        Assert.assertEquals("incorrect count returned",0,rs.getInt(1));
    }

    @Test
    public void testMultipleAggregate() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select min(oid), max(oid), count(distinct catalog), sum(score)  from "+ this.getPaddedTableReference("ORDERSUMMARY"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals(4, rs.getInt(2));
        Assert.assertEquals(4, rs.getInt(3));
        Assert.assertEquals(30.0, rs.getFloat(4), 0.01);
    }

    @Test
    public void testColumnWithNullValues() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(distinct i)  from "+ this.getPaddedTableReference("T"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(15, rs.getInt(1));
    }
}
