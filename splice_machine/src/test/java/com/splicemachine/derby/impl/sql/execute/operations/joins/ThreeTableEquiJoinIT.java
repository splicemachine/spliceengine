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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 7/28/14
 */
@RunWith(Parameterized.class)
@Ignore("DB-4272")
public class ThreeTableEquiJoinIT {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<String> joinStrategies = Arrays.asList(
                "nestedloop",
                "broadcast",
                "sortmerge"//,
//                "hash"
        );
        Collection<Object[]> data = Lists.newArrayList();
        for(String first: joinStrategies){
            for (String second : joinStrategies) {
                data.add(new Object[]{first, second});
            }
        }

        return data;
    }

    private final String firstJoin;
    private final String secondJoin;

    public ThreeTableEquiJoinIT(String firstJoin, String secondJoin) {
        this.firstJoin = firstJoin;
        this.secondJoin = secondJoin;
    }

    private static final String CLASS_NAME=ThreeTableEquiJoinIT.class.getSimpleName();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);
    private static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(TestUtils.createFileDataWatcher(classWatcher, "small_msdatasample/startup.sql", CLASS_NAME))
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(CallableStatement cs = classWatcher.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
                        cs.setString(1,"SYS");
                        cs.execute();

                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                    try(CallableStatement cs = classWatcher.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
                        cs.setString(1,schemaWatcher.schemaName);
                        cs.execute();

                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher(getClass().getSimpleName());

    @Test
    public void testThreeTableJoinWorks() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                "select \n" +
                        "t1.orl_order_id, t2.cst_id, t3.itm_id \n" +
                "from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
                        "order_line t1 \n"+
                        ", customer t2 --SPLICE-PROPERTIES joinStrategy="+firstJoin +"\n" +
                        ", item t3 --SPLICE-PROPERTIES joinStrategy="+secondJoin+"\n" +
                "where \n" +
                        "t1.orl_customer_id = t2.cst_id \n" +
                        "and t1.orl_item_id = t3.itm_id \n" +
                        "order by t3.itm_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(10, results.size());

        Map zero = results.get(0);
        Map fifth = results.get(5);

        Assert.assertEquals("10058_7_1", zero.get("ORL_ORDER_ID"));
        Assert.assertEquals(143, zero.get("CST_ID"));
        Assert.assertEquals(7, zero.get("ITM_ID"));

        Assert.assertEquals("10059_274_1", fifth.get("ORL_ORDER_ID"));
        Assert.assertEquals(327, fifth.get("CST_ID"));
        Assert.assertEquals(274, fifth.get("ITM_ID"));
    }

    @Test
    public void testThreeTableJoinExtraProjections() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                "select " +
                        "t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
                        "order_line t1" +
                        ", customer t2 --SPLICE-PROPERTIES joinStrategy="+firstJoin+"\n" +
                        ", item t3 --SPLICE-PROPERTIES joinStrategy="+secondJoin+"\n" +
                "where " +
                        "t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id " +
                "order by t3.itm_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

        Map favRooms = results.get(0);
        Map seal = results.get(5);

        Assert.assertEquals("10058_7_1", favRooms.get("ORL_ORDER_ID"));
        Assert.assertEquals("Deutsch", favRooms.get("CST_LAST_NAME"));
        Assert.assertEquals("Leslie", favRooms.get("CST_FIRST_NAME"));
        Assert.assertEquals("50 Favorite Rooms", favRooms.get("ITM_NAME"));

        Assert.assertEquals("10059_274_1", seal.get("ORL_ORDER_ID"));
        Assert.assertEquals("Marko", seal.get("CST_LAST_NAME"));
        Assert.assertEquals("Shelby", seal.get("CST_FIRST_NAME"));
        Assert.assertEquals("Seal (94)", seal.get("ITM_NAME"));
    }

    @Test
    public void testThreeTableJoinWithCriteria() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                "select " +
                        "t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
                        "order_line t1" +
                        ", customer t2 --SPLICE-PROPERTIES joinStrategy="+firstJoin+"\n" +
                        ", item t3 --SPLICE-PROPERTIES joinStrategy="+secondJoin+"\n" +
                "where " +
                        "t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id" +
                "      and t2.cst_last_name = 'Deutsch'");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(2, results.size());


        int foundRows=0;
        for(int i=0;i<results.size();i++){
            Map value = results.get(i);
            if("10058_325_1".equals(value.get("ORL_ORDER_ID"))){
                foundRows++;
                Assert.assertEquals("Deutsch", value.get("CST_LAST_NAME"));
                Assert.assertEquals("Leslie", value.get("CST_FIRST_NAME"));
                Assert.assertEquals("Waiting to Exhale: The Soundtrack", value.get("ITM_NAME"));
            }else if("10058_7_1".equals(value.get("ORL_ORDER_ID"))){
                foundRows++;
                Assert.assertEquals("Deutsch", value.get("CST_LAST_NAME"));
                Assert.assertEquals("Leslie", value.get("CST_FIRST_NAME"));
                Assert.assertEquals("50 Favorite Rooms", value.get("ITM_NAME"));
            }else{
                Assert.fail("Unexpected row returned:"+value.get("ORL_ORDER_ID"));
            }
        }
        Assert.assertEquals("Incorrect number of rows returned",2,foundRows);

    }

    @Test
    public void testThreeTableJoinWithSorting() throws Exception {
        ResultSet rs1 = methodWatcher.executeQuery(
                "select " +
                "t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
                "order_line t1" +
                ", customer t2 --SPLICE-PROPERTIES joinStrategy="+firstJoin+"\n" +
                ", item t3 --SPLICE-PROPERTIES joinStrategy="+secondJoin+"\n" +
                "where " +
                "t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id " +
                "order by orl_order_id asc");

        ResultSet rs2 = methodWatcher.executeQuery(
                "select " +
                        "t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                        "from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
                        "order_line t1" +
                        ", customer t2 --SPLICE-PROPERTIES joinStrategy="+firstJoin+"\n" +
                        ", item t3 --SPLICE-PROPERTIES joinStrategy="+secondJoin+"\n" +
                        "where " +
                        "t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id " +
                        "order by orl_order_id desc");


        List<Map> results1 = TestUtils.resultSetToMaps(rs1);
        LinkedList<Map> results2 = new LinkedList<Map>(TestUtils.resultSetToMaps(rs2));

        Assert.assertEquals(10, results1.size());
        Assert.assertEquals(10, results2.size());

        Iterator<Map> it1 = results1.iterator();
        Iterator<Map> it2 = results2.descendingIterator();

        while( it1.hasNext() && it2.hasNext()){
            Assert.assertEquals(it1.next(), it2.next());
        }
    }

    @Test
    public void testThreeTableJoinOnItems() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(
                "select " +
                        "t1.itm_name, t2.sbc_desc, t3.cat_name " +
                "from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
                        "item t1" +
                        ", category_sub t2 --SPLICE-PROPERTIES joinStrategy = "+ firstJoin+"\n" +
                        ", category t3 --SPLICE-PROPERTIES joinStrategy="+secondJoin+"\n" +
                "where " +
                        "t1.itm_subcat_id = t2.sbc_id and t2.sbc_category_id = t3.cat_id " +
                "order by t1.itm_name");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

        Assert.assertEquals("50 Favorite Rooms", results.get(0).get("ITM_NAME"));
        Assert.assertEquals("MicroStrategy Books", results.get(0).get("CAT_NAME"));
        Assert.assertEquals("Art & Architecture", results.get(0).get("SBC_DESC"));

        Assert.assertEquals("Seal (94)", results.get(7).get("ITM_NAME"));
        Assert.assertEquals("MicroStrategy Music", results.get(7).get("CAT_NAME"));
        Assert.assertEquals("Alternative", results.get(7).get("SBC_DESC"));

    }

    @Test
    public void testThreeTableJoinOnItemsWithCriteria() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(
                "select " +
                        "t1.itm_name, t2.sbc_desc, t3.cat_name " +
                "from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                        "item t1" +
                        ", category_sub t2 --SPLICE-PROPERTIES joinStrategy="+firstJoin+"\n" +
                        ", category t3 --SPLICE-PROPERTIES joinStrategy="+secondJoin+"\n" +
                "where t1.itm_subcat_id = t2.sbc_id and t2.sbc_category_id = t3.cat_id " +
                "and t1.itm_name = 'Seal (94)'");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(1, results.size());

        Assert.assertEquals("Seal (94)", results.get(0).get("ITM_NAME"));
        Assert.assertEquals("MicroStrategy Music", results.get(0).get("CAT_NAME"));
        Assert.assertEquals("Alternative", results.get(0).get("SBC_DESC"));
    }
}
