/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.customer.Price;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

/**
 *
 * Created by jyuan on 11/3/15.
 */
@Category(SerialTest.class) //serial because it loads a jar
public class SpliceUDTIT extends SpliceUnitTest {
    public static final String CLASS_NAME = SpliceUDTIT.class.getSimpleName().toUpperCase();
    protected static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static final String CALL_INSTALL_JAR_FORMAT_STRING = "CALL SQLJ.INSTALL_JAR('%s', '%s', 0)";
    private static final String CALL_REMOVE_JAR_FORMAT_STRING = "CALL SQLJ.REMOVE_JAR('%s', 0)";
    private static final String STORED_PROCS_JAR_FILE = getResourceDirectory() + "UDT.jar";
    private static final String JAR_FILE_SQL_NAME = CLASS_NAME + ".UDT_JAR";

    private static final String CALL_SET_CLASSPATH_FORMAT_STRING = "CALL SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath', '%s')";


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @ClassRule
    public static final SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void setup() throws Exception {
        createData(spliceClassWatcher);
    }

    private static void createData(SpliceWatcher w) throws Exception {
        try {
            w.execute(String.format(CALL_INSTALL_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
        }catch(SQLException se){
            if(!"SE014".equals(se.getSQLState())){
                //write-conflict. That means someone ELSE is loading this same jar. That's cool, just keep going
                throw se;
            }
        }
        w.execute(String.format(CALL_SET_CLASSPATH_FORMAT_STRING,JAR_FILE_SQL_NAME));
        w.execute("create derby aggregate median for int external name 'com.splicemachine.customer.Median'");

        Connection conn = w.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table t(i int)")
                .withInsert("insert into t values(?)")
                .withRows(rows(
                        row(1),
                        row(2),
                        row(3),
                        row(4),
                        row(5)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t1(itemName varchar(30), rawPrice int)")
                .withInsert("insert into t1 values(?, ?)")
                .withRows(rows(
                row("Coffee", 4),
                row("Tea", 3),
                row("Milk", 2),
                row("Soda", 2),
                row("Bagel", 1),
                row("Donut", 1)))
                .create();

        w.execute("CREATE TYPE price EXTERNAL NAME 'com.splicemachine.customer.Price' language Java");
        w.execute("CREATE FUNCTION makePrice(varchar(30), double)\n"+
                "RETURNS Price\n"+
                "LANGUAGE JAVA\n"+
                "PARAMETER STYLE JAVA\n"+
                "NO SQL EXTERNAL NAME 'com.splicemachine.customer.CreatePrice.createPriceObject'");

        w.execute("create function getAmount( int ) returns double language java parameter style java no sql\n" +
                       "external name 'com.splicemachine.customer.Price.getAmount'\n" );

        w.execute("create table orders(orderID INT,customerID INT,totalPrice price)");
        w.execute("insert into orders values (12345, 12, makePrice('USD', 12))");

        w.execute("CREATE FUNCTION testConnection()\n" +
                "RETURNS VARCHAR(100)\n" +
                "LANGUAGE JAVA\n" +
                "PARAMETER STYLE JAVA \n" +
                "EXTERNAL NAME 'com.splicemachine.customer.NielsenTesting.testInternalConnection'");
        w.execute("create table test(id integer, name varchar(30))");
        w.execute("insert into test values(1,'erin')");

        w.execute("create function throwNPE(int)\n" +
                "RETURNS int\n" +
                "LANGUAGE JAVA\n" +
                "PARAMETER STYLE JAVA \n" +
                "EXTERNAL NAME 'com.splicemachine.customer.ThrowNPE.throwNPE'");

    }

    @Test
    public void testAggregationReferencingUDF() throws Exception {
        try(ResultSet rs =  methodWatcher.executeQuery("SELECT count(*) FROM\n" +
                                                "(\n" +
                                                "SELECT makePrice('USD', t1.rawPrice) AS ItemPrice\n" +
                                                "FROM t1" +
                                                ") x")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(6, rs.getInt(1));
        }

        try(ResultSet rs =  methodWatcher.executeQuery("SELECT count(*) FROM\n" +
            "(\n" +
            "SELECT makePrice('USD', t1.rawPrice) AS ItemPrice\n" +
            "FROM t1 JOIN t\n" +
            "ON t.i = t1.rawPrice\n" +
            ") x") ) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(6, rs.getInt(1));
        }

        try(ResultSet rs =  methodWatcher.executeQuery("SELECT count(*) from t1 where testconnection() < 'a'") ) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(6, rs.getInt(1));
        }

        try(ResultSet rs =  methodWatcher.executeQuery("SELECT count(testconnection()) from t1")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(6, rs.getInt(1));
        }
    }

    @Test
    public void testUDA() throws Exception {
        try(ResultSet rs = methodWatcher.executeQuery("select median(i) from t")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    public void testUDT() throws Exception {
        try(ResultSet rs = methodWatcher.executeQuery("select orderID, customerID, totalPrice from orders")) {
            Assert.assertTrue(rs.next());
            int oid = rs.getInt(1);
            int cid = rs.getInt(2);
            Price price = (Price) rs.getObject(3);
            Assert.assertEquals(12345, oid);
            Assert.assertEquals(12, cid);
            Assert.assertEquals(12, price.amount, 0.01);
            Assert.assertTrue(price.currencyCode.compareTo("USD") == 0);
        }
    }

    @Test
    public void testSelectStatistics() throws Exception {
        methodWatcher.execute("analyze schema " + CLASS_NAME);
        try(ResultSet rs = methodWatcher.executeQuery("select count(*) from sysvw.syscolumnstatistics") ) {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getInt(1) > 0);
        }
    }

    @Test
    public void testConnection() throws Exception {
        try(SpliceWatcher watcher2 = new SpliceWatcher(CLASS_NAME))
        {
            watcher2.setConnection( watcher2.connectionBuilder().useOLAP(true).build() );
            try(ResultSet rs = watcher2.executeQuery("select testConnection() from test")) {
                String result = rs.next() ? rs.getString(1) : null;
                Assert.assertNotNull(result);
                Assert.assertTrue(result, result.compareTo("Got an internal connection") == 0);
            }
        }
    }

    @Test
    public void testSparkUDA() throws Exception {
        methodWatcher.execute("create derby aggregate string_concat for varchar(2000) external name 'com.splicemachine.tools.StringConcat'");

        methodWatcher.execute("create table strings (v varchar(20))");
        methodWatcher.execute("insert into strings values 'a','b','c','d'");

        for (boolean useSpark : Arrays.asList(true, false)) {
            try(ResultSet rs = methodWatcher.executeQuery("select string_concat(v) from strings --splice-properties useSpark="+useSpark)) {
                Assert.assertTrue(rs.next());
                String res = rs.getString(1);
                assertThat(res, allOf(containsString("a"), containsString("b"), containsString("c"), containsString("d")));
            }
        }
    }

    @Test
    public void testDB12107() throws Exception {
        methodWatcher.execute("create table t0(c0 boolean, c1 timestamp)");
        methodWatcher.execute("create table t2(c0 timestamp not null)");
        methodWatcher.execute("insert into t0(c0) values (false)");
        methodWatcher.execute("INSERT INTO T0(C0, C1) VALUES (TRUE, '1970-01-13 04:08:02'), (TRUE, '1969-12-12 03:20:09')");
        methodWatcher.execute("INSERT INTO T0(C1) VALUES ('1969-12-12 03:20:09')");
        methodWatcher.execute("INSERT INTO T0(C1) VALUES ('1970-01-13 08:53:47')");

        testFail("SELECT MAX(agg0) " +
                "FROM (SELECT MAX(T2.C0)  as agg0 FROM T0 LEFT OUTER JOIN T2 ON ((T0.C1)!=(T2.C0)) and 1 = thrownpe(1)" +
                "WHERE T0.C0 and 1 = thrownpe(1)" +
                "UNION ALL " +
                "SELECT MAX(T2.C0)  as agg0 " +
                "FROM T0 LEFT OUTER JOIN T2 ON ((T0.C1)!=(T2.C0)) and 1 = thrownpe(1) " +
                "WHERE NOT (T0.C0) and 1 = thrownpe(1)" +
                "UNION ALL " +
                "SELECT MAX(T2.C0)  as agg0 " +
                "FROM T0 LEFT OUTER JOIN T2 ON ((T0.C1)!=(T2.C0)) and 1 = thrownpe(1)" +
                "WHERE (T0.C0) IS NULL and 1 = thrownpe(1)) as asdf",
                Collections.singletonList("Splice Engine exception: unexpected exception"),
            methodWatcher);

        methodWatcher.execute("drop table t0");
        methodWatcher.execute("drop table t2");
    }
}
