/*
 *
 *  * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *  *
 *  * This file is part of Splice Machine.
 *  * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 *  * GNU Affero General Public License as published by the Free Software Foundation, either
 *  * version 3, or (at your option) any later version.
 *  * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  * See the GNU Affero General Public License for more details.
 *  * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 *  * If not, see <http://www.gnu.org/licenses/>.
 *
 *
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.catalog.types.TypeDescriptorImpl;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A Collection of ITs oriented around scanning and inserting from Spark with failures.
 */
public class FailuresSparkIT {
    private static Logger LOG=Logger.getLogger(FailuresSparkIT.class);

    public static final String CLASS_NAME = FailuresSparkIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    private static final SpliceTableWatcher a_table = new SpliceTableWatcher("a",CLASS_NAME,"(col1 int primary key)");

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(a_table);

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(CLASS_NAME);


    @Test
    public void testSparkSerializesFullTxnStack() throws Throwable {
        String uniqueName="veryUniqueTableNameNotUsedBefore";
        try (Connection c = methodWatcher.createConnection()) {
            c.setAutoCommit(false);

            try(Statement s =c.createStatement()) {
                s.executeUpdate("create table "+uniqueName+"(a int)");

                c.setSavepoint("pt1");

                try(ResultSet rs = s.executeQuery("select tablename from sys.systables --splice-properties useSpark=true \n" +
                        "where tablename = '" + uniqueName.toUpperCase() +"'")) {
                    assertTrue("Spark scan couldn't see table created on its own transaction", rs.next());
                }
            }
            c.rollback();
        }
    }

    @Test
    public void testPKViolationIsRolledback() throws Throwable {
        try(Statement s =methodWatcher.getOrCreateConnection().createStatement()) {
            String sql = "insert into a (col1) select * from ( " +
                    "values (1) union all values (1) " +
                    "union all select * from a --splice-properties useSpark=true\n" +
                    ") a";
            try {
                s.executeUpdate(sql);
                fail("Should have failed due to PK violation");
            } catch (SQLException se) {
                // ERROR 23505: The statement was aborted because it would have caused a duplicate key value in a unique
                Assert.assertEquals("Should fail with PK violation exception", "23505", se.getSQLState());
            }
            try (ResultSet rs = s.executeQuery("select * from a")) {
                Assert.assertFalse("Rows returned from query!", rs.next());
            }
        }
    }

    @Test(timeout = 10000)
    public void testDeletedCellsInMemstoreDontHangSparkScans() throws Throwable {
        try (Connection con1 = methodWatcher.createConnection()) {
            con1.setAutoCommit(false);
            try (Connection con2 = methodWatcher.createConnection()) {
                con2.setAutoCommit(false);

                try (Statement s2 = con2.createStatement()) {
                    // start con2 transaction
                    try (ResultSet rs = s2.executeQuery("select * from a")) {
                        rs.next();
                    }


                    try (Statement s1 = con1.createStatement()) {
                        s1.executeUpdate("insert into a values 10");
                    }
                    con1.rollback();

                    // force WW conflict to rollfoward (by deleting) the previous write
                    try {
                        s2.executeUpdate("insert into a values 10");
                        fail("Expected WW conflict");
                    } catch (SQLException se) {
                        assertEquals("SE014", se.getSQLState());
                    }


                    con2.commit();


                    // run Spark query
                    try (Statement s1 = con1.createStatement()) {
                        try (ResultSet rs = s1.executeQuery("select * from a --splice-properties useSpark=true")) {
                            while (rs.next()) {
                                // do nothing
                            }
                        }
                    }

                    con1.commit();
                }
            }

        }
    }

    @Test
    public void testSparkReadsSysColumns() throws Throwable {
        try(Statement s =methodWatcher.getOrCreateConnection().createStatement()) {
            String sql = "select a.columndatatype from sys.syscolumns a --splice-properties useSpark=true, joinStrategy=sortmerge\n" +
            "join sys.syscolumns b on  a.columnname = b.columnname {limit 10}";
            try(ResultSet rs = s.executeQuery(sql)) {
                assertTrue(rs.next());
                Object object = rs.getObject(1);
                assertTrue(object instanceof TypeDescriptorImpl);
            }
        }
    }


    @Test
    public void testResubmitToSpark() throws Throwable {
        try(Statement s =methodWatcher.getOrCreateConnection().createStatement()) {
            s.execute("create table resubmit(i int)");
            s.executeUpdate("insert into resubmit values 1");
            for (int i = 0; i < 10; ++i) {
                s.executeUpdate("insert into resubmit select i + (select count(*) from resubmit) from resubmit");
            }
            String sql = "select count(distinct a.i + (b.i*10001)) from resubmit a, resubmit b";
            try(ResultSet rs = s.executeQuery("explain " + sql)) {
                assertTrue(rs.next());
                String plan = rs.getString(1);
                assertTrue(plan.contains("OLTP"));
            }
            try(ResultSet rs = s.executeQuery(sql)) {
                assertTrue(rs.next());
                int count = rs.getInt(1);
                assertEquals(1048576, count);
            }
        }
    }
}
