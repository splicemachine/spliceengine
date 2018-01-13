/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.jdbc;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.util.StatementUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Ordering;
import org.spark_project.guava.collect.Sets;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.resultSetSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JdbcApiIT {

    private static final String CLASS_NAME = JdbcApiIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static final SpliceTableWatcher A_TABLE = new SpliceTableWatcher("A",schemaWatcher.schemaName,
            "(a1 int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(A_TABLE)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.execute("insert into a values 1,2");
                        try (PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into a select a1 + (select count(*) from a) from a")) {
                            for (int i = 0; i < 10; i++) {
                                ps.execute();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private TestConnection conn;

    @Before
    public void setUp() throws Exception{
        conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
        conn.reset();
    }

    @Rule
    public Timeout globalTimeout= new Timeout(15, TimeUnit.SECONDS);


    @Test(expected = SQLTimeoutException.class)
    public void testTimeoutSpark() throws Exception {
        String sql = "select count(*) from a --splice-properties useSpark=true \n" +
                "natural join a a1 natural join a a2 natural join a a3";
        try(Statement s = conn.createStatement()){
            s.setQueryTimeout(2);
            ResultSet rs = s.executeQuery(sql);
        }
    }

    @Test(expected = SQLTimeoutException.class)
    public void testTimeoutControl() throws Exception {
        String sql = "select count(*) from a --splice-properties useSpark=false \n" +
                "natural join a a1 natural join a a2 natural join a a3";
        try(Statement s = conn.createStatement()){
            s.setQueryTimeout(2);
            ResultSet rs = s.executeQuery(sql);
        }
    }

}
