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

package com.splicemachine.triggers;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests that triggers are dropped when the table or column(s) they depend on are dropped.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class Trigger_Dependency_IT {

    private static final String SCHEMA = Trigger_Dependency_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private TriggerBuilder tb = new TriggerBuilder();
    private TriggerDAO triggerDAO = new TriggerDAO(methodWatcher.getOrCreateConnection());

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"});
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
        return params;
    }

    private String connectionString;

    public Trigger_Dependency_IT(String connecitonString) {
        this.connectionString = connecitonString;
    }

    @Before
    public void createTables() throws Exception {
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, "R", "T");
        methodWatcher.executeUpdate("create table T (a int, b int, c int)");
        methodWatcher.executeUpdate("create table R (z int)");
        Connection conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setSchema(SCHEMA.toUpperCase());
        methodWatcher.setConnection(conn);
    }

    @Test
    public void dropTableDropsTriggers() throws Exception {
        // given
        createTrigger(tb.named("trig1").after().update().of("a").on("T").row().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.named("trig2").after().update().of("b").on("T").row().then("INSERT INTO R VALUES(2)"));
        triggerDAO.assertTriggerExists("trig1", "trig2");

        // when - drop table
        methodWatcher.executeUpdate("DROP TABLE T");

        // then - triggers gone
        triggerDAO.assertTriggerGone("trig1");
        triggerDAO.assertTriggerGone("trig2");
    }

    @Test
    public void dropColumnDropsTrigger() throws Exception {
        // given
        createTrigger(tb.named("trig1").after().update().of("a").on("T").statement().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.named("trig2").after().update().of("b").on("T").statement().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.named("trig3").after().update().of("c").on("T").statement().then("INSERT INTO R VALUES(1)"));

        // when - drop column b
        methodWatcher.executeUpdate("ALTER TABLE T DROP COLUMN b");

        // then - trigger2 is gone, but others are still there.
        triggerDAO.assertTriggerGone("trig2");
        triggerDAO.assertTriggerExists("trig1", "trig3");
    }

    @Test
    public void dropTableReferencedFromTriggerActionInvalidatesButDoesNotDropTrigger() throws Exception {
        // given
        createTrigger(tb.named("trig1").after().insert().on("T").statement().then("INSERT INTO R VALUES(1)"));

        // when - drop action-referenced table
        methodWatcher.executeUpdate("DROP TABLE R");

        // then - trigger still exists
        triggerDAO.assertTriggerExists("trig1");

        // but trigger will not fire
        try {
            methodWatcher.executeUpdate("insert into T values(1,1,1)");
            fail("expected insert statement to fail because related trigger should fail");
        } catch (SQLException e) {
            assertEquals("Table/View 'R' does not exist.", e.getMessage());
        }
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void createTrigger(TriggerBuilder tb) throws Exception {
        methodWatcher.executeUpdate(tb.build());
    }
}
