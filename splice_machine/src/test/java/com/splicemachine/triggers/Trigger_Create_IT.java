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
import org.apache.commons.lang3.StringUtils;
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
 * Tests creating/defining triggers.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class Trigger_Create_IT {

    private static final String SCHEMA = Trigger_Create_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table T (a int, b int, c int)");
        classWatcher.executeUpdate("create table R (z int)");
    }

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

    public Trigger_Create_IT(String connecitonString) {
        this.connectionString = connecitonString;
    }

    @Before
    public void createTables() throws Exception {
        triggerDAO.dropAllTriggers(SCHEMA, "T");
        Connection conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setSchema(SCHEMA.toUpperCase());
        methodWatcher.setConnection(conn);
    }

    @Test
    public void create_statementTriggers() throws Exception {
        // given: AFTER STATEMENT
        createTrigger(tb.on("T").named("trig01").after().update().statement().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.on("T").named("trig02").after().delete().statement().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.on("T").named("trig03").after().insert().statement().then("INSERT INTO R VALUES(1)"));
        // given: BEFORE STATEMENT
        createTrigger(tb.on("T").named("trig04").before().update().statement().then("SELECT * FROM sys.systables"));
        createTrigger(tb.on("T").named("trig05").before().delete().statement().then("SELECT * FROM sys.systables"));
        createTrigger(tb.on("T").named("trig06").before().insert().statement().then("SELECT * FROM sys.systables"));

        triggerDAO.assertTriggerExists("trig01", "trig02", "trig03", "trig04", "trig05", "trig06");

        // when - drop trigger
        triggerDAO.drop("trig01", "trig02", "trig03", "trig04", "trig05", "trig06");

        // then - gone
        triggerDAO.assertTriggerGone("trig01", "trig02", "trig03", "trig04", "trig05", "trig06");
    }

    @Test
    public void create_rowTriggers() throws Exception {
        // given: AFTER ROW
        createTrigger(tb.on("T").named("rTrig01").after().update().row().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.on("T").named("rTrig02").after().delete().row().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.on("T").named("rTrig03").after().insert().row().then("INSERT INTO R VALUES(1)"));
        // given: BEFORE ROW
        createTrigger(tb.on("T").named("rTrig04").before().update().row().then("SELECT * FROM sys.systables"));
        createTrigger(tb.on("T").named("rTrig05").before().delete().row().then("SELECT * FROM sys.systables"));
        createTrigger(tb.on("T").named("rTrig06").before().insert().row().then("SELECT * FROM sys.systables"));

        triggerDAO.assertTriggerExists("rTrig01", "rTrig02", "rTrig03", "rTrig04", "rTrig05", "rTrig06");

        // when - drop trigger
        triggerDAO.drop("rTrig01", "rTrig02", "rTrig03", "rTrig04", "rTrig05", "rTrig06");

        // then - gone
        triggerDAO.assertTriggerGone("rTrig01", "rTrig02", "rTrig03", "rTrig04", "rTrig05", "rTrig06");
    }

    @Test
    public void create_rowTriggersReferencing() throws Exception {
        // UPDATE AFTER
        createTrigger(tb.on("T").named("t1").after().update().row().referencing("NEW AS N").then("select N.a from sys.systables"));
        createTrigger(tb.on("T").named("t2").after().update().row().referencing("OLD AS O").then("select O.a from sys.systables"));
        createTrigger(tb.on("T").named("t3").after().update().row().referencing("NEW AS N OLD AS O").then("select N.a,O.a from sys.systables"));
        // UPDATE BEFORE
        createTrigger(tb.on("T").named("t4").before().update().row().referencing("NEW AS N").then("select N.a from sys.systables"));
        createTrigger(tb.on("T").named("t5").before().update().row().referencing("OLD AS O").then("select O.a from sys.systables"));
        createTrigger(tb.on("T").named("t6").before().update().row().referencing("NEW AS N OLD AS O").then("select N.a,O.a from sys.systables"));

        // INSERT AFTER
        createTrigger(tb.on("T").named("t7").after().insert().row().referencing("NEW AS N").then("select N.a from sys.systables"));
        // INSERT BEFORE
        createTrigger(tb.on("T").named("t8").before().insert().row().referencing("NEW AS N").then("select N.a from sys.systables"));

        // DELETE AFTER
        createTrigger(tb.on("T").named("t9").after().delete().row().referencing("OLD AS O").then("select O.a from sys.systables"));
        // DELETE BEFORE
        createTrigger(tb.on("T").named("t10").before().delete().row().referencing("OLD AS O").then("select O.a from sys.systables"));
    }

    @Test
    public void create_illegal_triggerNameTooLong() throws Exception {
        String triggerNameOk = StringUtils.rightPad("trigNameOk",128,'x');
        String triggerNameBad = StringUtils.rightPad("trigNameBad", 129, 'x');
        try {
            createTrigger(tb.on("T").named(triggerNameOk).after().update().statement().then("INSERT INTO R VALUES(1)"));
            createTrigger(tb.on("T").named(triggerNameBad).after().update().statement().then("INSERT INTO R VALUES(1)"));
            fail("didn't expect to be able to crate a trigger with a name longer than 128");
        } catch (SQLException e) {
            assertEquals("The name '" + triggerNameBad.toUpperCase() + "' is too long. The maximum length is '128'.", e.getMessage());
        }
    }

    @Test
    public void create_illegal_statementTriggersCannotHaveReferencingClause() throws Exception {
        verifyTriggerCreateFails(
                tb.on("T").named("t1").before().delete().referencing("NEW AS N").statement().then("select * from sys.systables"),
                "DELETE triggers may only reference OLD transition variables/tables.");
    }

    @Test
    public void create_illegal_beforeTriggersCannotHaveInsertUpdateDeleteActions() throws Exception {
        // BEFORE statement
        verifyTriggerCreateFails(tb.on("T").named("trig").before().update().statement().then("UPDATE R set z=2"),
                "'UPDATE' statements are not allowed in 'BEFORE' triggers.");
        verifyTriggerCreateFails(tb.on("T").named("trig").before().update().statement().then("UPDATE R set z=2"),
                "'UPDATE' statements are not allowed in 'BEFORE' triggers.");
        verifyTriggerCreateFails(tb.on("T").named("trig").before().update().statement().then("INSERT INTO R VALUES(1)"),
                "'INSERT' statements are not allowed in 'BEFORE' triggers.");
        verifyTriggerCreateFails(tb.on("T").named("trig").before().update().statement().then("INSERT INTO R VALUES(1)"),
                "'INSERT' statements are not allowed in 'BEFORE' triggers.");

        // BEFORE row
        verifyTriggerCreateFails(tb.on("T").named("trig").before().delete().row().then("UPDATE R set z=2"),
                "'UPDATE' statements are not allowed in 'BEFORE' triggers.");
        verifyTriggerCreateFails(tb.on("T").named("trig").before().delete().row().then("UPDATE R set z=2"),
                "'UPDATE' statements are not allowed in 'BEFORE' triggers.");
        verifyTriggerCreateFails(tb.on("T").named("trig").before().delete().row().then("INSERT INTO R VALUES(1)"),
                "'INSERT' statements are not allowed in 'BEFORE' triggers.");
        verifyTriggerCreateFails(tb.on("T").named("trig").before().delete().row().then("INSERT INTO R VALUES(1)"),
                "'INSERT' statements are not allowed in 'BEFORE' triggers.");

    }

    @Test
    public void create_illegal_invalidTransitionVariables() throws Exception {
        // INSERT row triggers cannot reference an OLD row
        verifyTriggerCreateFails(tb.on("T").named("t1").before().insert().referencing("OLD as OLD_ROW").row().then("select OLD_ROW.a from sys.systables"),
                "INSERT triggers may only reference new transition variables/tables.");
        verifyTriggerCreateFails(tb.on("T").named("t1").after().insert().referencing("OLD as OLD_ROW").row().then("select OLD_ROW.a from sys.systables"),
                "INSERT triggers may only reference new transition variables/tables.");

        // DELETE row triggers cannot reference a NEW row
        verifyTriggerCreateFails(tb.on("T").named("t1").before().delete().referencing("NEW as NEW_ROW").row().then("select NEW_ROW.a from sys.systables"),
                "DELETE triggers may only reference OLD transition variables/tables.");
        verifyTriggerCreateFails(tb.on("T").named("t1").after().delete().referencing("NEW as NEW_ROW").row().then("select NEW_ROW.a from sys.systables"),
                "DELETE triggers may only reference OLD transition variables/tables.");
    }

    @Test
    public void create_illegal_triggersNotAllowedOnSystemTables() throws Exception {
        // Cannot create triggers on sys tables.
        verifyTriggerCreateFails(tb.on("SYS.SYSTABLES").named("trig").before().delete().row().then("select * from sys.systables"),
                "'CREATE TRIGGER' is not allowed on the System table '\"SYS\".\"SYSTABLES\"'.");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void createTrigger(TriggerBuilder tb) throws Exception {
        methodWatcher.executeUpdate(tb.build());
    }

    private void verifyTriggerCreateFails(TriggerBuilder tb, String expectedError) throws Exception {
        try {
            createTrigger(tb);
            fail("expected trigger creation to fail for=" + tb.build());
        } catch (Exception e) {
            assertEquals(expectedError, e.getMessage());
        }

    }
}
