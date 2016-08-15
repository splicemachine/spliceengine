/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.apache.ddlutils.testutils.TestUtils.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.model.TableType;
import org.apache.ddlutils.task.WriteSchemaSqlToFileCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test dump DDL, create and drop from running Splice instance.
 */
public class DumpSpliceIT {

    private static BasicDataSource dataSource;

    @BeforeClass
    public static void beforeClass() throws Exception {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(DRIVERCLASSNAME);
        dataSource.setUrl(URL);
        dataSource.setUsername(USERNAME);
        dataSource.setPassword(PASSWORD);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    //============================================================================================

    /**
     * Test that reads all schemas in DB and prints create SQL to STDOUT.
     * @throws Exception
     */
    @Test
    @Ignore("Test manually on whole splice DB after ITs run. Takes 8 min to run.")
    public void testSpliceIT_DropCreateSQL() throws Exception {
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        Database db = platform.readModelFromDatabase("splice_IT_model");

        WriteSchemaSqlToFileCommand toSqlFile = new WriteSchemaSqlToFileCommand();
        toSqlFile.setPlatform(platform);
        toSqlFile.setOutputFile(getOutputFile("splice_IT_model.sql"));
        toSqlFile.setAlterDatabase(false);
        toSqlFile.setDoDrops(true);
        toSqlFile.execute(null, db);
    }

    @Test
    public void testGenSQLToFile() throws Exception {
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        Database db = null;
        try (Connection connection = dataSource.getConnection()) {
            try {
                // set up
                setup(getInputFileAsResource("DUMP_DDL_MULTI_SCHEMA.sql", getClass()), platform, connection);

                // read model
                db = platform.readModelFromDatabase("da-splish", null, "DUMP_DDL_MULTI%", null);

                // validate model
                assertEquals("Unexpected number of schemas.", 11, db.getSchemaCount());
                assertCountTableType(db, 90, TableType.TABLE);

                // write the model create SQL
                WriteSchemaSqlToFileCommand toSqlFile = new WriteSchemaSqlToFileCommand();
                toSqlFile.setPlatform(platform);
                toSqlFile.setOutputFile(getOutputFile("compare-sql.sql"));
                toSqlFile.setAlterDatabase(false);
                toSqlFile.setDoDrops(false);
                toSqlFile.execute(null, db);

                // See javadoc comments for caveats using this utility
                assertFilesCompare(getInputFileAsResource("DUMP_DDL_MULTI_SCHEMA.sql", getClass()), getOutputFile("compare-sql.sql"));
            } finally {
                if (db != null) {
                    platform.dropModel(connection, db, true);
                }
            }
            assertSchemaDropped("DUMP_DDL_MULTI%", connection);
        }
    }

    @Test
    public void testGenSQLWithViews() throws Exception {
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        Database db = null;
        try (Connection connection = dataSource.getConnection()) {
            try {
                // set up
                setup(getInputFileAsResource("DUMP_DDL_VIEWS.sql", getClass()), platform, connection);
                // read model
                db = platform.readModelFromDatabase("dump-views", null, "DUMP_DDL_VIEWS%", null);
                String sql = platform.getCreateModelSql(db, false, true, true);
                assertTrue("No VIEWS!", sql.contains("CREATE VIEW"));
            } finally {
                if (db != null) {
                    platform.dropModel(connection, db, true);
                }
            }
            assertSchemaDropped("DUMP_DDL_VIEWS%", connection);
        }
    }

    @Test
    public void testRoundTripWithViews() throws Exception {
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        Database db = null;
        try (Connection connection = dataSource.getConnection()) {
            try {
                // set up
                setup(getInputFileAsResource("DUMP_DDL_VIEWS.sql", getClass()), platform, connection);
                // read model
                db = platform.readModelFromDatabase("dump-views", null, "DUMP_DDL_VIEWS%", null);
                Schema schema = db.findSchema("DUMP_DDL_VIEWS");
                assertNotNull("Can't find schema 'DUMP_DDL_VIEWS', in model '"+db.getName()+"'", schema);
                assertCountTableType(schema, 9, TableType.TABLE);
                assertCountTableType(schema, 1, TableType.VIEW);
            } finally {
                if (db != null) {
                    platform.dropModel(connection, db, true);
                }
            }
            assertSchemaDropped("DUMP_DDL_VIEWS%", connection);
        }
    }

    @Test
    public void testViewsDrop() throws Exception {
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        Database db = null;
        try (Connection connection = dataSource.getConnection()) {
            try {
                setup(getInputFileAsResource("DUMP_DDL_VIEWS.sql", getClass()), platform, connection);
                // read model
                db = platform.readModelFromDatabase("dump-views", null, "DUMP_DDL_VIEWS%", null);
                String sql = platform.getDropModelSql(db);
                assertCountString("DROP SCHEMA", sql, 2);
                assertCountString("DROP VIEW", sql, 1);
                assertCountString("DROP TABLE", sql, 10);
            } finally {
                if (db != null) {
                    platform.dropModel(connection, db, true);
                }
            }
            assertSchemaDropped("DUMP_DDL_VIEWS%", connection);
        }
    }

    @Test
    public void testSqlAuthDump() throws Exception {
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        Database db = null;
        try (Connection connection = dataSource.getConnection()) {
            try {
                setup(getInputFileAsResource("roundtrip_auth.sql", getClass()), platform, connection);
                // read model
                db = platform.readModelFromDatabase("dump-auth", null, "AUTH_%", null);
                String sql = platform.getCreateModelSql(db, true, true, true);
                assertNotNull("Didn't find user Joe", db.findUser("Joe"));
                assertNotNull("Didn't find user FRED", db.findUser("FRED"));
                assertCountString("DROP SCHEMA", sql, 2);
                assertCountString("DROP VIEW", sql, 1);
                assertCountString("DROP TABLE", sql, 3);
                assertCountString("CREATE ROLE ROLE1", sql, 1);
                assertCountString("CREATE ROLE ROLE2", sql, 1);
                assertCountString("GRANT ROLE1 TO ROLE2", sql, 1);
                assertCountString("GRANT DELETE, INSERT, SELECT ON AUTH_FRED.ONE_INT_TABLE TO ROLE1", sql, 1);
            } finally {
                if (db != null) {
                    platform.dropModel(connection, db, true);
                }
            }
            assertSchemaDropped("AUTH_%", connection);
        }
    }

    @Test
    @Ignore("No triggers yet")
    public void testSqlSchemaTriggersQuery() throws Exception {

        // get triggers
        String query = String.format(
            "select a.TRIGGERNAME from SYS.SYSTRIGGERS a inner join SYS.SYSSCHEMAS b on a.SCHEMAID = b.SCHEMAID where b" +
                ".SCHEMANAME = '%s' ORDER BY a.TRIGGERNAME", "%");

        try (Connection connection = dataSource.getConnection()) {
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery(query)) {
                    List<String> triggers = new ArrayList<>();
                    while (rs.next()) {
                        triggers.add(rs.getString(1));
                    }
                    Collections.sort(triggers);
                    for (String line : triggers) {
                        System.out.println(line);
                    }
                }
            }
        }
    }

    @Test
    @Ignore
    public void roleImpact() throws Exception {
        // TODO: for testing implementation only. delete
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        try (Connection connection = platform.borrowConnection()) {
            System.out.println("* create user");
            printSysTableImpact(platform, "SYS", "CALL SYSCS_UTIL.SYSCS_CREATE_USER('Ted','')");

            System.out.println("* create user");
            printSysTableImpact(platform, "SYS", "CALL SYSCS_UTIL.SYSCS_CREATE_USER('Joe','')");

            System.out.println("* create role");
            printSysTableImpact(platform, "SYS", "Create ROLE funny_role");
            execQuery(connection, "select * from sys.sysroles");

            System.out.println("* grant role");
            printSysTableImpact(platform, "SYS", "GRANT funny_role to Ted");
            execQuery(connection, "select * from sys.sysroles");

            System.out.println("* grant role");
            printSysTableImpact(platform, "SYS", "GRANT funny_role to Joe");
            execQuery(connection, "select * from sys.sysroles");

            System.out.println("* revoke role");
            printSysTableImpact(platform, "SYS", "revoke funny_role from Ted");
        } finally {
            // clean up
            try (Connection connection = platform.borrowConnection()) {
                exec(connection, "drop role funny_role");
                exec(connection, "CALL SYSCS_UTIL.SYSCS_DROP_USER('Ted')");
                exec(connection, "CALL SYSCS_UTIL.SYSCS_DROP_USER('Joe')");
            }
        }
    }

    @Test
    @Ignore
    public void permissionImpact() throws Exception {
        // TODO: for testing implementation only. delete
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        try (Connection connection = platform.borrowConnection()) {
            System.out.println("* create user");
            exec(connection, "CALL SYSCS_UTIL.SYSCS_CREATE_USER('Ted','')");
            exec(connection, "create table TedsTable(x int, y int)");

            System.out.println("* create user");
            exec(connection, "CALL SYSCS_UTIL.SYSCS_CREATE_USER('Joe','')");
            exec(connection, "create table JoesTable(x int, y int)");

            System.out.println("* create user");
            exec(connection, "CALL SYSCS_UTIL.SYSCS_CREATE_USER('Jean','')");

            System.out.println("* create permission");
            printSysTableImpact(platform, "SYS", "grant update on TedsTable to Joe, Ted");
            execQuery(connection, "select * from sys.SYSTABLEPERMS");
            printSysTableImpact(platform, "SYS", "grant select on TedsTable to Jean");
            execQuery(connection, "select * from sys.SYSTABLEPERMS");
            execQuery(connection, "select a.tablepermsid, a.GRANTEE, a.GRANTOR, a.SELECTPRIV, a.DELETEPRIV, a.INSERTPRIV, a.UPDATEPRIV, a.REFERENCESPRIV, a.TRIGGERPRIV, c.schemaname, b.tablename from sys.SYSTABLEPERMS a, sys.SYSTABLES b, sys.sysschemas c where a.tableID = b.tableID and b.schemaid = c.schemaid");
        } finally {
            // clean up
            try (Connection connection = platform.borrowConnection()) {
                exec(connection, "drop table if exists TedsTable");
                exec(connection, "CALL SYSCS_UTIL.SYSCS_DROP_USER('Ted')");
                exec(connection, "drop table if exists JoesTable");
                exec(connection, "CALL SYSCS_UTIL.SYSCS_DROP_USER('Joe')");
                exec(connection, "CALL SYSCS_UTIL.SYSCS_DROP_USER('Jean')");
            }
        }

    }
 }
