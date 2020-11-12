/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ForeignKeyGraphIT {

    private static final String SCHEMA = ForeignKeyGraphIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceWatcher watcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    static int counter = 0;
    private static String generateName() {
        return "T" + counter++;
    }

    enum Action { NO_ACTION, SET_NULL, CASCADE, RESTRICT };

    static class DDLGenerator {
        List<String> tables = null;
        List<String> ddls = null;
        String constraintName = null;

        public DDLGenerator(int count) {
            tables = new ArrayList<>(count);
            ddls = new ArrayList<>();
            for(int i=0; i<count; ++i) {
                String tableName = generateName();
                tables.add(tableName);
                ddls.add(String.format("CREATE TABLE %s(C1 int primary key, C2 int)", tableName));
            }
        }

        private static String toString(Action action) {
            switch(action) {
                case NO_ACTION:
                    return "NO ACTION";
                case SET_NULL:
                    return "SET NULL";
                case CASCADE:
                    return "CASCADE";
                case RESTRICT:
                    return "RESTRICT";
                default:
                    assert false; // should not happen.
            }
            return ""; // make compiler happy.
        }

        private String generateConstraintDdl(int child, int parent, Action action) {
            assert child >= 0 && child < tables.size();
            assert parent >= 0 && parent < tables.size();
            constraintName = generateName();
            return String.format("ALTER TABLE %s ADD CONSTRAINT \"%s\" FOREIGN KEY (C2) REFERENCES %s(C1) ON DELETE %s",
                                 tables.get(child), constraintName, tables.get(parent), toString(action));
        }

        public DDLGenerator link(int child, int parent, Action action) {
            ddls.add(generateConstraintDdl(child, parent, action));
            return this;
        }

        public DDLGenerator execute() throws Exception {
            for(String ddl : ddls) {
                watcher.executeUpdate(ddl);
            }
            return this;
        }

        public DDLGenerator shouldFail(int child, int parent, Action action) {
            String ddl = generateConstraintDdl(child, parent, action);
            try {
                watcher.executeUpdate(ddl);
                Assert.fail("Adding foreign key should have failed with an error message");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
                SQLException sqlException = (SQLException) e;
                Assert.assertTrue(sqlException.getMessage().contains("adding this foreign key leads to the conflicting delete actions on the table"));
            }
            return this;
        }

        public DDLGenerator shouldSucceed(int child, int parent, Action action) throws Exception {
            String ddl = generateConstraintDdl(child, parent, action);
            watcher.executeUpdate(ddl); // consumer is responsible for reacting to this.
            return this;
        }
    }

    @Test
    public void invalidCase1() throws Exception {
        DDLGenerator ddlGenerator = new DDLGenerator(4);
        ddlGenerator
                .link(1, 0, Action.CASCADE)
                .link(2, 0, Action.CASCADE)
                .link(3, 1, Action.SET_NULL)
                .execute()
                .shouldFail(3, 2, Action.CASCADE);
    }

    @Test
    public void invalidCase2() throws Exception {
        DDLGenerator ddlGenerator = new DDLGenerator(4);
        ddlGenerator
                .link(1, 0, Action.CASCADE)
                .link(2, 0, Action.CASCADE)
                .link(3, 1, Action.CASCADE)
                .execute()
                .shouldFail(3, 2, Action.NO_ACTION);
    }

    @Test
    public void validCase1() throws Exception {
        DDLGenerator ddlGenerator = new DDLGenerator(4);
        ddlGenerator
                .link(2, 0, Action.SET_NULL)
                .link(1, 0, Action.SET_NULL)
                .link(1, 2, Action.SET_NULL)
                .execute()
                .shouldSucceed(2, 3, Action.CASCADE);
    }

    @Test // the FK analysis algorithm rules this case out incorrectly, it seems to require a substantial rewrite to make it work properly.
    public void validCase2() throws Exception {
        DDLGenerator ddlGenerator = new DDLGenerator(4);
        ddlGenerator
                .link(1, 0, Action.SET_NULL)
                .link(2, 1, Action.SET_NULL)
                .link(2, 0, Action.CASCADE)
                .execute()
                .shouldSucceed(1, 3, Action.CASCADE);
    }

    /* DB-10545 */
    @Test
    public void validCase3() throws Exception {
        DDLGenerator ddlGenerator = new DDLGenerator(4);
        ddlGenerator
                .link(1, 0, Action.SET_NULL)
                .link(2, 0, Action.CASCADE)
                .link(2, 1, Action.RESTRICT)
                .execute()
                .shouldSucceed(1, 3, Action.CASCADE);
    }

    @Test
    public void validCase4_1() throws Exception {
        DDLGenerator ddlGenerator = new DDLGenerator(6);
        ddlGenerator
                .link(1, 0, Action.CASCADE)
                .link(4, 0, Action.CASCADE)
                .link(2, 1, Action.SET_NULL)
                .link(3, 2, Action.CASCADE)
                .execute()
                .shouldSucceed(5,3, Action.CASCADE)
                .shouldSucceed(5,4, Action.RESTRICT);
    }

    @Test
    public void validCase4_2() throws Exception {
        DDLGenerator ddlGenerator = new DDLGenerator(6);
        ddlGenerator
                .link(1, 0, Action.CASCADE)
                .link(4, 0, Action.CASCADE)
                .link(2, 1, Action.SET_NULL)
                .link(3, 2, Action.CASCADE)
                .execute()
                .shouldSucceed(5,4, Action.RESTRICT)
                .shouldSucceed(5,3, Action.CASCADE);
    }
}