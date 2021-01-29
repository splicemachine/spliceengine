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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import com.splicemachine.utils.Pair;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.sql.*;
import java.util.*;

import static com.splicemachine.derby.impl.sql.execute.operations.DropAddColumnIT.ColType.INT;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.format;

@Category(value = {SerialTest.class})
public class DropAddColumnIT {

    enum ColType{
        INT("INT"),
        VARCAHR("VARCHAR(10)"),
        DECIMAL("DECIMAL(10, 2)"),
        TIMESTAMP("TIMESTAMP");

        ColType(String sql) {
            this.sql = sql;
        }

        String sql;
    }

    enum ConstraintType {
        PRIMARY_KEY("PRIMARY KEY"),
        UNIQUE("UNIQUE");

        ConstraintType(String sql) {
            this.sql = sql;
        }

        String sql;
    }

    private static class Col extends Pair<String,ColType> {

        public Col(String a, ColType b) {
            super(a,b);
        }

        @Override
        public String toString() {
            return format("%s %s", first, second.sql);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if(!(other instanceof Col)) {
                return false;
            }
            return first.equals(((Col)other).first);
        }
    }

    static class Harness {

        private final List<Table> tables;
        private final TestConnection connection;

        private StringBuilder ddl;
        private String tableName;
        private String firstCol;

        @SafeVarargs
        private static <T> String tupleWithParenthesis(T... items) {
            assert items.length > 0;
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            sb.append(tuple(items));
            sb.append(")");
            return sb.toString();
        }

        @SafeVarargs
        private static <T> String tuple(T... items) {
            assert items.length > 0;
            StringBuilder sb = new StringBuilder();
            for(T item : items) {
                sb.append(item);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }

        Harness(TestConnection connection) {
            this.connection = connection;
            tables = new ArrayList<>();
            ddl = new StringBuilder();
        }

        private Harness createTable(String name, Col... columns) throws Exception {
            assert Arrays.stream(columns).distinct().count() == columns.length;
            assert tables.stream().noneMatch(t -> t.name.equals(name));
            assert columns.length > 1;
            ddl.setLength(0);
            tableName = name;
            ddl.append(format("CREATE TABLE (%s, %s", name, tuple(columns))); // finish the statement with calling begin.
            firstCol = columns[0].getFirst();
            return this;
        }

        private Harness createTable(String name, String... columns) throws Exception {
            assert tables.stream().noneMatch(t -> t.name.equals(name));
            assert columns.length > 1;
            ddl.setLength(0);
            tableName = name;
            ddl.append(format("CREATE TABLE %s (%s", name, tuple(Arrays.stream(columns).map(c -> new Col(c, INT)).toArray()))); // finish the statement with calling begin.
            firstCol = columns[0];
            return this;
        }

        Harness withConstraint(ConstraintType constraintType, String name, String... cols) throws SQLException {
            assert ddl.length() > 0;
            assert tableName != null && !tableName.isEmpty();
            ddl.append(format(", CONSTRAINT %s%s", name, constraintType.sql, tupleWithParenthesis(cols)));
            return this;
        }

        Table begin() throws SQLException {
            assert ddl.length() > 0;
            ddl.append(")");
            try (Statement statement = connection.createStatement()) {
                statement.execute(ddl.toString());
            }
            assert tableName != null && !tableName.isEmpty();
            Table result = new Table(this, tableName, firstCol);
            tables.add(result);
            return result;
        }

        void finish() throws SQLException {
            for(Table table : tables) {
                connection.execute(format("drop table %s", table.name));
            }
        }

        static class Table {
            final TestConnection connection;
            final String name;
            private final Harness harness;
            private final String firstCol;

            Table(Harness harness, String name, String firstCol /*for sorting results*/) {
                this.harness = harness;
                this.connection = harness.connection;
                this.name = name;
                this.firstCol = firstCol;
            }

            Table addIndex(String name, String... cols) throws SQLException {
                try(Statement statement = connection.createStatement()) {
                    statement.execute(format("CREATE INDEX %s on %s%s", name, this.name, tupleWithParenthesis(cols)));
                }
                return this;
            }

            private Table when() { return this; } // lexical sugar
            private Table and() { return this; } // lexical sugar
            private Table then() { return this; } // lexical sugar

            Table addConstraint(ConstraintType constraintType, String name, String... cols) throws SQLException {
                try(Statement statement = connection.createStatement()) {
                    statement.execute(format("ALTER TABLE %s ADD CONSTRAINT %s%s", name, constraintType.sql, tupleWithParenthesis(cols)));
                }
                return this;
            }

            Table dropConstraint(String name) throws SQLException {
                try(Statement statement = connection.createStatement()) {
                    statement.execute(format("ALTER TABLE %s DROP CONSTRAINT %s", this.name, name));
                }
                return this;
            }

            Table dropColumn(String col) throws SQLException {
                try(Statement statement = connection.createStatement()) {
                    statement.execute(format("ALTER TABLE %s DROP COLUMN %s", name, col));
                }
                return this;
            }

            Table addColumn(String col, ColType type) throws SQLException {
                try(Statement statement = connection.createStatement()) {
                    statement.execute(format("ALTER TABLE %s ADD COLUMN %s %s", name, col, type.sql));
                }
                return this;
            }

            Table addRow(Integer... values) throws SQLException {
                try(Statement statement = connection.createStatement()) {
                    statement.execute(format("INSERT INTO %s values %s", name, tupleWithParenthesis(values)));
                }
                return this;
            }

            Table shouldContain(int[][] rows) throws Exception {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery(String.format("select * from %s order by %s asc", name, firstCol))) {
                        List<int[]> list = Arrays.asList(rows);
                        Collections.sort(list, Comparator.comparing(o -> o[0])); // order by col1
                        for (int[] row : list) {
                            Assert.assertTrue(resultSet.next());
                            int cnt = 1;
                            for (Integer col : row) {
                                Assert.assertEquals((int) col, resultSet.getInt(cnt++));
                            }
                        }
                        Assert.assertFalse(resultSet.next());
                    }
                }
                return this;
            }

            Harness done() {
                return harness;
            }
        }
    }

    private static final String SCHEMA = DropAddColumnIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher  spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private TestConnection connection = spliceClassWatcher.getOrCreateConnection();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);


    @Test
    public void dropColumnWorks() throws Exception {
        Harness harness = new Harness(connection);

        harness.createTable("t1", "c1", "c2", "c3", "c4")
                .begin()
                .addRow(10, 210, 30, 40)
                .addRow(100, 2100, 300, 400)
                .shouldContain(new int[][]{{10, 210, 30, 40}, {100, 2100, 300, 400}})
                .dropColumn("c2")
                .shouldContain(new int[][]{{10, 30, 40}, {100, 300, 400}})
                .addIndex("IDX1", "c1", "c3")
                .dropColumn("c4")
                .shouldContain(new int[][]{{10, 30}, {100, 300}})
                .done();
                //.finish();
    }
}

