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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 *
 *
 */
@RunWith(Parameterized.class)
public class JoinOperationCharJoinPredicateIT extends SpliceUnitTest {
    private static final String SCHEMA = JoinOperationCharJoinPredicateIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static String[] JOINS = {"NESTEDLOOP", "SORTMERGE", "BROADCAST"};
    private static String[] USE_SPARK = {"false", "true"};
    private static boolean[] USE_INDEX = {true, false};
    private static int[] CHAR_SIZES = {3,5};
    private static String[] CHAR_TYPES = {"CHAR"}; //, "VARCHAR"}; // VARBIT?

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(192);
        for (String joinAlgo: JOINS) {
            for (String useSpark: USE_SPARK) {
                for (boolean useIndex: USE_INDEX) {
                    for (String leftType: CHAR_TYPES) {
                        for (int leftSize: CHAR_SIZES) {
                            for (String rightType: CHAR_TYPES) {
                                for (int rightSize : CHAR_SIZES) {
                                    params.add(new Object[] {
                                            joinAlgo,
                                            useSpark,
                                            useIndex,
                                            getLeftTableName(leftType, leftSize),
                                            getRightTableName(rightType, rightSize, useIndex)});
                                }
                            }
                        }
                    }
                }
            }
        }
        return params;
    }

    private String joinStrategy;
    private String useSparkString;
    private boolean useIndex;
    private String leftTable;
    private String rightTable;

    public JoinOperationCharJoinPredicateIT(String joinStrategy, String useSparkString, boolean useIndex, String leftTable, String rightTable) {
        this.joinStrategy = joinStrategy;
        this.useSparkString = useSparkString;
        this.useIndex = useIndex;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private static String getTableName(String type, int size, boolean index) {
        StringBuilder sb = new StringBuilder();
        sb.append(type);
        sb.append("_");
        sb.append(size);
        if (index) {
            sb.append("_INDEXED");
        }
        return sb.toString();
    }

    private static String getLeftTableName(String type, int size) {
        return "LEFT_" + getTableName(type, size, false);
    }

    private static String getRightTableName(String type, int size, boolean index) {
        return "RIGHT_" + getTableName(type, size, index);
    }

    private static String getIndexName(String tableName) {
        return "INDEX_" + tableName;
    }

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        for (String type: CHAR_TYPES) {
            for (int size: CHAR_SIZES) {
                for (boolean index: USE_INDEX) {
                    for (boolean left: new boolean[] {true, false}) {
                        if (index && left) {
                            continue;
                        }
                        String tableName = left? getLeftTableName(type, size): getRightTableName(type, size, index);
                        TableCreator t = new TableCreator(connection)
                                .withCreate(String.format("create table %s (a %s(%s))", tableName, type, size))
                                .withInsert(String.format("insert into %s values(?)", tableName));
                        if (index) {
                            t.withIndex(String.format("create index %s on %s(a)", getIndexName(tableName), tableName));
                        }
                        t.withRows(rows(row("1"), row("2"), row("3"), row("4"), row("5"))).create();
                    }
                }
            }
        }
    }

    @Test
    public void testJoin() throws Exception {
        String query = String.format(
                "select count(*) from --splice-properties joinOrder=fixed\n" +
                        "%s,\n" +
                        "%s --splice-properties joinStrategy=%s, useSpark=%s, index=%s\n" +
                        "where %s.a = %s.a",
                leftTable,
                rightTable, joinStrategy, useSparkString, useIndex ? getIndexName(rightTable): "NULL",
                leftTable, rightTable);
        ResultSet rs = methodWatcher.executeQuery(query);
        rs.next();
        assertEquals(String.format("Missing Records for %s", query), 5, rs.getInt(1));
    }

}
