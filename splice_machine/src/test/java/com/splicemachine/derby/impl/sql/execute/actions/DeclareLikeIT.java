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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.*;
import java.util.Collection;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class DeclareLikeIT extends SpliceUnitTest {
    public static final String CLASS_NAME = DeclareLikeIT.class.getSimpleName().toUpperCase();
    private static final String schema = CLASS_NAME;
    private static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(schema);

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(schemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }

    private boolean temporary;

    public DeclareLikeIT(boolean temporary) {
        this.temporary = temporary;
    }

    /*
     * Helper function to strip header, divider, and create table line
     * to be able to compare SHOW CREATE TABLE from two different tables
     *
     * Example:
     * DDL                                        |
     *  ------------------------------------------------------------------------------------
     * CREATE LOCAL TEMPORARY TABLE "DECLARELIKEIT"."SIMPLE_TABLE_TEMP" (
     * "A" INTEGER
     * ) ; |
     *
     * would return
     * "A" INTEGER
     * ) ; |
     *
     */
    private String stripHeaderAndFooter(String s) {
        String[] lines = s.split("\\r?\\n");
        int begin = 3;
        int end = lines.length - 1;
        if (lines[end].equals(") ; |")) {
            end = end -1;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = begin; i <= end; ++i) {
            sb.append(lines[i]).append("\n");
        }
        return sb.toString();
    }

    private String removeConstraints(String s) {
        String[] lines = s.split("\\r?\\n");
        StringBuilder sb = new StringBuilder();
        for (String line: lines) {
            if (!line.startsWith(", CONSTRAINT")) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString();
    }

    private String baseTableName(String name) {
        return name + "_base";
    }

    private String likeTableName(String name) {
        return name + "_like" + (temporary ? "_temp" : "");
    }

    private void createBaseAndDeclareLikeLikeBase(String name, String definition) throws Exception {
        methodWatcher.executeUpdate(format("drop table %s.%s if exists", schema, baseTableName(name)));
        methodWatcher.executeUpdate(format("create table %s.%s %s", schema, baseTableName(name), definition));
        methodWatcher.executeUpdate(format("drop table %s.%s if exists", schema, likeTableName(name)));
        if (temporary)
            methodWatcher.executeUpdate(format("DECLARE GLOBAL TEMPORARY TABLE %s.%s LIKE %s.%s", schema, likeTableName(name), schema, baseTableName(name)));
        else
            methodWatcher.executeUpdate(format("CREATE TABLE %s.%s LIKE %s.%s", schema, likeTableName(name), schema, baseTableName(name)));
    }

    private String show(String name) throws Exception {
        try (ResultSet rs = methodWatcher.executeQuery(format("CALL SYSCS_UTIL.SHOW_CREATE_TABLE('%s', '%s')", schema, name))) {
            return TestUtils.FormattedResult.ResultFactory.toString(rs);
        }
    }

    private String showBase(String name) throws Exception {
        return show(baseTableName(name));
    }

    private String showLike(String name) throws Exception {
        return show(likeTableName(name));
    }

    private void testIdentical(String name, String definition) throws Exception {
        createBaseAndDeclareLikeLikeBase(name, definition);
        Assert.assertEquals(stripHeaderAndFooter(showBase(name)), stripHeaderAndFooter(showLike(name)));
    }

    private void testConstraintsRemoved(String name, String definition) throws Exception {
        createBaseAndDeclareLikeLikeBase(name, definition);
        Assert.assertEquals(
                removeConstraints(stripHeaderAndFooter(showBase(name))),
                stripHeaderAndFooter(showLike(name))
                );
    }

    @Test
    public void testSimpleTable() throws Exception {
        testIdentical("simple_table", "(a int)");
    }

    @Test
    public void testNotNull() throws Exception {
        testIdentical("NOT_NULL", "(a int not null)");
    }

    @Test
    public void testDefault() throws Exception {
        testIdentical("DEFAULT_NOT_NULL", "(a int not null with default 4)");
        testIdentical("DEFAULT_NULL", "(a int with default 4)");
    }

    @Test
    public void testEmptyDefault() throws Exception {
        testIdentical("EMPTY_DEFAULT", "(a int not null with default)");
        testIdentical("EMPTY_DEFAULT2", "(a timestamp not null with default)");
    }

    @Test
    public void testMultipleColumns() throws Exception {
        testIdentical("MULTIPLE_COLUMNS", "(a int, b int not null, c int not null with default 4, d int with default 5)");
    }

    @Test
    public void testPrimaryKey() throws Exception {
        // The new table does not have a primary key
        testConstraintsRemoved("PRIMARY_KEY", "(a int not null primary key)");
        testConstraintsRemoved("PRIMARY_KEY2", "(a int not null, b int not null, primary key(a, b))");
    }
}
