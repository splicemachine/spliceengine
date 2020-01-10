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

package com.splicemachine.derby.impl.sql.catalog;

import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TableDAO;

/**
 * Test the functionality of SYSIBM.SYSDUMMY1 table.
 */
public class TheSysDummyIT {

    private static final String SCHEMA = TheSysDummyIT.class.getSimpleName().toUpperCase();
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testInsertIntoSelect() throws Exception {
        String tableName = "A";
        TableDAO td = new TableDAO(methodWatcher.getOrCreateConnection());
        td.drop(SCHEMA, tableName);

        methodWatcher.getOrCreateConnection().createStatement().executeUpdate(
            String.format("create table %s ",SCHEMA+"."+tableName)+"(i int, b bigint, v varchar(5), c char(1))");

        methodWatcher.getOrCreateConnection().createStatement().
            executeUpdate(String.format("insert into %s (i,b,v,c) select 1,2,'a','b' from sysibm.sysdummy1", SCHEMA+"."+tableName)) ;

        String sqlText = String.format("select * from %s", SCHEMA+"."+tableName);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "I | B | V | C |\n" +
                "----------------\n" +
                " 1 | 2 | a | b |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();


    }

    @Test
    public void testInsertIntoSelectDate() throws Exception {
        String tableName = "V_MAX_DATE";
        TableDAO td = new TableDAO(methodWatcher.getOrCreateConnection());
        td.drop(SCHEMA, tableName);

        methodWatcher.getOrCreateConnection().createStatement().executeUpdate(
            String.format("create table %s ",SCHEMA+"."+tableName)+"(date DATE)");

        methodWatcher.getOrCreateConnection().createStatement().
            executeUpdate(String.format("INSERT INTO %s SELECT LAST_DAY(DATE('2016-01-04')) + 1 FROM SYSIBM.SYSDUMMY1",
                                        SCHEMA+"."+tableName)) ;

        String sqlText = String.format("select * from %s", SCHEMA+"."+tableName);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "DATE    |\n" +
                "------------\n" +
                "2016-02-01 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelectDate() throws Exception {
        String sqlText = "SELECT DATE('2016-01-04') FROM SYSIBM.SYSDUMMY1";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "1     |\n" +
                "------------\n" +
                "2016-01-04 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelectAdd() throws Exception {
        String sqlText = "SELECT 1+1 FROM SYSIBM.SYSDUMMY1";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "1 |\n" +
                "----\n" +
                " 2 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelectChar() throws Exception {
        String sqlText = "SELECT '1' FROM SYSIBM.SYSDUMMY1";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "1 |\n" +
                "----\n" +
                " 1 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
}
