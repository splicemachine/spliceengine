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

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class CaseExpressionIT extends SpliceUnitTest {

    public static final String CLASS_NAME = CaseExpressionIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME = "TAB";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);
    private static final int MAX=10;
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s values (?)", spliceTableWatcher));
                        for(int i=1;i<MAX+1;i++){
                            ps.setInt(1,i);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * This '@Before' method is ran before every '@Test' method
     */
    @Before
    public void setUp() throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(
                String.format("select * from %s", this.getTableReference(TABLE_NAME)));
        Assert.assertEquals(MAX, resultSetSize(resultSet));
        resultSet.close();
    }

    @Test
    public void testCaseWhen() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                String.format("select * from %s order by case when mod(i,2) = 0 then -i else i end", this.getTableReference(TABLE_NAME)));

        String expected =
                "I |\n" +
                "----\n" +
                "10 |\n" +
                " 8 |\n" +
                " 6 |\n" +
                " 4 |\n" +
                " 2 |\n" +
                " 1 |\n" +
                " 3 |\n" +
                " 5 |\n" +
                " 7 |\n" +
                " 9 |";

        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
    }

    @Test
    public void testCaseExpressionWhen() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                String.format("select * from %s order by case mod(i,2) when 0 then -i when 1 then i end", this.getTableReference(TABLE_NAME)));

        String expected =
                "I |\n" +
                "----\n" +
                "10 |\n" +
                " 8 |\n" +
                " 6 |\n" +
                " 4 |\n" +
                " 2 |\n" +
                " 1 |\n" +
                " 3 |\n" +
                " 5 |\n" +
                " 7 |\n" +
                " 9 |";

        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
    }
}
