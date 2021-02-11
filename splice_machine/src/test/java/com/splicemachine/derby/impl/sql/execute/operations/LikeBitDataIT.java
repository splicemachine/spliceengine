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
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertArrayEquals;

@RunWith(Parameterized.class)
public class LikeBitDataIT extends SpliceUnitTest {

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(id int, a1 char(10) for bit data, a2 varchar(10) for bit data, a3 long varchar for bit data)" )
                .withInsert("insert into t1 values (?, ?, ?, ?) ")
                .withRows(rows(
                        row(1, "abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes()),
                        row(2, "abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes()),
                        row(3, "abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes()),
                        row(4, "ab".getBytes(),      "ab".getBytes(),      "ab".getBytes()),
                        row(5, "".getBytes(),        "".getBytes(),        "".getBytes()),
                        row(6, "ab".getBytes(),      "ab".getBytes(),      "ab".getBytes()),
                        row(7, "aa".getBytes(),      "aa".getBytes(),      "aa".getBytes()),
                        row(8, "_%".getBytes(),      "_%".getBytes(),      "+%".getBytes()),
                        row(9, null,                 null,                 null)
                ))
                .create();

        conn.commit();

    }

    static class TestConfiguration {
        public final String datatype;
        public final boolean useSpark;
        public final boolean prepared;
        public final String castDataType;

        TestConfiguration(String datatype, boolean useSpark, boolean prepared, String castDataType) {
            this.datatype = datatype;
            this.useSpark = useSpark;
            this.prepared = prepared;
            this.castDataType = castDataType;
        }

        @Override
        public String toString() {
            return "datatype=" + datatype +
                   ", useSpark=" + useSpark +
                   ", prepared=" + prepared +
                   ", castDataType=" + castDataType;
        }
    }

    TestConfiguration testConfiguration;

    public LikeBitDataIT(TestConfiguration testConfiguration) {
        this.testConfiguration = testConfiguration;
    }

    public static final String CLASS_NAME = LikeBitDataIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).around(spliceSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);


    @Parameterized.Parameters(name = "with configuration {0}")
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(1);

        params.add(new Object[]{new TestConfiguration("char for bit data", true, true,"varchar(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("char for bit data", false, true,"varchar(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("char for bit data", true, false,"varchar(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("char for bit data", false, false,"varchar(3) for bit data")});

        params.add(new Object[]{new TestConfiguration("varchar for bit data", true, true,"varchar(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", false, true,"varchar(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", true, false,"varchar(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", false, false,"varchar(3) for bit data")});

        params.add(new Object[]{new TestConfiguration("long varchar for bit data", true, true,"varchar(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", false, true,"varchar(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", true, false,"varchar(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", false, false,"varchar(3) for bit data")});

        params.add(new Object[]{new TestConfiguration("char for bit data", true, true,"long varchar for bit data")});
        params.add(new Object[]{new TestConfiguration("char for bit data", false, true,"long varchar for bit data")});
        params.add(new Object[]{new TestConfiguration("char for bit data", true, false,"long varchar for bit data")});
        params.add(new Object[]{new TestConfiguration("char for bit data", false, false,"long varchar for bit data")});

        params.add(new Object[]{new TestConfiguration("varchar for bit data", true, true,"long varchar for bit data")});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", false, true,"long varchar for bit data")});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", true, false,"long varchar for bit data")});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", false, false,"long varchar for bit data")});

        params.add(new Object[]{new TestConfiguration("long varchar for bit data", true, true,"long varchar for bit data")});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", false, true,"long varchar for bit data")});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", true, false,"long varchar for bit data")});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", false, false,"long varchar for bit data")});

        params.add(new Object[]{new TestConfiguration("char for bit data", true, true,"char(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("char for bit data", false, true,"char(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("char for bit data", true, false,"char(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("char for bit data", false, false,"char(3) for bit data")});

        params.add(new Object[]{new TestConfiguration("varchar for bit data", true, true,"char(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", false, true,"char(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", true, false,"char(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", false, false,"char(3) for bit data")});

        params.add(new Object[]{new TestConfiguration("long varchar for bit data", true, true,"char(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", false, true,"char(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", true, false,"char(3) for bit data")});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", false, false,"char(3) for bit data")});

        params.add(new Object[]{new TestConfiguration("char for bit data", true, true,null)});
        params.add(new Object[]{new TestConfiguration("char for bit data", false, true,null)});
        params.add(new Object[]{new TestConfiguration("char for bit data", true, false,null)});
        params.add(new Object[]{new TestConfiguration("char for bit data", false, false,null)});

        params.add(new Object[]{new TestConfiguration("varchar for bit data", true, true,null)});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", false, true,null)});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", true, false,null)});
        params.add(new Object[]{new TestConfiguration("varchar for bit data", false, false,null)});

        params.add(new Object[]{new TestConfiguration("long varchar for bit data", true, true,null)});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", false, true,null)});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", true, false,null)});
        params.add(new Object[]{new TestConfiguration("long varchar for bit data", false, false,null)});

        return params;
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    public String padRightSpaces(String inputString, int length) {
        if (inputString.length() >= length) {
            return inputString;
        }
        StringBuilder sb = new StringBuilder();
        while (sb.length() < length - inputString.length()) {
            sb.append('0');
        }
        sb.append(inputString);

        return sb.toString();
    }

    private void testLikeInternal(String likePredicate,
                                  byte[][] expected,
                                  String preparedLikePattern,
                                  String escapePattern) throws Exception {
        String colName = "a1";
        boolean fixedSize = true;
        if(testConfiguration.datatype.startsWith("varchar")) {
            colName = "a2";
            fixedSize = false;
        } else if(testConfiguration.datatype.startsWith("long varchar")) {
            colName = "a3";
            fixedSize = false;
        }

        String predicate = "";
        if(testConfiguration.prepared) {
            predicate = "?";
        } else {
            if(testConfiguration.castDataType == null) {
                predicate = likePredicate;
            } else {
                predicate = "cast (" + likePredicate + "as " + testConfiguration.castDataType + ")";
            }
        }

        String escapeClause = "";
        if(escapePattern != null) {
            escapeClause = " escape " + escapePattern + " ";
            predicate = predicate.replaceAll("char\\(3\\)", "char(5)");
            if(fixedSize) {
                predicate = predicate.replaceAll("\\\\_\\\\%", "\\\\_\\\\%%");
            }
        }

        String sqlText = format("select id, %s from t1 --splice-properties useSpark=%s\n where %s like %s %s order by id asc",
                                colName, testConfiguration.useSpark, colName, predicate, escapeClause);
        ResultSet rs = null;
        try {
            if (testConfiguration.prepared) {
                PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
                if (preparedLikePattern != null) {
                    ps.setString(1, preparedLikePattern);
                }
                rs = ps.executeQuery();
            } else {
                rs = methodWatcher.executeQuery(sqlText);
            }

            int i = 0;
            while (rs.next()) {
                int id = rs.getInt(1);
                byte[] byteArray = rs.getBytes(2);

                if(fixedSize) {
                    byte[] paddedExpectedArray = new byte[]{32, 32, 32, 32, 32, 32, 32, 32, 32, 32};
                    System.arraycopy(expected[i], 0, paddedExpectedArray, 0, expected[i].length);
                    assertArrayEquals("like for row with id=" + id + " does not match the expected result.", paddedExpectedArray, byteArray);
                } else {
                    assertArrayEquals("like for row with id=" + id + " does not match the expected result.", expected[i], byteArray);
                }
                i++;
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    @Test
    public void testLikeOperatorWithUnderscorePlaceHolderWithCast() throws Exception {
        testLikeInternal("cast('a__c' as varchar(3) for bit data)",
                                    new byte[][]{"abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes()},
                         "a__c",
                         null);
    }

    @Test
    public void testLikeOperatorWithPercentagePlaceHolderWithCast() throws Exception {
        testLikeInternal("cast('a%' as varchar(3) for bit data)",
                         new byte[][]{"abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes(), "ab".getBytes(), "ab".getBytes(), "aa".getBytes()},
                         "a%",
                         null);
    }

    @Test
    public void testLikeOperatorWithUnderscorePlaceHolderWithoutCast() throws Exception {
        testLikeInternal("'a__c'",
                         new byte[][]{"abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes()},
                         "a__c",
                         null);
    }

    @Test
    public void testLikeOperatorWithPercentagePlaceHolderWithoutCast() throws Exception {
        testLikeInternal("'a%'",
                         new byte[][]{"abac bc".getBytes(), "abac bc".getBytes(), "abac bc".getBytes(), "ab".getBytes(), "ab".getBytes(), "aa".getBytes()},
                         "a%",
                         null);
    }

    @Test
    public void testLikeOperatorWithUnderscorePlaceHolderWithoutCastAndEscape() throws Exception {
        testLikeInternal("'\\_\\%'",
                         new byte[][]{"_%".getBytes()},
                         "\\_\\%",
                         "'\\'");
    }
}
