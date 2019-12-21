/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Created by yxia on 12/17/19.
 */
public class ViewsInSysIbmIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(ViewsInSysIbmIT.class);
    public static final String CLASS_NAME = ViewsInSysIbmIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn) throws Exception {
        new TableCreator(conn)
                .withCreate("create table t1 (a1 int not null unique, b1 int not null, c1 int not null, d1 int not null, primary key(b1,c1))")
                .withIndex("create index idx1_t1 on t1(b1)")
                .withIndex("create unique index idx2_t1 on t1(d1)")
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, constraint fk1 foreign key (b2,c2) references t1(b1,c1))")
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, constraint fk2 foreign key (b3,c3) references t1(b1,c1))")
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int constraint ck1 check (a4>4), b4 int, c4 int, constraint fk3 foreign key (a4) references t1(a1))")
                .create();

        new TableCreator(conn)
                .withCreate("create table t5 (a5 int not null, b5 smallint, c5 bigint not null, d5 float, e5 date default '2019-01-01', f5 decimal(10,2) default 0.00, g5 timestamp not null, h5 time, i5 char(10), j5 varchar(200), k5 boolean, l5 CLOB(32765), m5 BLOB(32768), primary key (g5,c5,a5) )")
                .create();

        new TableCreator(conn)
                .withCreate("create table t6 (a6 int not null unique, b6 int not null, c6 int, d6 int not null, primary key(d6), constraint unique_con1 unique(a6,b6))")
                .create();

        conn.commit();
    }

    public static void cleanoutDirectory() {
        try {
            File file = new File(getExternalResourceDirectory());
            if (file.exists())
                FileUtils.deleteDirectory(new File(getExternalResourceDirectory()));
            file.mkdir();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        cleanoutDirectory();
        createData(spliceClassWatcher.getOrCreateConnection());
    }

    @Test
    public void testSysColumns() throws Exception {
        String sqlText = format("select name, tbname, tbcreator, '-'||coltype||'-', nulls, codepage, length, scale, colno, typename, longlength, keyseq from sysibm.syscolumns where tbname='T5' and TBCREATOR='%s'", CLASS_NAME);
        String expected = "NAME |TBNAME |   TBCREATOR    |     4     | NULLS |CODEPAGE |LENGTH | SCALE | COLNO |TYPENAME  |LONGLENGTH |KEYSEQ |\n" +
                "--------------------------------------------------------------------------------------------------------------------\n" +
                " A5  |  T5   |VIEWSINSYSIBMIT |-INTEGER - |   N   |    0    |   4   |   0   |   0   | INTEGER  |     4     |   3   |\n" +
                " B5  |  T5   |VIEWSINSYSIBMIT |-SMALLINT- |   Y   |    0    |   2   |   0   |   1   |SMALLINT  |     2     | NULL  |\n" +
                " C5  |  T5   |VIEWSINSYSIBMIT |-BIGINT  - |   N   |    0    |   8   |   0   |   2   | BIGINT   |     8     |   2   |\n" +
                " D5  |  T5   |VIEWSINSYSIBMIT |-DOUBLE  - |   Y   |    0    |   8   |   0   |   3   | DOUBLE   |     8     | NULL  |\n" +
                " E5  |  T5   |VIEWSINSYSIBMIT |-DATE    - |   Y   |    0    |   4   |   0   |   4   |  DATE    |     4     | NULL  |\n" +
                " F5  |  T5   |VIEWSINSYSIBMIT |-DECIMAL - |   Y   |    0    |  10   |   2   |   5   | DECIMAL  |    10     | NULL  |\n" +
                " G5  |  T5   |VIEWSINSYSIBMIT |-TIMESTMP- |   N   |    0    |  10   |   6   |   6   |TIMESTAMP |    10     |   1   |\n" +
                " H5  |  T5   |VIEWSINSYSIBMIT |-TIME    - |   Y   |    0    |   3   |   0   |   7   |  TIME    |     3     | NULL  |\n" +
                " I5  |  T5   |VIEWSINSYSIBMIT |-CHAR    - |   Y   |  1208   |  10   |   0   |   8   |CHARACTER |    10     | NULL  |\n" +
                " J5  |  T5   |VIEWSINSYSIBMIT |-VARCHAR - |   Y   |  1208   |  200  |   0   |   9   | VARCHAR  |    200    | NULL  |\n" +
                " K5  |  T5   |VIEWSINSYSIBMIT |-BOOLEAN - |   Y   |    0    |   1   |   0   |  10   | BOOLEAN  |     1     | NULL  |\n" +
                " L5  |  T5   |VIEWSINSYSIBMIT |-CLOB    - |   Y   |  1208   | 32765 |   0   |  11   |  CLOB    |   32765   | NULL  |\n" +
                " M5  |  T5   |VIEWSINSYSIBMIT |-BLOB    - |   Y   |    0    |  -1   |   0   |  12   |  BLOB    |   32768   | NULL  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSysTables() throws Exception {
        String sqlText = format("select name, creator, type, colcount, keycolumns, keyunique, codepage from sysibm.systables where CREATOR='%s' and name in ('T1', 'T2', 'T3', 'T4')", CLASS_NAME);
        String expected = "NAME |    CREATOR     |TYPE |COLCOUNT |KEYCOLUMNS | KEYUNIQUE |CODEPAGE |\n" +
                "-------------------------------------------------------------------------\n" +
                " T1  |VIEWSINSYSIBMIT |  T  |    4    |     2     |     1     |  1208   |\n" +
                " T2  |VIEWSINSYSIBMIT |  T  |    3    |     0     |     0     |  1208   |\n" +
                " T3  |VIEWSINSYSIBMIT |  T  |    3    |     0     |     0     |  1208   |\n" +
                " T4  |VIEWSINSYSIBMIT |  T  |    3    |     0     |     0     |  1208   |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testKeyUnique() throws Exception {
        String sqlText = format("select name, creator, type, colcount, keycolumns, keyunique, codepage from sysibm.systables where CREATOR='%s' and name ='T6'", CLASS_NAME);
        String expected = "NAME |    CREATOR     |TYPE |COLCOUNT |KEYCOLUMNS | KEYUNIQUE |CODEPAGE |\n" +
                "-------------------------------------------------------------------------\n" +
                " T6  |VIEWSINSYSIBMIT |  T  |    4    |     1     |     2     |  1208   |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSystableType() throws Exception {
        String sqlText = format("select name, creator, type, colcount, keycolumns, keyunique, codepage from sysibm.systables where CREATOR='%s' and name in ('SYSCOLUMNS', 'SYSTABLES')", "SYS");
        String expected = "NAME    | CREATOR |TYPE |COLCOUNT |KEYCOLUMNS | KEYUNIQUE |CODEPAGE |\n" +
                "------------------------------------------------------------------------\n" +
                "SYSCOLUMNS |   SYS   |  T  |   13    |     0     |     0     |  1208   |\n" +
                " SYSTABLES |   SYS   |  T  |   15    |     0     |     0     |  1208   |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testAlias() throws Exception {
        methodWatcher.executeUpdate(format("create synonym %s.S5 for t5", CLASS_NAME));
        String sqlText = format("select name, creator, type, colcount, keycolumns, keyunique, codepage from sysibm.systables where CREATOR='%s' and name='S5'", CLASS_NAME);
        String expected = "NAME |    CREATOR     |TYPE |COLCOUNT |KEYCOLUMNS | KEYUNIQUE |CODEPAGE |\n" +
                "-------------------------------------------------------------------------\n" +
                " S5  |VIEWSINSYSIBMIT |  A  |    0    |     0     |     0     |    0    |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        methodWatcher.executeUpdate(format("drop synonym %s.S5", CLASS_NAME));
    }

    @Test
    public void testView() throws Exception {
        methodWatcher.executeUpdate(format("create view %s.V5 as select * from t5", CLASS_NAME));
        String sqlText = format("select name, creator, type, colcount, keycolumns, keyunique, codepage from sysibm.systables where CREATOR='%s' and name='V5'", CLASS_NAME);
        String expected = "NAME |    CREATOR     |TYPE |COLCOUNT |KEYCOLUMNS | KEYUNIQUE |CODEPAGE |\n" +
                "-------------------------------------------------------------------------\n" +
                " V5  |VIEWSINSYSIBMIT |  V  |   13    |     0     |     0     |  1208   |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        methodWatcher.executeUpdate(format("drop view %s.V5", CLASS_NAME));
    }

    @Test
    @Category(HBaseTest.class)
    public void testExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"orc_partition_second";
        methodWatcher.execute(String.format("create external table %s.orc_part_2nd (col1 int, col2 int, col3 varchar(10)) " +
                "partitioned by (col2,col3) STORED AS ORC LOCATION '%s'", CLASS_NAME, tablePath));

        // check syscolumns content
        String sqlText = format("select * from sysibm.syscolumns where tbname='ORC_PART_2ND' and TBCREATOR='%s'", CLASS_NAME);
        String expected = "NAME |   TBNAME    |   TBCREATOR    | COLTYPE | NULLS |CODEPAGE |LENGTH | SCALE | COLNO |TYPENAME |LONGLENGTH |KEYSEQ |\n" +
                "-----------------------------------------------------------------------------------------------------------------------\n" +
                "COL1 |ORC_PART_2ND |VIEWSINSYSIBMIT | INTEGER |   Y   |    0    |   4   |   0   |   0   | INTEGER |     4     | NULL  |\n" +
                "COL2 |ORC_PART_2ND |VIEWSINSYSIBMIT | INTEGER |   Y   |    0    |   4   |   0   |   1   | INTEGER |     4     | NULL  |\n" +
                "COL3 |ORC_PART_2ND |VIEWSINSYSIBMIT | VARCHAR |   Y   |  1208   |  10   |   0   |   2   | VARCHAR |    10     | NULL  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // check systables content
        sqlText = format("select * from sysibm.systables where CREATOR='%s' and name = 'ORC_PART_2ND'", CLASS_NAME);
        expected = "NAME     |    CREATOR     |TYPE |COLCOUNT |KEYCOLUMNS | KEYUNIQUE |CODEPAGE |\n" +
                "---------------------------------------------------------------------------------\n" +
                "ORC_PART_2ND |VIEWSINSYSIBMIT |  E  |    3    |     0     |     0     |  1208   |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        methodWatcher.executeUpdate(format("drop table %s.orc_part_2nd", CLASS_NAME));
    }


    public static String getExternalResourceDirectory() {
        return SpliceUnitTest.getHBaseDirectory()+"/target/external2/";
    }
}