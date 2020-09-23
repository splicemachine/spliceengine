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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by tgildersleeve on 7/11/17.
 */
public class DefaultIndexIT extends SpliceUnitTest{

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final String CLASS_NAME = DefaultIndexIT.class.getSimpleName().toUpperCase();

    protected  static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static SpliceTableWatcher BLANK_TABLE = new SpliceTableWatcher("BLANK_TABLE", schemaWatcher.schemaName,"(i varchar(10) default '', j varchar(10))");
    private static SpliceTableWatcher NULL_TABLE = new SpliceTableWatcher("NULL_TABLE", schemaWatcher.schemaName,"(i varchar(10), j varchar(10))");
    private static SpliceIndexWatcher NULL_INDEX_NO_NULLS = new SpliceIndexWatcher("NULL_TABLE", schemaWatcher.schemaName, "NULL_INDEX_NO_NULLS",
            schemaWatcher.schemaName, "(i)",false,true,false);
    private static SpliceIndexWatcher BLANK_INDEX_NO_DEFAULTS = new SpliceIndexWatcher("BLANK_TABLE", schemaWatcher.schemaName, "BLANK_INDEX_NO_DEFAULTS",
            schemaWatcher.schemaName, "(i)",false,false,true);
    private static SpliceTableWatcher INDEX_AFTER_LOAD_BLANK_TABLE = new SpliceTableWatcher("INDEX_AFTER_LOAD_BLANK_TABLE", schemaWatcher.schemaName,"(i varchar(10) default '', j varchar(10))");
    private static SpliceTableWatcher INDEX_AFTER_LOAD_NULL_TABLE = new SpliceTableWatcher("INDEX_AFTER_LOAD_NULL_TABLE", schemaWatcher.schemaName,"(i varchar(10), j varchar(10))");
    private static SpliceTableWatcher BULK_HFILE_BLANK_TABLE = new SpliceTableWatcher("BULK_HFILE_BLANK_TABLE", schemaWatcher.schemaName,"(i varchar(10) default '', j varchar(10))");
    private static SpliceTableWatcher BULK_HFILE_NULL_TABLE = new SpliceTableWatcher("BULK_HFILE_NULL_TABLE", schemaWatcher.schemaName,"(i varchar(10), j varchar(10))");
    private static SpliceIndexWatcher BULK_HFILE_NULL_TABLE_IX = new SpliceIndexWatcher(BULK_HFILE_NULL_TABLE.tableName, schemaWatcher.schemaName, "BULK_HFILE_NULL_TABLE_IX",
            schemaWatcher.schemaName, "(i)",false,true,false);
    private static SpliceIndexWatcher BULK_HFILE_BLANK_TABLE_IX = new SpliceIndexWatcher(BULK_HFILE_BLANK_TABLE.tableName, schemaWatcher.schemaName, "BULK_HFILE_BLANK_TABLE_IX",
            schemaWatcher.schemaName, "(i)",false,false,true);
    private static SpliceTableWatcher T1 = new SpliceTableWatcher("T1", schemaWatcher.schemaName, "(a1 int, b1 int default 5, c1 int, d1 varchar(20) default 'NNN', e1 varchar(20))");
    private static SpliceIndexWatcher T1_IX_B1_EXCL_DEFAULTS = new SpliceIndexWatcher(T1.tableName, schemaWatcher.schemaName, "T1_IX_B1_EXCL_DEFAULTS", schemaWatcher.schemaName, "(b1)", false, false, true);
    private static SpliceIndexWatcher T1_IX_C1_EXCL_NULL = new SpliceIndexWatcher(T1.tableName, schemaWatcher.schemaName, "T1_IX_C1_EXCL_NULL", schemaWatcher.schemaName, "(c1)", false, true, false);
    private static SpliceIndexWatcher T1_IX_D1_EXCL_DEFAULTS = new SpliceIndexWatcher(T1.tableName, schemaWatcher.schemaName, "T1_IX_D1_EXCL_DEFAULTS", schemaWatcher.schemaName, "(d1)", false, false, true);
    private static SpliceIndexWatcher T1_IX_E1_EXCL_NULL = new SpliceIndexWatcher(T1.tableName, schemaWatcher.schemaName, "T1_IX_E1_EXCL_NULL", schemaWatcher.schemaName, "(e1)", false, true, false);
    private static SpliceTableWatcher T2 = new SpliceTableWatcher("T2", schemaWatcher.schemaName, "(a2 int, b2 int default 5, c2 int, d2 varchar(20) default 'DEF', e2 varchar(20), primary key(a2))");
    private static SpliceIndexWatcher T2_IX_B2_EXCL_DEFAULTS = new SpliceIndexWatcher(T2.tableName, schemaWatcher.schemaName, "T2_IX_B2_EXCL_DEFAULTS", schemaWatcher.schemaName, "(b2)", false, false, true);
    private static SpliceIndexWatcher T2_IX_C2_EXCL_NULL = new SpliceIndexWatcher(T2.tableName, schemaWatcher.schemaName, "T2_IX_C2_EXCL_NULL", schemaWatcher.schemaName, "(c2)", false, true, false);

    private static SpliceTableWatcher T3 = new SpliceTableWatcher("T3", schemaWatcher.schemaName, "(a3 int, b3 int default 5, c3 char(5) default ' ', d3 varchar(5) default ' ', e3 varchar(20), primary key(a3, b3))");
    private static SpliceIndexWatcher T3_IX_C3_DESC_EXCL_DEFAULTS = new SpliceIndexWatcher(T3.tableName, schemaWatcher.schemaName, "T3_IX_C3_DESC_EXCL_DEFAULTS", schemaWatcher.schemaName, "(c3 desc)", false, false, true);
    private static SpliceIndexWatcher T3_IX_D3_DESC_EXCL_DEFAULTS = new SpliceIndexWatcher(T3.tableName, schemaWatcher.schemaName, "T3_IX_D3_DESC_EXCL_DEFAULTS", schemaWatcher.schemaName, "(d3 desc)", false, false, true);
    private static SpliceIndexWatcher T3_IX_B3_DESC_C3_EXCL_DEFAULTS = new SpliceIndexWatcher(T3.tableName, schemaWatcher.schemaName, "T3_IX_B3_DESC_C3_EXCL_DEFAULTS", schemaWatcher.schemaName, "(b3 desc, c3)", false, false, true);

    private static SpliceTableWatcher T4 = new SpliceTableWatcher("T4", schemaWatcher.schemaName, "(a4 int, b4 varchar(30) default 'DEFAULT', c4 int, d4 char(20))");

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(schemaWatcher.schemaName);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(BLANK_TABLE)
            .around(NULL_TABLE)
            .around(NULL_INDEX_NO_NULLS)
            .around(BLANK_INDEX_NO_DEFAULTS)
            .around(INDEX_AFTER_LOAD_BLANK_TABLE)
            .around(INDEX_AFTER_LOAD_NULL_TABLE)
            .around(BULK_HFILE_BLANK_TABLE)
            .around(BULK_HFILE_NULL_TABLE)
            .around(BULK_HFILE_NULL_TABLE_IX)
            .around(BULK_HFILE_BLANK_TABLE_IX)
            .around(T1)
            .around(T1_IX_B1_EXCL_DEFAULTS)
            .around(T1_IX_C1_EXCL_NULL)
            .around(T1_IX_D1_EXCL_DEFAULTS)
            .around(T1_IX_E1_EXCL_NULL)
            .around(T2)
            .around(T2_IX_B2_EXCL_DEFAULTS)
            .around(T2_IX_C2_EXCL_NULL)
            .around(T3)
            .around(T3_IX_C3_DESC_EXCL_DEFAULTS)
            .around(T3_IX_D3_DESC_EXCL_DEFAULTS)
            .around(T3_IX_B3_DESC_C3_EXCL_DEFAULTS)
            .around(T4)
    ;

    private Connection conn;

    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDownTest() throws Exception{
        try {
            conn.rollback();
        } catch (Exception e) {} // Swallow for HFile Bit Running in Control
     }

    @Before
    public void setUp() throws Exception {
        methodWatcher.executeUpdate(String.format("INSERT INTO BLANK_TABLE(i,J) VALUES ('',''),('SD','SD'),(null,null)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO NULL_TABLE(i,J) VALUES ('',''),('SD','SD'),(null,null)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO INDEX_AFTER_LOAD_BLANK_TABLE(i,J) VALUES ('',''),('SD','SD'),(null,null)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO INDEX_AFTER_LOAD_NULL_TABLE(i,J) VALUES ('',''),('SD','SD'),(null,null)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO T1(a1, c1, e1) VALUES(1, null, null)," +
                                                                                   "(3, null, null), " +
                                                                                   "(5, null, null), " +
                                                                                   "(7, null, null), " +
                                                                                   "(9, null, null)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO T1 VALUES(2, 2, 2, 'AAA', 'AAA'), " +
                                                                       "(4, 4, 4, 'CCC', 'CCC'), " +
                                                                       "(6, 6, 6, 'EEE', 'EEE'), " +
                                                                       "(8, 8, 8, 'GGG', 'GGG'), " +
                                                                       "(10, 10, 10, 'III', 'III')"));
        methodWatcher.executeUpdate(String.format("INSERT INTO T2(a2, c2, e2) VALUES(1, null, null)," +
                "(5, null, null), " +
                "(9, null, null)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO T2 VALUES(2, 2, 2, 'AAA', 'AAA'), " +
                "(6, 6, 6, 'EEE', 'EEE'), " +
                "(10, 10, 10, 'III', 'III')"));
        methodWatcher.executeUpdate(String.format("INSERT INTO T3 VALUES(1, 1, 'AAA', 'AAA', 'AAA'), " +
                "(2, 5, ' ', ' ', 'BBB'), " +
                "(3, 5, '     ', '     ', 'CCC')"));

        methodWatcher.executeUpdate(String.format("INSERT INTO T4 VALUES " +
                "(1,'a',1, 'A'), " +
                "(2, 'b', 2, 'B')," +
                "(3, 'DEFAULT', null, 'C')," +
                "(4, null, null, 'D')"));

    }

    @Test
    public void testIndexAfterLoadBlankTable() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(conn,schemaWatcher.schemaName,INDEX_AFTER_LOAD_BLANK_TABLE.tableName,"INDEX_AFTER_LOAD_BLANK_TABLE_IX","(i)",false,false,true);
            methodWatcher.executeQuery(String.format("SELECT * FROM INDEX_AFTER_LOAD_BLANK_TABLE --SPLICE-PROPERTIES index=INDEX_AFTER_LOAD_BLANK_TABLE_IX\n"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }

    @Test
    public void testIndexAfterLoadNullTable() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(conn,schemaWatcher.schemaName,INDEX_AFTER_LOAD_NULL_TABLE.tableName,"INDEX_AFTER_LOAD_NULL_TABLE_IX","(i)",false,false,true);
            methodWatcher.executeQuery(String.format("SELECT * FROM INDEX_AFTER_LOAD_NULL_TABLE --SPLICE-PROPERTIES index=INDEX_AFTER_LOAD_NULL_TABLE_IX\n"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }


    @Test
    public void testEmptyPredicateScanCannotUseExcludeNullIndex() throws Exception {
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM NULL_TABLE --SPLICE-PROPERTIES index=NULL_INDEX_NO_NULLS\n"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        //query should run without specifying the index
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM NULL_TABLE where i = 'SD'"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "SD |SD |", TestUtils.FormattedResult.ResultFactory.toString(rs));
     }

    @Test
    public void testIndexPredicateEqualityScanCanUseExcludeNullIndex() throws Exception {
            ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM NULL_TABLE --SPLICE-PROPERTIES index=NULL_INDEX_NO_NULLS\n where i = 'SD'"));
            Assert.assertEquals("I | J |\n" +
                    "--------\n" +
                    "SD |SD |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testIndexIsNotNullScanCanUseExcludeNullIndex() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM NULL_TABLE --SPLICE-PROPERTIES index=NULL_INDEX_NO_NULLS\n where i is not null"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "   |   |\n" +
                "SD |SD |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testIndexIsNullScanCannotUseExcludeNullIndex() throws Exception {
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM NULL_TABLE --SPLICE-PROPERTIES index=NULL_INDEX_NO_NULLS\n where i is null"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }


    @Test
    public void testNonIndexPredicateEqualityScanCanUseExcludeNullIndex() throws Exception {
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM NULL_TABLE --SPLICE-PROPERTIES index=NULL_INDEX_NO_NULLS\n where j = 'SD'"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }

    @Test
    public void testPredicateEqualityScanChoosesExcludeNullIndex() throws Exception {
        thirdRowContainsQuery("explain SELECT I FROM NULL_TABLE where i = 'SD'","Index",methodWatcher);
    }

    @Test
    public void testEmptyPredicateScanCannotUseExcludeDefaultIndex() throws Exception {
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM BLANK_TABLE --SPLICE-PROPERTIES index=BLANK_INDEX_NO_DEFAULTS\n"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

    }

    @Test
    public void testIndexExcludeDefault() throws Exception {
        /**
         *Group 1: predicate value is smaller than the default value
         */
        /* case 1 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 > 4"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
        /* case 2 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 >= 4"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 3 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 <> 4"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 4 */
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1=4"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                " 4 | 4 | 4 |CCC |CCC |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 5 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1<4"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                " 2 | 2 | 2 |AAA |AAA |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 6 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1<=4"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                " 2 | 2 | 2 |AAA |AAA |\n" +
                " 4 | 4 | 4 |CCC |CCC |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /**
         * Group 2: predicate value is the same as the default value
         */
        /* case 1 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 = 5"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
        /* case 2 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 >= 5"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
        /* case 3 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 <= 5"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 4 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 < 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                " 2 | 2 | 2 |AAA |AAA |\n" +
                " 4 | 4 | 4 |CCC |CCC |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 5 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 > 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 6 | 6 | 6 |EEE |EEE |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 6 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 <> 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 2 | 2 | 2 |AAA |AAA |\n" +
                " 4 | 4 | 4 |CCC |CCC |\n" +
                " 6 | 6 | 6 |EEE |EEE |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        /**
         * Group 3: prediate value is larger than the default value
         */
        /* case 1 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 < 7"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
        /* case 2 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 <= 7"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
        /* case 3 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 <> 7"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
        /* case 4 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 = 7"));
        Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 5 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 >= 7"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 6 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 > 7"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        /**
         * Group 4: prediate is inlist
         */
        /* case 1 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 in (1,3,5,7,9)"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 2 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 in (2,4,6,8,10)"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 2 | 2 | 2 |AAA |AAA |\n" +
                " 4 | 4 | 4 |CCC |CCC |\n" +
                " 6 | 6 | 6 |EEE |EEE |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 3 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1=1 or b1=5"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 4 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1=2 or b1=4"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                " 2 | 2 | 2 |AAA |AAA |\n" +
                " 4 | 4 | 4 |CCC |CCC |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        /**
         * Group 5: predicate is IS NULL/IS NOT NULL
         */
        /* case 1 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 is not null"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 2 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1 is null"));
        Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs));
        /**
         * Group 6: predicate contains OR term
         */
        /* case 1 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1=6 or c1=6"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 2 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where (b1=6 or c1=6) and (b1=4 or b1=6)"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                " 6 | 6 | 6 |EEE |EEE |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /**
         * Group 7: other negative test cases where predicates involves expressions
         */
        /* case 1 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n where b1+1=3"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }

    @Test
    public void testIndexExcludeDefaultDesc() throws Exception {
        /* case 1 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T3 --SPLICE-PROPERTIES index=T3_IX_C3_DESC_EXCL_DEFAULTS"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
        /* case 2 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T3 --SPLICE-PROPERTIES index=T3_IX_D3_DESC_EXCL_DEFAULTS\n"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 3 */
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T3 --SPLICE-PROPERTIES index=T3_IX_B3_DESC_C3_EXCL_DEFAULTS\n where B3 <> 4"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 4 */
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM T3 --SPLICE-PROPERTIES index=T3_IX_C3_DESC_EXCL_DEFAULTS\n where C3<>' '"));
        Assert.assertEquals("A3 |B3 |C3  |D3  |E3  |\n" +
                "-----------------------\n" +
                " 1 | 1 |AAA |AAA |AAA |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 5 */
        rs = methodWatcher.executeQuery(String.format("SELECT a3, b3, '-'||c3||'-', '-'||d3||'-', '-'||e3||'-' FROM T3 --SPLICE-PROPERTIES index=T3_IX_D3_DESC_EXCL_DEFAULTS\n where d3 <> ' '"));
        Assert.assertEquals("A3 |B3 |   3    |   4    |  5   |\n" +
                "---------------------------------\n" +
                " 1 | 1 |-AAA  - | -AAA-  |-AAA- |\n" +
                " 3 | 5 |-     - |-     - |-CCC- |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 6 */
        rs = methodWatcher.executeQuery(String.format("SELECT a3, b3, '-'||c3||'-', '-'||d3||'-', '-'||e3||'-' FROM T3 --SPLICE-PROPERTIES index=T3_IX_B3_DESC_C3_EXCL_DEFAULTS\n where b3<=4"));
        Assert.assertEquals("A3 |B3 |   3    |  4   |  5   |\n" +
                "-------------------------------\n" +
                " 1 | 1 |-AAA  - |-AAA- |-AAA- |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testIndexExcludeNull() throws Exception {
        /**
         *Group 1: predicate value is smaller than the default value
         */
        /* case 1 */
        try {
            methodWatcher.executeQuery(String.format("select * from T1 --splice-properties index=T1_IX_C1_EXCL_NULL\n" +
                    "where case when c1 is null then 3 else c1 end > 2"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
        /* case 2 */
        try {
            methodWatcher.executeQuery(String.format("select * from T1 --splice-properties index=T1_IX_E1_EXCL_NULL\n" +
                    "where substr(E1, 1, 1) is null"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 3 test or term*/
        try {
            methodWatcher.executeQuery(String.format("select * from T1 --splice-properties index=T1_IX_E1_EXCL_NULL\n" +
                    "where E1 ='AAA' or D1='BBB'"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 4 test constant condition*/
        try {
            methodWatcher.executeQuery(String.format("select * from T1 --splice-properties index=T1_IX_E1_EXCL_NULL\n" +
                    "where 1<2"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /* case 5 test inlist */
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_E1_EXCL_NULL\n" +
                    "where E1 in ('AAA', 'CCC')"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                " 2 | 2 | 2 |AAA |AAA |\n" +
                " 4 | 4 | 4 |CCC |CCC |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 6: test null filtering expression */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_C1_EXCL_NULL\n" +
                "where c1+1=5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                " 4 | 4 | 4 |CCC |CCC |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testIndexInJoins() throws Exception {
        /**
         * group 1: index exclude defaults
         */
        /* case 1 */
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n" +
                ",T2 --SPLICE-PROPERTIES index=T2_IX_B2_EXCL_DEFAULTS\n" +
                "where a1=a2 and b1 in (2,4,6,8) and b2 in (2,4,6,8)"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |A2 |B2 |C2 |D2  |E2  |\n" +
                "--------------------------------------------\n" +
                " 2 | 2 | 2 |AAA |AAA | 2 | 2 | 2 |AAA |AAA |\n" +
                " 6 | 6 | 6 |EEE |EEE | 6 | 6 | 6 |EEE |EEE |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 2 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T2 --SPLICE-PROPERTIES index=T2_IX_B2_EXCL_DEFAULTS\n" +
                ",T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n" +
                "where a1=a2 and b1 in (2,4,6,8) and b2 in (2,4,6,8)"));
        Assert.assertEquals("A2 |B2 |C2 |D2  |E2  |A1 |B1 |C1 |D1  |E1  |\n" +
                "--------------------------------------------\n" +
                " 2 | 2 | 2 |AAA |AAA | 2 | 2 | 2 |AAA |AAA |\n" +
                " 6 | 6 | 6 |EEE |EEE | 6 | 6 | 6 |EEE |EEE |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 3 inlist contains default value 5*/
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_B1_EXCL_DEFAULTS\n" +
                    ",T2 --SPLICE-PROPERTIES index=T2_IX_B2_EXCL_DEFAULTS\n" +
                    "where a1=a2 and b2 in (1,3,5,7)"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        /**
         * group 2: index exclude nulls
         */
        /* case 1 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_C1_EXCL_NULL\n" +
                ",T2 --SPLICE-PROPERTIES index=T2_IX_C2_EXCL_NULL\n" +
                "where c1=c2 and c1 is not null and c2 is not null"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |A2 |B2 |C2 |D2  |E2  |\n" +
                "--------------------------------------------\n" +
                "10 |10 |10 |III |III |10 |10 |10 |III |III |\n" +
                " 2 | 2 | 2 |AAA |AAA | 2 | 2 | 2 |AAA |AAA |\n" +
                " 6 | 6 | 6 |EEE |EEE | 6 | 6 | 6 |EEE |EEE |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 2 */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T2 --SPLICE-PROPERTIES index=T2_IX_C2_EXCL_NULL\n" +
                ",T1 --SPLICE-PROPERTIES index=T1_IX_C1_EXCL_NULL\n" +
                "where c1=c2 and c1 is not null and c2 is not null"));
        Assert.assertEquals("A2 |B2 |C2 |D2  |E2  |A1 |B1 |C1 |D1  |E1  |\n" +
                "--------------------------------------------\n" +
                "10 |10 |10 |III |III |10 |10 |10 |III |III |\n" +
                " 2 | 2 | 2 |AAA |AAA | 2 | 2 | 2 |AAA |AAA |\n" +
                " 6 | 6 | 6 |EEE |EEE | 6 | 6 | 6 |EEE |EEE |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 3 TC genreates c1=3, so indexes qualify */
        rs = methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_c1_EXCL_NULL\n" +
                ",T2 --SPLICE-PROPERTIES index=T2_IX_C2_EXCL_NULL\n" +
                "where c1=c2 and coalesce(c1,0)=3 and c2=3"));
        Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* case 4 coalesce case*/
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM T1 --SPLICE-PROPERTIES index=T1_IX_c1_EXCL_NULL\n" +
                    ",T2 --SPLICE-PROPERTIES index=T2_IX_C2_EXCL_NULL\n" +
                    "where c1=c2 and coalesce(c1,0)=3"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }

    @Test
    public void testIndexExcludeDefaultsAfterUpdate() throws Exception {
        /* before image */
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from t1"));
        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                " 1 | 5 |NULL |NNN |NULL |\n" +
                "10 |10 | 10  |III | III |\n" +
                " 2 | 2 |  2  |AAA | AAA |\n" +
                " 3 | 5 |NULL |NNN |NULL |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 | 5 |NULL |NNN |NULL |\n" +
                " 6 | 6 |  6  |EEE | EEE |\n" +
                " 7 | 5 |NULL |NNN |NULL |\n" +
                " 8 | 8 |  8  |GGG | GGG |\n" +
                " 9 | 5 |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* change one row to default value */
        methodWatcher.executeUpdate(String.format("update t1 set b1=5 where a1=2"));

        /* after image */
        rs = methodWatcher.executeQuery(String.format("select * from t1"));
        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                " 1 | 5 |NULL |NNN |NULL |\n" +
                "10 |10 | 10  |III | III |\n" +
                " 2 | 5 |  2  |AAA | AAA |\n" +
                " 3 | 5 |NULL |NNN |NULL |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 | 5 |NULL |NNN |NULL |\n" +
                " 6 | 6 |  6  |EEE | EEE |\n" +
                " 7 | 5 |NULL |NNN |NULL |\n" +
                " 8 | 8 |  8  |GGG | GGG |\n" +
                " 9 | 5 |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* query after image */
        rs = methodWatcher.executeQuery(String.format("select * from t1 --splice-properties index=t1_ix_b1_excl_defaults\n" +
                "where b1 <> 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 4 | 4 | 4 |CCC |CCC |\n" +
                " 6 | 6 | 6 |EEE |EEE |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* change one row to non-default value */
        methodWatcher.executeUpdate(String.format("update t1 set b1=50 where a1=5"));
        /* query after image */
        rs = methodWatcher.executeQuery(String.format("select * from t1 --splice-properties index=t1_ix_b1_excl_defaults\n" +
                "where b1 <> 5"));
        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                "10 |10 | 10  |III | III |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 |50 |NULL |NNN |NULL |\n" +
                " 6 | 6 |  6  |EEE | EEE |\n" +
                " 8 | 8 |  8  |GGG | GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testIndexExcludeDefaultsAfterDelete() throws Exception {
        /* delete one row with non-default value */
        methodWatcher.executeUpdate(String.format("delete from t1 where a1=2"));
        /* query after image */
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from t1 --splice-properties index=t1_ix_b1_excl_defaults\n" +
                "where b1 <> 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 4 | 4 | 4 |CCC |CCC |\n" +
                " 6 | 6 | 6 |EEE |EEE |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* delete one row with default value */
        methodWatcher.executeUpdate(String.format("delete from t1 where a1=1"));
        /* query after image */
        rs = methodWatcher.executeQuery(String.format("select * from t1 --splice-properties index=t1_ix_b1_excl_defaults\n" +
                "where b1 <> 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 4 | 4 | 4 |CCC |CCC |\n" +
                " 6 | 6 | 6 |EEE |EEE |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testIndexExcludeNullAfterUpdate() throws Exception {
        /* before image */
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from t1"));
        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                " 1 | 5 |NULL |NNN |NULL |\n" +
                "10 |10 | 10  |III | III |\n" +
                " 2 | 2 |  2  |AAA | AAA |\n" +
                " 3 | 5 |NULL |NNN |NULL |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 | 5 |NULL |NNN |NULL |\n" +
                " 6 | 6 |  6  |EEE | EEE |\n" +
                " 7 | 5 |NULL |NNN |NULL |\n" +
                " 8 | 8 |  8  |GGG | GGG |\n" +
                " 9 | 5 |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* change one row to null value */
        methodWatcher.executeUpdate(String.format("update t1 set c1=null where a1=2"));

        /* after image */
        rs = methodWatcher.executeQuery(String.format("select * from t1"));
        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                " 1 | 5 |NULL |NNN |NULL |\n" +
                "10 |10 | 10  |III | III |\n" +
                " 2 | 2 |NULL |AAA | AAA |\n" +
                " 3 | 5 |NULL |NNN |NULL |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 | 5 |NULL |NNN |NULL |\n" +
                " 6 | 6 |  6  |EEE | EEE |\n" +
                " 7 | 5 |NULL |NNN |NULL |\n" +
                " 8 | 8 |  8  |GGG | GGG |\n" +
                " 9 | 5 |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* query after image */
        rs = methodWatcher.executeQuery(String.format("select * from t1 --splice-properties index=t1_ix_c1_excl_null\n" +
                "where c1 <> 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 4 | 4 | 4 |CCC |CCC |\n" +
                " 6 | 6 | 6 |EEE |EEE |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* change one row to not-null value */
        methodWatcher.executeUpdate(String.format("update t1 set c1=50 where a1=5"));
        /* query after image */
        rs = methodWatcher.executeQuery(String.format("select * from t1 --splice-properties index=t1_ix_c1_excl_null\n" +
                "where c1 <> 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  | E1  |\n" +
                "-----------------------\n" +
                "10 |10 |10 |III | III |\n" +
                " 4 | 4 | 4 |CCC | CCC |\n" +
                " 5 | 5 |50 |NNN |NULL |\n" +
                " 6 | 6 | 6 |EEE | EEE |\n" +
                " 8 | 8 | 8 |GGG | GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testIndexExcludeNullAfterDelete() throws Exception {
        /* delete one row with not-null value */
        methodWatcher.executeUpdate(String.format("delete from t1 where a1=2"));

        /* query after image */
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from t1 --splice-properties index=t1_ix_c1_excl_null\n" +
                "where c1 <> 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 4 | 4 | 4 |CCC |CCC |\n" +
                " 6 | 6 | 6 |EEE |EEE |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* delete one row with null value */
        methodWatcher.executeUpdate(String.format("delete from t1 where a1=1"));
        /* query after image */
        rs = methodWatcher.executeQuery(String.format("select * from t1 --splice-properties index=t1_ix_c1_excl_null\n" +
                "where c1 <> 5"));
        Assert.assertEquals("A1 |B1 |C1 |D1  |E1  |\n" +
                "----------------------\n" +
                "10 |10 |10 |III |III |\n" +
                " 4 | 4 | 4 |CCC |CCC |\n" +
                " 6 | 6 | 6 |EEE |EEE |\n" +
                " 8 | 8 | 8 |GGG |GGG |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testIndexExcludeDefaultsInInsertSelect() throws Exception {
        methodWatcher.executeUpdate(String.format("insert into t1 select a2+10, b2, c2, d2, e2 from t2 --splice-properties index=t2_ix_b2_excl_defaults\n" +
                "where b2>5"));
        ResultSet rs = methodWatcher.executeQuery("select * from t1");

        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                " 1 | 5 |NULL |NNN |NULL |\n" +
                "10 |10 | 10  |III | III |\n" +
                "16 | 6 |  6  |EEE | EEE |\n" +
                " 2 | 2 |  2  |AAA | AAA |\n" +
                "20 |10 | 10  |III | III |\n" +
                " 3 | 5 |NULL |NNN |NULL |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 | 5 |NULL |NNN |NULL |\n" +
                " 6 | 6 |  6  |EEE | EEE |\n" +
                " 7 | 5 |NULL |NNN |NULL |\n" +
                " 8 | 8 |  8  |GGG | GGG |\n" +
                " 9 | 5 |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        try {
            methodWatcher.executeUpdate(String.format("insert into t1 select a2+10, b2, c2, d2, e2 from t2 --splice-properties index=t2_ix_b2_excl_defaults\n" +
                    "where b2>4"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }

    @Test
    public void testIndexExcludeDefaultsInUpdate() throws Exception {
        methodWatcher.executeUpdate(String.format("update t1 --splice-properties index=t1_ix_b1_excl_defaults\n" +
                "set b1=100 where b1>5"));
        ResultSet rs = methodWatcher.executeQuery("select * from t1");

        Assert.assertEquals("A1 |B1  | C1  |D1  | E1  |\n" +
                "--------------------------\n" +
                " 1 | 5  |NULL |NNN |NULL |\n" +
                "10 |100 | 10  |III | III |\n" +
                " 2 | 2  |  2  |AAA | AAA |\n" +
                " 3 | 5  |NULL |NNN |NULL |\n" +
                " 4 | 4  |  4  |CCC | CCC |\n" +
                " 5 | 5  |NULL |NNN |NULL |\n" +
                " 6 |100 |  6  |EEE | EEE |\n" +
                " 7 | 5  |NULL |NNN |NULL |\n" +
                " 8 |100 |  8  |GGG | GGG |\n" +
                " 9 | 5  |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        try {
            methodWatcher.executeUpdate(String.format("update t1 --splice-properties index=t1_ix_b1_excl_defaults\n" +
                    "set b1=100 where b1=5"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

    }

    @Test
    public void testIndexExcludeDefaultsInDelete() throws Exception {
        methodWatcher.executeUpdate(String.format("delete from t1 --splice-properties index=t1_ix_b1_excl_defaults\n" +
                "where b1>5"));
        ResultSet rs = methodWatcher.executeQuery("select * from t1");

        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                " 1 | 5 |NULL |NNN |NULL |\n" +
                " 2 | 2 |  2  |AAA | AAA |\n" +
                " 3 | 5 |NULL |NNN |NULL |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 | 5 |NULL |NNN |NULL |\n" +
                " 7 | 5 |NULL |NNN |NULL |\n" +
                " 9 | 5 |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        try {
            methodWatcher.executeUpdate(String.format("delete from t1 --splice-properties index=t1_ix_b1_excl_defaults\n" +
                    "where b1=5"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }

    @Test
    public void testIndexExcludeNullInInsertSelect() throws Exception {
        methodWatcher.executeUpdate(String.format("insert into t1 select a2+10, b2, c2, d2, e2 from t2 --splice-properties index=t2_ix_c2_excl_null\n" +
                "where c2>5"));
        ResultSet rs = methodWatcher.executeQuery("select * from t1");

        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                " 1 | 5 |NULL |NNN |NULL |\n" +
                "10 |10 | 10  |III | III |\n" +
                "16 | 6 |  6  |EEE | EEE |\n" +
                " 2 | 2 |  2  |AAA | AAA |\n" +
                "20 |10 | 10  |III | III |\n" +
                " 3 | 5 |NULL |NNN |NULL |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 | 5 |NULL |NNN |NULL |\n" +
                " 6 | 6 |  6  |EEE | EEE |\n" +
                " 7 | 5 |NULL |NNN |NULL |\n" +
                " 8 | 8 |  8  |GGG | GGG |\n" +
                " 9 | 5 |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        try {
            methodWatcher.executeUpdate(String.format("insert into t1 select a2+10, b2, c2, d2, e2 from t2 --splice-properties index=t2_ix_c2_excl_null\n" +
                    "where coalesce(c2,0) = 5"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }

    @Test
    public void testIndexExcludeNullInUpdate() throws Exception {
        methodWatcher.executeUpdate(String.format("update t1 --splice-properties index=t1_ix_c1_excl_null\n" +
                "set c1=100 where c1>5"));
        ResultSet rs = methodWatcher.executeQuery("select * from t1");

        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                " 1 | 5 |NULL |NNN |NULL |\n" +
                "10 |10 | 100 |III | III |\n" +
                " 2 | 2 |  2  |AAA | AAA |\n" +
                " 3 | 5 |NULL |NNN |NULL |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 | 5 |NULL |NNN |NULL |\n" +
                " 6 | 6 | 100 |EEE | EEE |\n" +
                " 7 | 5 |NULL |NNN |NULL |\n" +
                " 8 | 8 | 100 |GGG | GGG |\n" +
                " 9 | 5 |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        try {
            methodWatcher.executeUpdate(String.format("update t1 --splice-properties index=t1_ix_c1_excl_null\n" +
                    "set c1=100 where c1 is null"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

    }

    @Test
    public void testIndexExcludeNullInDelete() throws Exception {
        /* case 3: delete */
        methodWatcher.executeUpdate(String.format("delete from t1 --splice-properties index=t1_ix_c1_excl_null\n" +
                "where c1>5"));
        ResultSet rs = methodWatcher.executeQuery("select * from t1");

        Assert.assertEquals("A1 |B1 | C1  |D1  | E1  |\n" +
                "-------------------------\n" +
                " 1 | 5 |NULL |NNN |NULL |\n" +
                " 2 | 2 |  2  |AAA | AAA |\n" +
                " 3 | 5 |NULL |NNN |NULL |\n" +
                " 4 | 4 |  4  |CCC | CCC |\n" +
                " 5 | 5 |NULL |NNN |NULL |\n" +
                " 7 | 5 |NULL |NNN |NULL |\n" +
                " 9 | 5 |NULL |NNN |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        try {
            methodWatcher.executeUpdate(String.format("delete from t1 --splice-properties index=t1_ix_c1_excl_null\n" +
                    "where coalesce(c1,0)=5"));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
    }

    @Test
    public void testIndexExcludeNullThroughSpark() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("set session_property useSpark=true");
            s.executeUpdate("create index T4_IX_B4_EXCL_NULLS on T4(c4) exclude null keys");
            String sql = String.format("SELECT count(*) FROM T4 --SPLICE-PROPERTIES index=T4_IX_B4_EXCL_NULLS\n where c4 is not null");
            ResultSet rs = s.executeQuery(sql);

            String expected = "1 |\n" +
                    "----\n" +
                    " 2 |";
            Assert.assertEquals("\n"+sql+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
            s.executeUpdate("set session_property useSpark=null");
        }
    }

    @Test
    public void testIndexExcludeDefaultThroughSpark() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("set session_property useSpark=true");
            s.executeUpdate("create index T4_IX_B4_EXCL_DEFAULTS on T4(b4) exclude default keys");
            String sql = String.format("SELECT count(*) FROM T4 --SPLICE-PROPERTIES index=T4_IX_B4_EXCL_DEFAULTS\n where b4 != 'DEFAULT'");
            ResultSet rs = s.executeQuery(sql);

            String expected = "1 |\n" +
                    "----\n" +
                    " 2 |";
            Assert.assertEquals("\n"+sql+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
            s.executeUpdate("set session_property useSpark=null");
        }
    }
}
