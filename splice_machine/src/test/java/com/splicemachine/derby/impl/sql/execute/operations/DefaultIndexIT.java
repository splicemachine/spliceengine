/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(schemaWatcher.schemaName);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(BLANK_TABLE)
            .around(NULL_TABLE)
            .around(NULL_INDEX_NO_NULLS)
            .around(BLANK_INDEX_NO_DEFAULTS);

    private Connection conn;

    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDownTest() throws Exception{
        conn.rollback();
    }

    @Before
    public void setUp() throws Exception {
        methodWatcher.executeUpdate(String.format("INSERT INTO BLANK_TABLE(i,J) VALUES ('',''),('SD','SD'),(null,null)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO NULL_TABLE(i,J) VALUES ('',''),('SD','SD'),(null,null)"));
    }

    @Test
    public void testEmptyPredicateScanCannotUseExcludeNullIndex() throws Exception {
        try {
            methodWatcher.executeQuery(String.format("SELECT * FROM NULL_TABLE --SPLICE-PROPERTIES index=NULL_INDEX_NO_NULLS\n"));
            Assert.fail("did not throw exception");
        } catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }
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



    /*
    @Test
    public void CHAR_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM CHAR_TABLE --SPLICE-PROPERTIES index=CHAR_INDEX\n WHERE I = 'A'"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                " A | A |\n" +
                " A | A |\n" +
                " A | A |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM CHAR_TABLE --SPLICE-PROPERTIES index=CHAR_INDEX\n WHERE I = 'A'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void VARCHAR_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM VARCHAR_TABLE --SPLICE-PROPERTIES index=VARCHAR_INDEX\n WHERE I = 'AAA'"));
        Assert.assertEquals("I  | J  |\n" +
                "----------\n" +
                "AAA |AAA |\n" +
                "AAA |AAA |\n" +
                "AAA |AAA |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM VARCHAR_TABLE --SPLICE-PROPERTIES index=VARCHAR_INDEX\n WHERE I = 'AAA'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void BLANK_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM BLANK_TABLE --SPLICE-PROPERTIES index=BLANK_INDEX\n WHERE I = ''"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "   |   |\n" +
                "   |   |\n" +
                "   |   |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM BLANK_TABLE --SPLICE-PROPERTIES index=BLANK_INDEX\n WHERE I = ''"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void DATE_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM DATE_TABLE --SPLICE-PROPERTIES index=DATE_INDEX\n WHERE I = '2000-01-01'"));
        Assert.assertEquals("I     |     J     |\n" +
                "------------------------\n" +
                "2000-01-01 |2000-01-01 |\n" +
                "2000-01-01 |2000-01-01 |\n" +
                "2000-01-01 |2000-01-01 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM DATE_TABLE --SPLICE-PROPERTIES index=DATE_INDEX\n WHERE I = '2000-01-01'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void TIME_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM TIME_TABLE --SPLICE-PROPERTIES index=TIME_INDEX\n WHERE I = '00:00:00'"));
        Assert.assertEquals("I    |    J    |\n" +
                "--------------------\n" +
                "00:00:00 |00:00:00 |\n" +
                "00:00:00 |00:00:00 |\n" +
                "00:00:00 |00:00:00 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM TIME_TABLE --SPLICE-PROPERTIES index=TIME_INDEX\n WHERE I = '00:00:00'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void TIMESTAMP_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM TIMESTAMP_TABLE --SPLICE-PROPERTIES index=TIMESTAMP_INDEX\n WHERE I = '2000-01-01 00:00:00'"));
        Assert.assertEquals("I           |          J           |\n" +
                "----------------------------------------------\n" +
                "2000-01-01 00:00:00.0 |2000-01-01 00:00:00.0 |\n" +
                "2000-01-01 00:00:00.0 |2000-01-01 00:00:00.0 |\n" +
                "2000-01-01 00:00:00.0 |2000-01-01 00:00:00.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM TIMESTAMP_TABLE --SPLICE-PROPERTIES index=TIMESTAMP_INDEX\n WHERE I = '2000-01-01 00:00:00'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void BIGINT_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM BIGINT_TABLE --SPLICE-PROPERTIES index=BIGINT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM BIGINT_TABLE --SPLICE-PROPERTIES index=BIGINT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void DECIMAL_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM DECIMAL_TABLE --SPLICE-PROPERTIES index=DECIMAL_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM DECIMAL_TABLE --SPLICE-PROPERTIES index=DECIMAL_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void DOUBLE_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM DOUBLE_TABLE --SPLICE-PROPERTIES index=DOUBLE_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM DOUBLE_TABLE --SPLICE-PROPERTIES index=DOUBLE_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void DOUBLE_PRECISION_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM DOUBLE_PRECISION_TABLE --SPLICE-PROPERTIES index=DOUBLE_PRECISION_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM DOUBLE_PRECISION_TABLE --SPLICE-PROPERTIES index=DOUBLE_PRECISION_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void FLOAT_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM FLOAT_TABLE --SPLICE-PROPERTIES index=FLOAT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM FLOAT_TABLE --SPLICE-PROPERTIES index=FLOAT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }
    @Test
    public void INTEGER_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM INTEGER_TABLE --SPLICE-PROPERTIES index=INTEGER_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM INTEGER_TABLE --SPLICE-PROPERTIES index=INTEGER_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }
    @Test
    public void NUMERIC_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM NUMERIC_TABLE --SPLICE-PROPERTIES index=NUMERIC_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM NUMERIC_TABLE --SPLICE-PROPERTIES index=NUMERIC_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }
    @Test
    public void REAL_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM REAL_TABLE --SPLICE-PROPERTIES index=REAL_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM REAL_TABLE --SPLICE-PROPERTIES index=REAL_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }
    @Test
    public void SMALLINT_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM SMALLINT_TABLE --SPLICE-PROPERTIES index=SMALLINT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM SMALLINT_TABLE --SPLICE-PROPERTIES index=SMALLINT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void BOOLEAN_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM BOOLEAN_TABLE --SPLICE-PROPERTIES index=BOOLEAN_INDEX\n WHERE I = true"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "true |true |\n" +
                "true |true |\n" +
                "true |true |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM BOOLEAN_TABLE --SPLICE-PROPERTIES index=BOOLEAN_INDEX\n WHERE I = true"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }
    */

}
