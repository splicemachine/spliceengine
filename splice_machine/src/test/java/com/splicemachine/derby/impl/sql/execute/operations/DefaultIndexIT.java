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
import org.spark_project.guava.base.Throwables;

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
    private static SpliceTableWatcher INDEX_AFTER_LOAD_BLANK_TABLE = new SpliceTableWatcher("INDEX_AFTER_LOAD_BLANK_TABLE", schemaWatcher.schemaName,"(i varchar(10) default '', j varchar(10))");
    private static SpliceTableWatcher INDEX_AFTER_LOAD_NULL_TABLE = new SpliceTableWatcher("INDEX_AFTER_LOAD_NULL_TABLE", schemaWatcher.schemaName,"(i varchar(10), j varchar(10))");
    private static SpliceTableWatcher BULK_HFILE_BLANK_TABLE = new SpliceTableWatcher("BULK_HFILE_BLANK_TABLE", schemaWatcher.schemaName,"(i varchar(10) default '', j varchar(10))");
    private static SpliceTableWatcher BULK_HFILE_NULL_TABLE = new SpliceTableWatcher("BULK_HFILE_NULL_TABLE", schemaWatcher.schemaName,"(i varchar(10), j varchar(10))");
    private static SpliceIndexWatcher BULK_HFILE_NULL_TABLE_IX = new SpliceIndexWatcher(BULK_HFILE_NULL_TABLE.tableName, schemaWatcher.schemaName, "BULK_HFILE_NULL_TABLE_IX",
            schemaWatcher.schemaName, "(i)",false,true,false);
    private static SpliceIndexWatcher BULK_HFILE_BLANK_TABLE_IX = new SpliceIndexWatcher(BULK_HFILE_BLANK_TABLE.tableName, schemaWatcher.schemaName, "BULK_HFILE_BLANK_TABLE_IX",
            schemaWatcher.schemaName, "(i)",false,false,true);


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
            .around(BULK_HFILE_BLANK_TABLE_IX);

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
    public void testBulkHFileImport() throws Exception {
        try {
            methodWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s',',','\"',null,null,null,0,null,true,null, '%s', false)", schemaWatcher.schemaName, BULK_HFILE_BLANK_TABLE.tableName
                    , getResourceDirectory() + "null_and_blanks.csv", getResourceDirectory() + "data")).execute();

            try {
                methodWatcher.executeQuery(String.format("SELECT * FROM BULK_HFILE_BLANK_TABLE --SPLICE-PROPERTIES index=BULK_HFILE_BLANK_TABLE_IX\n where i =''"));
                Assert.fail("did not throw exception");
            }
            catch (SQLException sqle) {
                Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
            }
            ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM BULK_HFILE_BLANK_TABLE --SPLICE-PROPERTIES index=BULK_HFILE_BLANK_TABLE_IX\n where i ='SD'"));


            Assert.assertEquals("I | J |\n" +
                    "--------\n" +
                    "SD |SD |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        }
        catch (Exception e) {
            java.lang.Throwable ex = Throwables.getRootCause(e);
            if (ex.getMessage().contains("bulk load not supported")) {
                // swallow (Control Tests)
            } else {
                throw e;
            }
        }
    }

}