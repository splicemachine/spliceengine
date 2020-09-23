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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Created on: 8/4/13
 */
@Ignore("Takes forever and uses up loads of memory")
public class CompoundUniqueIndexTest extends AbstractIndexTest {
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static String CLASS_NAME = CompoundUniqueIndexTest.class.getSimpleName().toUpperCase();
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static SpliceTableWatcher twoCtgColumns                          = new SpliceTableWatcher("TWO_CONTIGUOUS",               spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgColumnsAfter                     = new SpliceTableWatcher("TWO_CONTIGUOUS_AFTER",         spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgAscDescColumns                   = new SpliceTableWatcher("TWO_CONTIGUOUS_ASC_DESC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgAscDescColumnsAfter              = new SpliceTableWatcher("TWO_CONTIGUOUS_ASC_DESC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgDescAscColumns                   = new SpliceTableWatcher("TWO_CONTIGUOUS_DESC_ASC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgDescAscColumnsAfter              = new SpliceTableWatcher("TWO_CONTIGUOUS_DESC_ASC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher twoNonCtgColumns                       = new SpliceTableWatcher("TWO_NONCONTIGUOUS",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgColumnsAfter                  = new SpliceTableWatcher("TWO_NONCONTIGUOUS_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgAscDescColumns                = new SpliceTableWatcher("TWO_NONCONTIGUOUS_ASC_DESC",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgAscDescColumnsAfter           = new SpliceTableWatcher("TWO_NONCONTIGUOUS_ASC_DESC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgDescAscColumns                = new SpliceTableWatcher("TWO_NONCONTIGUOUS_DESC_ASC",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgDescAscColumnsAfter           = new SpliceTableWatcher("TWO_NONCONTIGUOUS_DESC_ASC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher twoOutOfOrderNonCtgColumns             = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER",               spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgColumnsAfter        = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_AFTER",         spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgAscDescColumns      = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_ASC_DESC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgAscDescColumnsAfter = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_ASC_DESC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgDescAscColumns      = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_DESC_ASC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgDescAscColumnsAfter = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_DESC_ASC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher threeCtgColumns                     = new SpliceTableWatcher("THREE_CTG",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgColumnsAfter                = new SpliceTableWatcher("THREE_CTG_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgAscAscDescColumns           = new SpliceTableWatcher("THREE_CTG_AAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgAscAscDescColumnsAfter      = new SpliceTableWatcher("THREE_CTG_AAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgAscDescAscColumns           = new SpliceTableWatcher("THREE_CTG_ADA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgAscDescAscColumnsAfter      = new SpliceTableWatcher("THREE_CTG_ADA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgDescAscAscColumns           = new SpliceTableWatcher("THREE_CTG_DAA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgDescAscAscColumnsAfter      = new SpliceTableWatcher("THREE_CTG_DAA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgDescDescAscColumns          = new SpliceTableWatcher("THREE_CTG_DDA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgDescDescAscColumnsAfter     = new SpliceTableWatcher("THREE_CTG_DDA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgDescAscDescColumns          = new SpliceTableWatcher("THREE_CTG_DAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgDescAscDescColumnsAfter     = new SpliceTableWatcher("THREE_CTG_DAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgAscDescDescColumns          = new SpliceTableWatcher("THREE_CTG_ADD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgAscDescDescColumnsAfter     = new SpliceTableWatcher("THREE_CTG_ADD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgDescDescDescColumns         = new SpliceTableWatcher("THREE_CTG_DDD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeCtgDescDescDescColumnsAfter    = new SpliceTableWatcher("THREE_CTG_DDD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher threeNonCtgColumns                     = new SpliceTableWatcher("THREE_NCTG",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgColumnsAfter                = new SpliceTableWatcher("THREE_NCTG_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgAscAscDescColumns           = new SpliceTableWatcher("THREE_NCTG_AAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgAscAscDescColumnsAfter      = new SpliceTableWatcher("THREE_NCTG_AAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgAscDescAscColumns           = new SpliceTableWatcher("THREE_NCTG_ADA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgAscDescAscColumnsAfter      = new SpliceTableWatcher("THREE_NCTG_ADA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgDescAscAscColumns           = new SpliceTableWatcher("THREE_NCTG_DAA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgDescAscAscColumnsAfter      = new SpliceTableWatcher("THREE_NCTG_DAA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgDescDescAscColumns          = new SpliceTableWatcher("THREE_NCTG_DDA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgDescDescAscColumnsAfter     = new SpliceTableWatcher("THREE_NCTG_DDA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgDescAscDescColumns          = new SpliceTableWatcher("THREE_NCTG_DAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgDescAscDescColumnsAfter     = new SpliceTableWatcher("THREE_NCTG_DAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgAscDescDescColumns          = new SpliceTableWatcher("THREE_NCTG_ADD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgAscDescDescColumnsAfter     = new SpliceTableWatcher("THREE_NCTG_ADD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgDescDescDescColumns         = new SpliceTableWatcher("THREE_NCTG_DDD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonCtgDescDescDescColumnsAfter    = new SpliceTableWatcher("THREE_NCTG_DDD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher threeOutOfOrderNonCtgColumns                     = new SpliceTableWatcher("THREE_OO_NCTG",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgColumnsAfter                = new SpliceTableWatcher("THREE_OO_NCTG_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgAscAscDescColumns           = new SpliceTableWatcher("THREE_OO_NCTG_AAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgAscAscDescColumnsAfter      = new SpliceTableWatcher("THREE_OO_NCTG_AAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgAscDescAscColumns           = new SpliceTableWatcher("THREE_OO_NCTG_ADA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgAscDescAscColumnsAfter      = new SpliceTableWatcher("THREE_OO_NCTG_ADA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgDescAscAscColumns           = new SpliceTableWatcher("THREE_OO_NCTG_DAA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgDescAscAscColumnsAfter      = new SpliceTableWatcher("THREE_OO_NCTG_DAA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgDescDescAscColumns          = new SpliceTableWatcher("THREE_OO_NCTG_DDA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgDescDescAscColumnsAfter     = new SpliceTableWatcher("THREE_OO_NCTG_DDA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgDescAscDescColumns          = new SpliceTableWatcher("THREE_OO_NCTG_DAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgDescAscDescColumnsAfter     = new SpliceTableWatcher("THREE_OO_NCTG_DAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgAscDescDescColumns          = new SpliceTableWatcher("THREE_OO_NCTG_ADD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgAscDescDescColumnsAfter     = new SpliceTableWatcher("THREE_OO_NCTG_ADD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgDescDescDescColumns         = new SpliceTableWatcher("THREE_OO_NCTG_DDD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonCtgDescDescDescColumnsAfter    = new SpliceTableWatcher("THREE_OO_NCTG_DDD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(twoCtgColumns)
            .around(twoCtgColumnsAfter)
            .around(twoCtgAscDescColumns)
            .around(twoCtgAscDescColumnsAfter)
            .around(twoCtgDescAscColumns)
            .around(twoCtgDescAscColumnsAfter)
            .around(twoNonCtgColumns)
            .around(twoNonCtgColumnsAfter)
            .around(twoNonCtgAscDescColumns)
            .around(twoNonCtgAscDescColumnsAfter)
            .around(twoNonCtgDescAscColumns)
            .around(twoNonCtgDescAscColumnsAfter)
            .around(twoOutOfOrderNonCtgColumns)
            .around(twoOutOfOrderNonCtgColumnsAfter)
            .around(twoOutOfOrderNonCtgAscDescColumns)
            .around(twoOutOfOrderNonCtgAscDescColumnsAfter)
            .around(twoOutOfOrderNonCtgDescAscColumns)
            .around(twoOutOfOrderNonCtgDescAscColumnsAfter)
            .around(threeCtgColumns)
            .around(threeCtgColumnsAfter)
            .around(threeCtgAscAscDescColumns)
            .around(threeCtgAscAscDescColumnsAfter)
            .around(threeCtgAscDescAscColumns)
            .around(threeCtgAscDescAscColumnsAfter)
            .around(threeCtgDescAscAscColumns)
            .around(threeCtgDescAscAscColumnsAfter)
            .around(threeCtgDescDescAscColumns)
            .around(threeCtgDescDescAscColumnsAfter)
            .around(threeCtgDescAscDescColumns)
            .around(threeCtgDescAscDescColumnsAfter)
            .around(threeCtgAscDescDescColumns)
            .around(threeCtgAscDescDescColumnsAfter)
            .around(threeCtgDescDescDescColumns)
            .around(threeCtgDescDescDescColumnsAfter)
            .around(threeNonCtgColumns)
            .around(threeNonCtgColumnsAfter)
            .around(threeNonCtgAscAscDescColumns)
            .around(threeNonCtgAscAscDescColumnsAfter)
            .around(threeNonCtgAscDescAscColumns)
            .around(threeNonCtgAscDescAscColumnsAfter)
            .around(threeNonCtgDescAscAscColumns)
            .around(threeNonCtgDescAscAscColumnsAfter)
            .around(threeNonCtgDescDescAscColumns)
            .around(threeNonCtgDescDescAscColumnsAfter)
            .around(threeNonCtgDescAscDescColumns)
            .around(threeNonCtgDescAscDescColumnsAfter)
            .around(threeNonCtgAscDescDescColumns)
            .around(threeNonCtgAscDescDescColumnsAfter)
            .around(threeNonCtgDescDescDescColumns)
            .around(threeNonCtgDescDescDescColumnsAfter)
            .around(threeOutOfOrderNonCtgColumns)
            .around(threeOutOfOrderNonCtgColumnsAfter)
            .around(threeOutOfOrderNonCtgAscAscDescColumns)
            .around(threeOutOfOrderNonCtgAscAscDescColumnsAfter)
            .around(threeOutOfOrderNonCtgAscDescAscColumns)
            .around(threeOutOfOrderNonCtgAscDescAscColumnsAfter)
            .around(threeOutOfOrderNonCtgDescAscAscColumns)
            .around(threeOutOfOrderNonCtgDescAscAscColumnsAfter)
            .around(threeOutOfOrderNonCtgDescDescAscColumns)
            .around(threeOutOfOrderNonCtgDescDescAscColumnsAfter)
            .around(threeOutOfOrderNonCtgDescAscDescColumns)
            .around(threeOutOfOrderNonCtgDescAscDescColumnsAfter)
            .around(threeOutOfOrderNonCtgAscDescDescColumns)
            .around(threeOutOfOrderNonCtgAscDescDescColumnsAfter)
            .around(threeOutOfOrderNonCtgDescDescDescColumns)
            .around(threeOutOfOrderNonCtgDescDescDescColumnsAfter);


    @Test
    public void testCanInsertIntoTwoIndexColumns() throws Exception {
        createUniqueIndex(twoCtgColumns,"TWO_CTG_BEFORE","(b,c)");
        insertData(3,twoCtgColumns.toString());
        assertCorrectScan(3,twoCtgColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,twoCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAfterInsertion() throws Exception {
        insertData(3,twoCtgColumnsAfter.toString());
        createUniqueIndex(twoCtgColumnsAfter,"TWO_CTG_AFTER","(b,c)");
        assertCorrectScan(3,twoCtgColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAscDesc() throws Exception {
        createUniqueIndex(twoCtgAscDescColumns,"TWO_CTG_ASC_DESC_BEFORE","(b asc,c desc)");
        insertData(3,twoCtgAscDescColumns.toString());
        assertCorrectScan(3,twoCtgAscDescColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,twoCtgAscDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAscDescAfterInsertion() throws Exception {
        insertData(3,twoCtgAscDescColumnsAfter.toString());
        createUniqueIndex(twoCtgAscDescColumnsAfter,"TWO_CTG_ASC_DESC_AFTER","(b asc,c desc)");
        assertCorrectScan(3,twoCtgAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsDescAsc() throws Exception {
        createUniqueIndex(twoCtgDescAscColumns,"TWO_CTG_DESC_ASC_BEFORE","(b desc,c asc)");
        insertData(3,twoCtgDescAscColumns.toString());
        assertCorrectScan(3,twoCtgDescAscColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,twoCtgDescAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsDescAscAfterInsertion() throws Exception {
        insertData(3,twoCtgDescAscColumnsAfter.toString());
        createUniqueIndex(twoCtgDescAscColumnsAfter,"TWO_CTG_DESC_ASC_AFTER","(b desc,c asc)");
        assertCorrectScan(3,twoCtgDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumns() throws Exception {
        createUniqueIndex(twoNonCtgColumns,"TWO_NCTG_BEFORE","(a,c)");
        insertData(3,twoNonCtgColumns.toString());
        assertCorrectScan(3,twoNonCtgColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,twoNonCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsAfterInsertion() throws Exception {
        insertData(3,twoNonCtgColumnsAfter.toString());
        createUniqueIndex(twoNonCtgColumnsAfter,"TWO_NCTG_AFTER","(a,c)");
        assertCorrectScan(3,twoNonCtgColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsAscDesc() throws Exception {
        createUniqueIndex(twoNonCtgAscDescColumns,"TWO_NCTG_ASC_DESC_BEFORE","(a asc,c desc)");
        insertData(3,twoNonCtgAscDescColumns.toString());
        assertCorrectScan(3,twoNonCtgAscDescColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,twoNonCtgAscDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsAscDescAfterInsertion() throws Exception {
        insertData(3,twoNonCtgAscDescColumnsAfter.toString());
        createUniqueIndex(twoNonCtgAscDescColumnsAfter,"TWO_NCTG_ASC_DESC_AFTER","(a asc,c desc)");
        assertCorrectScan(3,twoNonCtgAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsDescAsc() throws Exception {
        createUniqueIndex(twoNonCtgDescAscColumns,"TWO_NCTG_DESC_ASC_BEFORE","(a desc,c asc)");
        insertData(3,twoNonCtgDescAscColumns.toString());
        assertCorrectScan(3,twoNonCtgDescAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,twoNonCtgDescAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsDescAscAfterInsertion() throws Exception {
        insertData(3,twoNonCtgDescAscColumnsAfter.toString());
        createUniqueIndex(twoNonCtgDescAscColumnsAfter,"TWO_NCTG_DESC_ASC_AFTER","(a desc,c asc)");
        assertCorrectScan(3,twoNonCtgDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrder() throws Exception {
        createUniqueIndex(twoOutOfOrderNonCtgColumns,"TWO_NCTG_OO_BEFORE","(d,b)");
        insertData(3,twoOutOfOrderNonCtgColumns.toString());
        assertCorrectScan(3,twoOutOfOrderNonCtgColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,twoOutOfOrderNonCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderAfterInsertion() throws Exception {
        insertData(3,twoOutOfOrderNonCtgColumnsAfter.toString());
        createUniqueIndex(twoOutOfOrderNonCtgColumnsAfter,"TWO_NCTG_OO_AFTER","(d,b)");
        assertCorrectScan(3,twoOutOfOrderNonCtgColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderAscDesc() throws Exception {
        createUniqueIndex(twoOutOfOrderNonCtgAscDescColumns,"TWO_NCTG_OO_ASC_DESC_BEFORE","(d asc,b desc)");
        insertData(3,twoOutOfOrderNonCtgAscDescColumns.toString());
        assertCorrectScan(3,twoOutOfOrderNonCtgAscDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,twoOutOfOrderNonCtgAscDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderAscDescAfterInsertion() throws Exception {
        insertData(3,twoOutOfOrderNonCtgAscDescColumnsAfter.toString());
        createUniqueIndex(twoOutOfOrderNonCtgAscDescColumnsAfter,"TWO_NCTG_OO_ASC_DESC_AFTER","(d asc,b desc)");
        assertCorrectScan(3,twoOutOfOrderNonCtgAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderDescAsc() throws Exception {
        createUniqueIndex(twoOutOfOrderNonCtgDescAscColumns,"TWO_NCTG_OO_ASC_DESC_BEFORE","(d desc,b asc)");
        insertData(3,twoOutOfOrderNonCtgDescAscColumns.toString());
        assertCorrectScan(3,twoOutOfOrderNonCtgDescAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,twoOutOfOrderNonCtgDescAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderDescAscAfterInsertion() throws Exception {
        insertData(3,twoOutOfOrderNonCtgDescAscColumnsAfter.toString());
        createUniqueIndex(twoOutOfOrderNonCtgDescAscColumnsAfter,"TWO_NCTG_OO_DESC_ASC_AFTER","(d desc,b asc)");
        assertCorrectScan(3,twoOutOfOrderNonCtgDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeCtgIndexColumns() throws Exception {
        createUniqueIndex(threeCtgColumns,"THREE_CTG_BEFORE","(a,b,c)");
        insertData(3,threeCtgColumns.toString());
        assertCorrectScan(3,threeCtgColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,threeCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeCtgIndexColumnsAfterInsertion() throws Exception {
        insertData(3,threeCtgColumnsAfter.toString());
        createUniqueIndex(threeCtgColumnsAfter,"THREE_CTG_AFTER","(a,b,c)");
        assertCorrectScan(3,threeCtgColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeCtgAscAscDescColumns() throws Exception {
        createUniqueIndex(threeCtgAscAscDescColumns,"THREE_CTG_AAD_BEFORE","(a asc,b asc,c desc)");
        insertData(3,threeCtgAscAscDescColumns.toString());
        assertCorrectScan(3,threeCtgAscAscDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeCtgAscAscDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeCtgAscAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeCtgAscAscDescColumnsAfter.toString());
        createUniqueIndex(threeCtgAscAscDescColumnsAfter,"THREE_CTG_AAD_AFTER","(a asc,b asc ,c desc)");
        assertCorrectScan(3,threeCtgAscAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeCtgAscDescAscColumns() throws Exception {
        createUniqueIndex(threeCtgAscDescAscColumns,"THREE_CTG_ADA_BEFORE","(a asc,b desc,c asc)");
        insertData(3,threeCtgAscDescAscColumns.toString());
        assertCorrectScan(3,threeCtgAscDescAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeCtgAscDescAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeCtgAscDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeCtgAscDescAscColumnsAfter.toString());
        createUniqueIndex(threeCtgAscDescAscColumnsAfter,"THREE_CTG_ADA_AFTER","(a asc,b desc,c asc)");
        assertCorrectScan(3,threeCtgAscDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeCtgDescAscAscColumns() throws Exception {
        createUniqueIndex(threeCtgDescAscAscColumns,"THREE_CTG_DAA_BEFORE","(a desc,b asc,c asc)");
        insertData(3,threeCtgDescAscAscColumns.toString());
        assertCorrectScan(3,threeCtgDescAscAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeCtgDescAscAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeCtgDescAscAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeCtgDescAscAscColumnsAfter.toString());
        createUniqueIndex(threeCtgDescAscAscColumnsAfter,"THREE_CTG_DAA_AFTER","(a desc,b asc,c asc)");
        assertCorrectScan(3,threeCtgDescAscAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeCtgDescDescAscColumns() throws Exception {
        createUniqueIndex(threeCtgDescDescAscColumns,"THREE_CTG_DDA_BEFORE","(a desc,b desc,c asc)");
        insertData(3,threeCtgDescDescAscColumns.toString());
        assertCorrectScan(3,threeCtgDescDescAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeCtgDescDescAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeCtgDescDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeCtgDescDescAscColumnsAfter.toString());
        createUniqueIndex(threeCtgDescDescAscColumnsAfter,"THREE_CTG_DDA_AFTER","(a desc,b desc,c asc)");
        assertCorrectScan(3,threeCtgDescDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeCtgDescAscDescColumns() throws Exception {
        createUniqueIndex(threeCtgDescAscDescColumns,"THREE_CTG_DAD_BEFORE","(a desc,b asc,c desc)");
        insertData(3,threeCtgDescAscDescColumns.toString());
        assertCorrectScan(3,threeCtgDescAscDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeCtgDescAscDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeCtgDescAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeCtgDescAscDescColumnsAfter.toString());
        createUniqueIndex(threeCtgDescAscDescColumnsAfter,"THREE_CTG_DAD_AFTER","(a desc,b asc,c desc)");
        assertCorrectScan(3,threeCtgDescAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeCtgAscDescDescColumns() throws Exception {
        createUniqueIndex(threeCtgAscDescDescColumns,"THREE_CTG_ADD_BEFORE","(a asc,b desc,c desc)");
        insertData(3,threeCtgAscDescDescColumns.toString());
        assertCorrectScan(3,threeCtgAscDescDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeCtgAscDescDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeCtgAscDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeCtgAscDescDescColumnsAfter.toString());
        createUniqueIndex(threeCtgAscDescDescColumnsAfter,"THREE_CTG_ADD_AFTER","(a asc,b desc,c desc)");
        assertCorrectScan(3,threeCtgAscDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeCtgDescDescDescColumns() throws Exception {
        createUniqueIndex(threeCtgDescDescDescColumns,"THREE_CTG_DDD_BEFORE","(a desc,b desc,c desc)");
        insertData(3,threeCtgDescDescDescColumns.toString());
        assertCorrectScan(3,threeCtgDescDescDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeCtgDescDescDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeCtgDescDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeCtgDescDescDescColumnsAfter.toString());
        createUniqueIndex(threeCtgDescDescDescColumnsAfter,"THREE_CTG_DDD_AFTER","(a desc,b desc,c desc)");
        assertCorrectScan(3,threeCtgDescDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonCtgIndexColumns() throws Exception {
        createUniqueIndex(threeNonCtgColumns,"THREE_NCTG_BEFORE","(a,c,d)");
        insertData(3,threeNonCtgColumns.toString());
        assertCorrectScan(3,threeNonCtgColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeNonCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeNonCtgIndexColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonCtgColumnsAfter.toString());
        createUniqueIndex(threeNonCtgColumnsAfter,"THREE_NCTG_AFTER","(a,c,d)");
        assertCorrectScan(3,threeNonCtgColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscAscDescColumns() throws Exception {
        createUniqueIndex(threeNonCtgAscAscDescColumns,"THREE_NCTG_AAD_BEFORE","(a asc,c asc,d desc)");
        insertData(3,threeNonCtgAscAscDescColumns.toString());
        assertCorrectScan(3,threeNonCtgAscAscDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeNonCtgAscAscDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonCtgAscAscDescColumnsAfter.toString());
        createUniqueIndex(threeNonCtgAscAscDescColumnsAfter,"THREE_NCTG_AAD_AFTER","(a asc,c asc,d desc)");
        assertCorrectScan(3,threeNonCtgAscAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscDescAscColumns() throws Exception {
        createUniqueIndex(threeNonCtgAscDescAscColumns,"THREE_NCTG_ADA_BEFORE","(a asc,c desc,d asc)");
        insertData(3,threeNonCtgAscDescAscColumns.toString());
        assertCorrectScan(3,threeNonCtgAscDescAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeNonCtgAscDescAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonCtgAscDescAscColumnsAfter.toString());
        createUniqueIndex(threeNonCtgAscDescAscColumnsAfter,"THREE_NCTG_ADA_AFTER","(a asc,c desc,d asc)");
        assertCorrectScan(3,threeNonCtgAscDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescAscAscColumns() throws Exception {
        createUniqueIndex(threeNonCtgDescAscAscColumns,"THREE_NCTG_DAA_BEFORE","(a desc,c asc,d asc)");
        insertData(3,threeNonCtgDescAscAscColumns.toString());
        assertCorrectScan(3,threeNonCtgDescAscAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeNonCtgDescAscAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescAscAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonCtgDescAscAscColumnsAfter.toString());
        createUniqueIndex(threeNonCtgDescAscAscColumnsAfter,"THREE_NCTG_DAA_AFTER","(a desc,c asc,d asc)");
        assertCorrectScan(3,threeNonCtgDescAscAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescDescAscColumns() throws Exception {
        createUniqueIndex(threeNonCtgDescDescAscColumns,"THREE_NCTG_DDA_BEFORE","(a desc,c desc,d asc)");
        insertData(3,threeNonCtgDescDescAscColumns.toString());
        assertCorrectScan(3,threeNonCtgDescDescAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeNonCtgDescDescAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonCtgDescDescAscColumnsAfter.toString());
        createUniqueIndex(threeNonCtgDescDescAscColumnsAfter,"THREE_NCTG_DDA_AFTER","(a desc,c desc,d asc)");
        assertCorrectScan(3,threeNonCtgDescDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescAscDescColumns() throws Exception {
        createUniqueIndex(threeNonCtgDescAscDescColumns,"THREE_NCTG_DAD_BEFORE","(a desc,c asc,d desc)");
        insertData(3,threeNonCtgDescAscDescColumns.toString());
        assertCorrectScan(3,threeNonCtgDescAscDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeNonCtgDescAscDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonCtgDescAscDescColumnsAfter.toString());
        createUniqueIndex(threeNonCtgDescAscDescColumnsAfter,"THREE_NCTG_DAD_AFTER","(a desc,c asc,d desc)");
        assertCorrectScan(3,threeNonCtgDescAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscDescDescColumns() throws Exception {
        createUniqueIndex(threeNonCtgAscDescDescColumns,"THREE_NCTG_ADD_BEFORE","(a asc,c desc,d desc)");
        insertData(3,threeNonCtgAscDescDescColumns.toString());
        assertCorrectScan(3,threeNonCtgAscDescDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeNonCtgAscDescDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonCtgAscDescDescColumnsAfter.toString());
        createUniqueIndex(threeNonCtgAscDescDescColumnsAfter,"THREE_NCTG_ADD_AFTER","(a asc,c desc,d desc)");
        assertCorrectScan(3,threeNonCtgAscDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescDescDescColumns() throws Exception {
        createUniqueIndex(threeNonCtgDescDescDescColumns,"THREE_NCTG_DDD_BEFORE","(a desc,c desc,d desc)");
        insertData(3,threeNonCtgDescDescDescColumns.toString());
        assertCorrectScan(3,threeNonCtgDescDescDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeNonCtgDescDescDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonCtgDescDescDescColumnsAfter.toString());
        createUniqueIndex(threeNonCtgDescDescDescColumnsAfter,"THREE_NCTG_DDD_AFTER","(a desc,c desc,d desc)");
        assertCorrectScan(3,threeNonCtgDescDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgIndexColumns() throws Exception {
        createUniqueIndex(threeOutOfOrderNonCtgColumns,"THREE_OO_NCTG_BEFORE","(c,a,d)");
        insertData(3,threeOutOfOrderNonCtgColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgIndexColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonCtgColumnsAfter.toString());
        createUniqueIndex(threeOutOfOrderNonCtgColumnsAfter,"THREE_OO_NCTG_AFTER","(c,a,d)");
        assertCorrectScan(3,threeOutOfOrderNonCtgColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscAscDescColumns() throws Exception {
        createUniqueIndex(threeOutOfOrderNonCtgAscAscDescColumns,"THREE_OO_NCTG_AAD_BEFORE","(c asc,a asc,d desc)");
        insertData(3,threeOutOfOrderNonCtgAscAscDescColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgAscAscDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgAscAscDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonCtgAscAscDescColumnsAfter.toString());
        createUniqueIndex(threeOutOfOrderNonCtgAscAscDescColumnsAfter,"THREE_OO_NCTG_AAD_AFTER","(c asc,a asc,d desc)");
        assertCorrectScan(3,threeOutOfOrderNonCtgAscAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscDescAscColumns() throws Exception {
        createUniqueIndex(threeOutOfOrderNonCtgAscDescAscColumns,"THREE_OO_NCTG_ADA_BEFORE","(c asc,a desc,d asc)");
        insertData(3,threeOutOfOrderNonCtgAscDescAscColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgAscDescAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgAscDescAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonCtgAscDescAscColumnsAfter.toString());
        createUniqueIndex(threeOutOfOrderNonCtgAscDescAscColumnsAfter,"THREE_OO_NCTG_ADA_AFTER","(c asc,a desc,d asc)");
        assertCorrectScan(3,threeOutOfOrderNonCtgAscDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescAscAscColumns() throws Exception {
        createUniqueIndex(threeOutOfOrderNonCtgDescAscAscColumns,"THREE_OO_NCTG_DAA_BEFORE","(c desc,a asc,d asc)");
        insertData(3,threeOutOfOrderNonCtgDescAscAscColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgDescAscAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgDescAscAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescAscAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonCtgDescAscAscColumnsAfter.toString());
        createUniqueIndex(threeOutOfOrderNonCtgDescAscAscColumnsAfter,"THREE_OO_NCTG_DAA_AFTER","(c desc,a asc,d asc)");
        assertCorrectScan(3,threeOutOfOrderNonCtgDescAscAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescDescAscColumns() throws Exception {
        createUniqueIndex(threeOutOfOrderNonCtgDescDescAscColumns,"THREE_OO_NCTG_DDA_BEFORE","(c desc,a desc,d asc)");
        insertData(3,threeOutOfOrderNonCtgDescDescAscColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgDescDescAscColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgDescDescAscColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonCtgDescDescAscColumnsAfter.toString());
        createUniqueIndex(threeOutOfOrderNonCtgDescDescAscColumnsAfter,"THREE_OO_NCTG_DDA_AFTER","(c desc,a desc,d asc)");
        assertCorrectScan(3,threeOutOfOrderNonCtgDescDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescAscDescColumns() throws Exception {
        createUniqueIndex(threeOutOfOrderNonCtgDescAscDescColumns,"THREE_OO_NCTG_DAD_BEFORE","(c desc,a asc,d desc)");
        insertData(3,threeOutOfOrderNonCtgDescAscDescColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgDescAscDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgDescAscDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonCtgDescAscDescColumnsAfter.toString());
        createUniqueIndex(threeOutOfOrderNonCtgDescAscDescColumnsAfter,"THREE_OO_NCTG_DAD_AFTER","(c desc,a asc,d desc)");
        assertCorrectScan(3,threeOutOfOrderNonCtgDescAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscDescDescColumns() throws Exception {
        createUniqueIndex(threeOutOfOrderNonCtgAscDescDescColumns,"THREE_OO_NCTG_ADD_BEFORE","(c asc,a desc,d desc)");
        insertData(3,threeOutOfOrderNonCtgAscDescDescColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgAscDescDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgAscDescDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonCtgAscDescDescColumnsAfter.toString());
        createUniqueIndex(threeOutOfOrderNonCtgAscDescDescColumnsAfter,"THREE_OO_NCTG_ADD_AFTER","(c asc,a desc,d desc)");
        assertCorrectScan(3,threeOutOfOrderNonCtgAscDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescDescDescColumns() throws Exception {
        createUniqueIndex(threeOutOfOrderNonCtgDescDescDescColumns,"THREE_OO_NCTG_DDD_BEFORE","(c desc,a desc,d desc)");
        insertData(3,threeOutOfOrderNonCtgDescDescDescColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgDescDescDescColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgDescDescDescColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonCtgDescDescDescColumnsAfter.toString());
        createUniqueIndex(threeOutOfOrderNonCtgDescDescDescColumnsAfter,"THREE_OO_NCTG_DDD_AFTER","(c desc,a desc,d desc)");
        assertCorrectScan(3,threeOutOfOrderNonCtgDescDescDescColumnsAfter.toString());
    }

}
