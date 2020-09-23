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
import com.splicemachine.test.LongerThanTwoMinutes;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Tests indices on compound (multi-key) non-unique indices
 * @author Scott Fines
 *         Created on: 8/1/13
 */
@Category(LongerThanTwoMinutes.class)
public class CompoundNonUniqueIndexIT extends AbstractIndexTest{
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static String CLASS_NAME = CompoundNonUniqueIndexIT.class.getSimpleName().toUpperCase();
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static SpliceTableWatcher twoContiguousColumns                          = new SpliceTableWatcher("TWO_CONTIGUOUS",               spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoContiguousColumnsAfter                     = new SpliceTableWatcher("TWO_CONTIGUOUS_AFTER",         spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoContiguousAscDescColumns                   = new SpliceTableWatcher("TWO_CONTIGUOUS_ASC_DESC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoContiguousAscDescColumnsAfter              = new SpliceTableWatcher("TWO_CONTIGUOUS_ASC_DESC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoContiguousDescAscColumns                   = new SpliceTableWatcher("TWO_CONTIGUOUS_DESC_ASC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoContiguousDescAscColumnsAfter              = new SpliceTableWatcher("TWO_CONTIGUOUS_DESC_ASC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher twoNonContiguousColumns                       = new SpliceTableWatcher("TWO_NONCONTIGUOUS",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonContiguousColumnsAfter                  = new SpliceTableWatcher("TWO_NONCONTIGUOUS_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonContiguousAscDescColumns                = new SpliceTableWatcher("TWO_NONCONTIGUOUS_ASC_DESC",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonContiguousAscDescColumnsAfter           = new SpliceTableWatcher("TWO_NONCONTIGUOUS_ASC_DESC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonContiguousDescAscColumns                = new SpliceTableWatcher("TWO_NONCONTIGUOUS_DESC_ASC",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonContiguousDescAscColumnsAfter           = new SpliceTableWatcher("TWO_NONCONTIGUOUS_DESC_ASC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher twoOutOfOrderNonContiguousColumns             = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER",               spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonContiguousColumnsAfter        = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_AFTER",         spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonContiguousAscDescColumns      = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_ASC_DESC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonContiguousAscDescColumnsAfter = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_ASC_DESC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonContiguousDescAscColumns      = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_DESC_ASC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonContiguousDescAscColumnsAfter = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER_DESC_ASC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher threeContiguousColumns                     = new SpliceTableWatcher("THREE_CTG",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousColumnsAfter                = new SpliceTableWatcher("THREE_CTG_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousAscAscDescColumns           = new SpliceTableWatcher("THREE_CTG_AAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousAscAscDescColumnsAfter      = new SpliceTableWatcher("THREE_CTG_AAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousAscDescAscColumns           = new SpliceTableWatcher("THREE_CTG_ADA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousAscDescAscColumnsAfter      = new SpliceTableWatcher("THREE_CTG_ADA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousDescAscAscColumns           = new SpliceTableWatcher("THREE_CTG_DAA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousDescAscAscColumnsAfter      = new SpliceTableWatcher("THREE_CTG_DAA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousDescDescAscColumns          = new SpliceTableWatcher("THREE_CTG_DDA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousDescDescAscColumnsAfter     = new SpliceTableWatcher("THREE_CTG_DDA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousDescAscDescColumns          = new SpliceTableWatcher("THREE_CTG_DAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousDescAscDescColumnsAfter     = new SpliceTableWatcher("THREE_CTG_DAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousAscDescDescColumns          = new SpliceTableWatcher("THREE_CTG_ADD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousAscDescDescColumnsAfter     = new SpliceTableWatcher("THREE_CTG_ADD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousDescDescDescColumns         = new SpliceTableWatcher("THREE_CTG_DDD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeContiguousDescDescDescColumnsAfter    = new SpliceTableWatcher("THREE_CTG_DDD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher threeNonContiguousColumns                     = new SpliceTableWatcher("THREE_NCTG",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousColumnsAfter                = new SpliceTableWatcher("THREE_NCTG_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousAscAscDescColumns           = new SpliceTableWatcher("THREE_NCTG_AAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousAscAscDescColumnsAfter      = new SpliceTableWatcher("THREE_NCTG_AAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousAscDescAscColumns           = new SpliceTableWatcher("THREE_NCTG_ADA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousAscDescAscColumnsAfter      = new SpliceTableWatcher("THREE_NCTG_ADA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousDescAscAscColumns           = new SpliceTableWatcher("THREE_NCTG_DAA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousDescAscAscColumnsAfter      = new SpliceTableWatcher("THREE_NCTG_DAA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousDescDescAscColumns          = new SpliceTableWatcher("THREE_NCTG_DDA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousDescDescAscColumnsAfter     = new SpliceTableWatcher("THREE_NCTG_DDA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousDescAscDescColumns          = new SpliceTableWatcher("THREE_NCTG_DAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousDescAscDescColumnsAfter     = new SpliceTableWatcher("THREE_NCTG_DAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousAscDescDescColumns          = new SpliceTableWatcher("THREE_NCTG_ADD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousAscDescDescColumnsAfter     = new SpliceTableWatcher("THREE_NCTG_ADD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousDescDescDescColumns         = new SpliceTableWatcher("THREE_NCTG_DDD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeNonContiguousDescDescDescColumnsAfter    = new SpliceTableWatcher("THREE_NCTG_DDD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher threeOutOfOrderNonContiguousColumns                     = new SpliceTableWatcher("THREE_OO_NCTG",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousColumnsAfter                = new SpliceTableWatcher("THREE_OO_NCTG_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousAscAscDescColumns           = new SpliceTableWatcher("THREE_OO_NCTG_AAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousAscAscDescColumnsAfter      = new SpliceTableWatcher("THREE_OO_NCTG_AAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousAscDescAscColumns           = new SpliceTableWatcher("THREE_OO_NCTG_ADA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousAscDescAscColumnsAfter      = new SpliceTableWatcher("THREE_OO_NCTG_ADA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousDescAscAscColumns           = new SpliceTableWatcher("THREE_OO_NCTG_DAA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousDescAscAscColumnsAfter      = new SpliceTableWatcher("THREE_OO_NCTG_DAA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousDescDescAscColumns          = new SpliceTableWatcher("THREE_OO_NCTG_DDA",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousDescDescAscColumnsAfter     = new SpliceTableWatcher("THREE_OO_NCTG_DDA_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousDescAscDescColumns          = new SpliceTableWatcher("THREE_OO_NCTG_DAD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousDescAscDescColumnsAfter     = new SpliceTableWatcher("THREE_OO_NCTG_DAD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousAscDescDescColumns          = new SpliceTableWatcher("THREE_OO_NCTG_ADD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousAscDescDescColumnsAfter     = new SpliceTableWatcher("THREE_OO_NCTG_ADD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousDescDescDescColumns         = new SpliceTableWatcher("THREE_OO_NCTG_DDD",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher threeOutOfOrderNonContiguousDescDescDescColumnsAfter    = new SpliceTableWatcher("THREE_OO_NCTG_DDD_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(twoContiguousColumns)
            .around(twoContiguousColumnsAfter)
            .around(twoContiguousAscDescColumns)
            .around(twoContiguousAscDescColumnsAfter)
            .around(twoContiguousDescAscColumns)
            .around(twoContiguousDescAscColumnsAfter)
            .around(twoNonContiguousColumns)
            .around(twoNonContiguousColumnsAfter)
            .around(twoNonContiguousAscDescColumns)
            .around(twoNonContiguousAscDescColumnsAfter)
            .around(twoNonContiguousDescAscColumns)
            .around(twoNonContiguousDescAscColumnsAfter)
            .around(twoOutOfOrderNonContiguousColumns)
            .around(twoOutOfOrderNonContiguousColumnsAfter)
            .around(twoOutOfOrderNonContiguousAscDescColumns)
            .around(twoOutOfOrderNonContiguousAscDescColumnsAfter)
            .around(twoOutOfOrderNonContiguousDescAscColumns)
            .around(twoOutOfOrderNonContiguousDescAscColumnsAfter)
            .around(threeContiguousColumns)
            .around(threeContiguousColumnsAfter)
            .around(threeContiguousAscAscDescColumns)
            .around(threeContiguousAscAscDescColumnsAfter)
            .around(threeContiguousAscDescAscColumns)
            .around(threeContiguousAscDescAscColumnsAfter)
            .around(threeContiguousDescAscAscColumns)
            .around(threeContiguousDescAscAscColumnsAfter)
            .around(threeContiguousDescDescAscColumns)
            .around(threeContiguousDescDescAscColumnsAfter)
            .around(threeContiguousDescAscDescColumns)
            .around(threeContiguousDescAscDescColumnsAfter)
            .around(threeContiguousAscDescDescColumns)
            .around(threeContiguousAscDescDescColumnsAfter)
            .around(threeContiguousDescDescDescColumns)
            .around(threeContiguousDescDescDescColumnsAfter)
            .around(threeNonContiguousColumns)
            .around(threeNonContiguousColumnsAfter)
            .around(threeNonContiguousAscAscDescColumns)
            .around(threeNonContiguousAscAscDescColumnsAfter)
            .around(threeNonContiguousAscDescAscColumns)
            .around(threeNonContiguousAscDescAscColumnsAfter)
            .around(threeNonContiguousDescAscAscColumns)
            .around(threeNonContiguousDescAscAscColumnsAfter)
            .around(threeNonContiguousDescDescAscColumns)
            .around(threeNonContiguousDescDescAscColumnsAfter)
            .around(threeNonContiguousDescAscDescColumns)
            .around(threeNonContiguousDescAscDescColumnsAfter)
            .around(threeNonContiguousAscDescDescColumns)
            .around(threeNonContiguousAscDescDescColumnsAfter)
            .around(threeNonContiguousDescDescDescColumns)
            .around(threeNonContiguousDescDescDescColumnsAfter)
            .around(threeOutOfOrderNonContiguousColumns)
            .around(threeOutOfOrderNonContiguousColumnsAfter)
            .around(threeOutOfOrderNonContiguousAscAscDescColumns)
            .around(threeOutOfOrderNonContiguousAscAscDescColumnsAfter)
            .around(threeOutOfOrderNonContiguousAscDescAscColumns)
            .around(threeOutOfOrderNonContiguousAscDescAscColumnsAfter)
            .around(threeOutOfOrderNonContiguousDescAscAscColumns)
            .around(threeOutOfOrderNonContiguousDescAscAscColumnsAfter)
            .around(threeOutOfOrderNonContiguousDescDescAscColumns)
            .around(threeOutOfOrderNonContiguousDescDescAscColumnsAfter)
            .around(threeOutOfOrderNonContiguousDescAscDescColumns)
            .around(threeOutOfOrderNonContiguousDescAscDescColumnsAfter)
            .around(threeOutOfOrderNonContiguousAscDescDescColumns)
            .around(threeOutOfOrderNonContiguousAscDescDescColumnsAfter)
            .around(threeOutOfOrderNonContiguousDescDescDescColumns)
            .around(threeOutOfOrderNonContiguousDescDescDescColumnsAfter);


    @Test
    public void testCanInsertIntoTwoIndexColumns() throws Exception {
        createIndex(twoContiguousColumns,"TWO_CTG_BEFORE","(b,c)");
        insertData(3,twoContiguousColumns.toString());
        assertCorrectScan(3,twoContiguousColumns.toString());
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAfterInsertion() throws Exception {
        insertData(3,twoContiguousColumnsAfter.toString());
        createIndex(twoContiguousColumnsAfter,"TWO_CTG_AFTER","(b,c)");
        assertCorrectScan(3,twoContiguousColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAscDesc() throws Exception {
        createIndex(twoContiguousAscDescColumns,"TWO_CTG_ASC_DESC_BEFORE","(b asc,c desc)");
        insertData(3,twoContiguousAscDescColumns.toString());
        assertCorrectScan(3,twoContiguousAscDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAscDescAfterInsertion() throws Exception {
        insertData(3,twoContiguousAscDescColumnsAfter.toString());
        createIndex(twoContiguousAscDescColumnsAfter,"TWO_CTG_ASC_DESC_AFTER","(b asc,c desc)");
        assertCorrectScan(3,twoContiguousAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsDescAsc() throws Exception {
        createIndex(twoContiguousDescAscColumns,"TWO_CTG_DESC_ASC_BEFORE","(b desc,c asc)");
        insertData(3,twoContiguousDescAscColumns.toString());
        assertCorrectScan(3,twoContiguousDescAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsDescAscAfterInsertion() throws Exception {
        insertData(3,twoContiguousDescAscColumnsAfter.toString());
        createIndex(twoContiguousDescAscColumnsAfter,"TWO_CTG_DESC_ASC_AFTER","(b desc,c asc)");
        assertCorrectScan(3,twoContiguousDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumns() throws Exception {
        createIndex(twoNonContiguousColumns,"TWO_NCTG_BEFORE","(a,c)");
        insertData(3,twoNonContiguousColumns.toString());
        assertCorrectScan(3,twoNonContiguousColumns.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsAfterInsertion() throws Exception {
        insertData(3,twoNonContiguousColumnsAfter.toString());
        createIndex(twoNonContiguousColumnsAfter,"TWO_NCTG_AFTER","(a,c)");
        assertCorrectScan(3,twoNonContiguousColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsAscDesc() throws Exception {
        createIndex(twoNonContiguousAscDescColumns,"TWO_NCTG_ASC_DESC_BEFORE","(a asc,c desc)");
        insertData(3,twoNonContiguousAscDescColumns.toString());
        assertCorrectScan(3,twoNonContiguousAscDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsAscDescAfterInsertion() throws Exception {
        insertData(3,twoNonContiguousAscDescColumnsAfter.toString());
        createIndex(twoNonContiguousAscDescColumnsAfter,"TWO_NCTG_ASC_DESC_AFTER","(a asc,c desc)");
        assertCorrectScan(3,twoNonContiguousAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsDescAsc() throws Exception {
        createIndex(twoNonContiguousDescAscColumns,"TWO_NCTG_DESC_ASC_BEFORE","(a desc,c asc)");
        insertData(3,twoNonContiguousDescAscColumns.toString());
        assertCorrectScan(3,twoNonContiguousDescAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsDescAscAfterInsertion() throws Exception {
        insertData(3,twoNonContiguousDescAscColumnsAfter.toString());
        createIndex(twoNonContiguousDescAscColumnsAfter,"TWO_NCTG_DESC_ASC_AFTER","(a desc,c asc)");
        assertCorrectScan(3,twoNonContiguousDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsOutOfOrder() throws Exception {
        createIndex(twoOutOfOrderNonContiguousColumns,"TWO_NCTG_OO_BEFORE","(d,b)");
        insertData(3,twoOutOfOrderNonContiguousColumns.toString());
        assertCorrectScan(3,twoOutOfOrderNonContiguousColumns.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsOutOfOrderAfterInsertion() throws Exception {
        insertData(3,twoOutOfOrderNonContiguousColumnsAfter.toString());
        createIndex(twoOutOfOrderNonContiguousColumnsAfter,"TWO_NCTG_OO_AFTER","(d,b)");
        assertCorrectScan(3,twoOutOfOrderNonContiguousColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsOutOfOrderAscDesc() throws Exception {
        createIndex(twoOutOfOrderNonContiguousAscDescColumns,"TWO_NCTG_OO_ASC_DESC_BEFORE","(d asc,b desc)");
        insertData(3,twoOutOfOrderNonContiguousAscDescColumns.toString());
        assertCorrectScan(3,twoOutOfOrderNonContiguousAscDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsOutOfOrderAscDescAfterInsertion() throws Exception {
        insertData(3,twoOutOfOrderNonContiguousAscDescColumnsAfter.toString());
        createIndex(twoOutOfOrderNonContiguousAscDescColumnsAfter,"TWO_NCTG_OO_ASC_DESC_AFTER","(d asc,b desc)");
        assertCorrectScan(3,twoOutOfOrderNonContiguousAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsOutOfOrderDescAsc() throws Exception {
        createIndex(twoOutOfOrderNonContiguousDescAscColumns,"TWO_NCTG_OO_ASC_DESC_BEFORE","(d desc,b asc)");
        insertData(3,twoOutOfOrderNonContiguousDescAscColumns.toString());
        assertCorrectScan(3,twoOutOfOrderNonContiguousDescAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsOutOfOrderDescAscAfterInsertion() throws Exception {
        insertData(3,twoOutOfOrderNonContiguousDescAscColumnsAfter.toString());
        createIndex(twoOutOfOrderNonContiguousDescAscColumnsAfter,"TWO_NCTG_OO_DESC_ASC_AFTER","(d desc,b asc)");
        assertCorrectScan(3,twoOutOfOrderNonContiguousDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousIndexColumns() throws Exception {
        createIndex(threeContiguousColumns,"THREE_CTG_BEFORE","(a,b,c)");
        insertData(3,threeContiguousColumns.toString());
        assertCorrectScan(3,threeContiguousColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousIndexColumnsAfterInsertion() throws Exception {
        insertData(3,threeContiguousColumnsAfter.toString());
        createIndex(threeContiguousColumnsAfter,"THREE_CTG_AFTER","(a,b,c)");
        assertCorrectScan(3,threeContiguousColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousAscAscDescColumns() throws Exception {
        createIndex(threeContiguousAscAscDescColumns,"THREE_CTG_AAD_BEFORE","(a asc,b asc,c desc)");
        insertData(3,threeContiguousAscAscDescColumns.toString());
        assertCorrectScan(3,threeContiguousAscAscDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousAscAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeContiguousAscAscDescColumnsAfter.toString());
        createIndex(threeContiguousAscAscDescColumnsAfter,"THREE_CTG_AAD_AFTER","(a asc,b asc ,c desc)");
        assertCorrectScan(3,threeContiguousAscAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousAscDescAscColumns() throws Exception {
        createIndex(threeContiguousAscDescAscColumns,"THREE_CTG_ADA_BEFORE","(a asc,b desc,c asc)");
        insertData(3,threeContiguousAscDescAscColumns.toString());
        assertCorrectScan(3,threeContiguousAscDescAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousAscDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeContiguousAscDescAscColumnsAfter.toString());
        createIndex(threeContiguousAscDescAscColumnsAfter,"THREE_CTG_ADA_AFTER","(a asc,b desc,c asc)");
        assertCorrectScan(3,threeContiguousAscDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousDescAscAscColumns() throws Exception {
        createIndex(threeContiguousDescAscAscColumns,"THREE_CTG_DAA_BEFORE","(a desc,b asc,c asc)");
        insertData(3,threeContiguousDescAscAscColumns.toString());
        assertCorrectScan(3,threeContiguousDescAscAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousDescAscAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeContiguousDescAscAscColumnsAfter.toString());
        createIndex(threeContiguousDescAscAscColumnsAfter,"THREE_CTG_DAA_AFTER","(a desc,b asc,c asc)");
        assertCorrectScan(3,threeContiguousDescAscAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousDescDescAscColumns() throws Exception {
        createIndex(threeContiguousDescDescAscColumns,"THREE_CTG_DDA_BEFORE","(a desc,b desc,c asc)");
        insertData(3,threeContiguousDescDescAscColumns.toString());
        assertCorrectScan(3,threeContiguousDescDescAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousDescDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeContiguousDescDescAscColumnsAfter.toString());
        createIndex(threeContiguousDescDescAscColumnsAfter,"THREE_CTG_DDA_AFTER","(a desc,b desc,c asc)");
        assertCorrectScan(3,threeContiguousDescDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousDescAscDescColumns() throws Exception {
        createIndex(threeContiguousDescAscDescColumns,"THREE_CTG_DAD_BEFORE","(a desc,b asc,c desc)");
        insertData(3,threeContiguousDescAscDescColumns.toString());
        assertCorrectScan(3,threeContiguousDescAscDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousDescAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeContiguousDescAscDescColumnsAfter.toString());
        createIndex(threeContiguousDescAscDescColumnsAfter,"THREE_CTG_DAD_AFTER","(a desc,b asc,c desc)");
        assertCorrectScan(3,threeContiguousDescAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousAscDescDescColumns() throws Exception {
        createIndex(threeContiguousAscDescDescColumns,"THREE_CTG_ADD_BEFORE","(a asc,b desc,c desc)");
        insertData(3,threeContiguousAscDescDescColumns.toString());
        assertCorrectScan(3,threeContiguousAscDescDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousAscDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeContiguousAscDescDescColumnsAfter.toString());
        createIndex(threeContiguousAscDescDescColumnsAfter,"THREE_CTG_ADD_AFTER","(a asc,b desc,c desc)");
        assertCorrectScan(3,threeContiguousAscDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousDescDescDescColumns() throws Exception {
        createIndex(threeContiguousDescDescDescColumns,"THREE_CTG_DDD_BEFORE","(a desc,b desc,c desc)");
        insertData(3,threeContiguousDescDescDescColumns.toString());
        assertCorrectScan(3,threeContiguousDescDescDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeContiguousDescDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeContiguousDescDescDescColumnsAfter.toString());
        createIndex(threeContiguousDescDescDescColumnsAfter,"THREE_CTG_DDD_AFTER","(a desc,b desc,c desc)");
        assertCorrectScan(3,threeContiguousDescDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousIndexColumns() throws Exception {
        createIndex(threeNonContiguousColumns,"THREE_NCTG_BEFORE","(a,c,d)");
        insertData(3,threeNonContiguousColumns.toString());
        assertCorrectScan(3,threeNonContiguousColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousIndexColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonContiguousColumnsAfter.toString());
        createIndex(threeNonContiguousColumnsAfter,"THREE_NCTG_AFTER","(a,c,d)");
        assertCorrectScan(3,threeNonContiguousColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousAscAscDescColumns() throws Exception {
        createIndex(threeNonContiguousAscAscDescColumns,"THREE_NCTG_AAD_BEFORE","(a asc,c asc,d desc)");
        insertData(3,threeNonContiguousAscAscDescColumns.toString());
        assertCorrectScan(3,threeNonContiguousAscAscDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousAscAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonContiguousAscAscDescColumnsAfter.toString());
        createIndex(threeNonContiguousAscAscDescColumnsAfter,"THREE_NCTG_AAD_AFTER","(a asc,c asc,d desc)");
        assertCorrectScan(3,threeNonContiguousAscAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousAscDescAscColumns() throws Exception {
        createIndex(threeNonContiguousAscDescAscColumns,"THREE_NCTG_ADA_BEFORE","(a asc,c desc,d asc)");
        insertData(3,threeNonContiguousAscDescAscColumns.toString());
        assertCorrectScan(3,threeNonContiguousAscDescAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousAscDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonContiguousAscDescAscColumnsAfter.toString());
        createIndex(threeNonContiguousAscDescAscColumnsAfter,"THREE_NCTG_ADA_AFTER","(a asc,c desc,d asc)");
        assertCorrectScan(3,threeNonContiguousAscDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousDescAscAscColumns() throws Exception {
        createIndex(threeNonContiguousDescAscAscColumns,"THREE_NCTG_DAA_BEFORE","(a desc,c asc,d asc)");
        insertData(3,threeNonContiguousDescAscAscColumns.toString());
        assertCorrectScan(3,threeNonContiguousDescAscAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousDescAscAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonContiguousDescAscAscColumnsAfter.toString());
        createIndex(threeNonContiguousDescAscAscColumnsAfter,"THREE_NCTG_DAA_AFTER","(a desc,c asc,d asc)");
        assertCorrectScan(3,threeNonContiguousDescAscAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousDescDescAscColumns() throws Exception {
        createIndex(threeNonContiguousDescDescAscColumns,"THREE_NCTG_DDA_BEFORE","(a desc,c desc,d asc)");
        insertData(3,threeNonContiguousDescDescAscColumns.toString());
        assertCorrectScan(3,threeNonContiguousDescDescAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousDescDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonContiguousDescDescAscColumnsAfter.toString());
        createIndex(threeNonContiguousDescDescAscColumnsAfter,"THREE_NCTG_DDA_AFTER","(a desc,c desc,d asc)");
        assertCorrectScan(3,threeNonContiguousDescDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousDescAscDescColumns() throws Exception {
        createIndex(threeNonContiguousDescAscDescColumns,"THREE_NCTG_DAD_BEFORE","(a desc,c asc,d desc)");
        insertData(3,threeNonContiguousDescAscDescColumns.toString());
        assertCorrectScan(3,threeNonContiguousDescAscDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousDescAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonContiguousDescAscDescColumnsAfter.toString());
        createIndex(threeNonContiguousDescAscDescColumnsAfter,"THREE_NCTG_DAD_AFTER","(a desc,c asc,d desc)");
        assertCorrectScan(3,threeNonContiguousDescAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousAscDescDescColumns() throws Exception {
        createIndex(threeNonContiguousAscDescDescColumns,"THREE_NCTG_ADD_BEFORE","(a asc,c desc,d desc)");
        insertData(3,threeNonContiguousAscDescDescColumns.toString());
        assertCorrectScan(3,threeNonContiguousAscDescDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousAscDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonContiguousAscDescDescColumnsAfter.toString());
        createIndex(threeNonContiguousAscDescDescColumnsAfter,"THREE_NCTG_ADD_AFTER","(a asc,c desc,d desc)");
        assertCorrectScan(3,threeNonContiguousAscDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousDescDescDescColumns() throws Exception {
        createIndex(threeNonContiguousDescDescDescColumns,"THREE_NCTG_DDD_BEFORE","(a desc,c desc,d desc)");
        insertData(3,threeNonContiguousDescDescDescColumns.toString());
        assertCorrectScan(3,threeNonContiguousDescDescDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeNonContiguousDescDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeNonContiguousDescDescDescColumnsAfter.toString());
        createIndex(threeNonContiguousDescDescDescColumnsAfter,"THREE_NCTG_DDD_AFTER","(a desc,c desc,d desc)");
        assertCorrectScan(3,threeNonContiguousDescDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousIndexColumns() throws Exception {
        createIndex(threeOutOfOrderNonContiguousColumns,"THREE_OO_NCTG_BEFORE","(c,a,d)");
        insertData(3,threeOutOfOrderNonContiguousColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonContiguousColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousIndexColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonContiguousColumnsAfter.toString());
        createIndex(threeOutOfOrderNonContiguousColumnsAfter,"THREE_OO_NCTG_AFTER","(c,a,d)");
        assertCorrectScan(3,threeOutOfOrderNonContiguousColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousAscAscDescColumns() throws Exception {
        createIndex(threeOutOfOrderNonContiguousAscAscDescColumns,"THREE_OO_NCTG_AAD_BEFORE","(c asc,a asc,d desc)");
        insertData(3,threeOutOfOrderNonContiguousAscAscDescColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonContiguousAscAscDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousAscAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonContiguousAscAscDescColumnsAfter.toString());
        createIndex(threeOutOfOrderNonContiguousAscAscDescColumnsAfter,"THREE_OO_NCTG_AAD_AFTER","(c asc,a asc,d desc)");
        assertCorrectScan(3,threeOutOfOrderNonContiguousAscAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousAscDescAscColumns() throws Exception {
        createIndex(threeOutOfOrderNonContiguousAscDescAscColumns,"THREE_OO_NCTG_ADA_BEFORE","(c asc,a desc,d asc)");
        insertData(3,threeOutOfOrderNonContiguousAscDescAscColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonContiguousAscDescAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousAscDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonContiguousAscDescAscColumnsAfter.toString());
        createIndex(threeOutOfOrderNonContiguousAscDescAscColumnsAfter,"THREE_OO_NCTG_ADA_AFTER","(c asc,a desc,d asc)");
        assertCorrectScan(3,threeOutOfOrderNonContiguousAscDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousDescAscAscColumns() throws Exception {
        createIndex(threeOutOfOrderNonContiguousDescAscAscColumns,"THREE_OO_NCTG_DAA_BEFORE","(c desc,a asc,d asc)");
        insertData(3,threeOutOfOrderNonContiguousDescAscAscColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonContiguousDescAscAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousDescAscAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonContiguousDescAscAscColumnsAfter.toString());
        createIndex(threeOutOfOrderNonContiguousDescAscAscColumnsAfter,"THREE_OO_NCTG_DAA_AFTER","(c desc,a asc,d asc)");
        assertCorrectScan(3,threeOutOfOrderNonContiguousDescAscAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousDescDescAscColumns() throws Exception {
        createIndex(threeOutOfOrderNonContiguousDescDescAscColumns,"THREE_OO_NCTG_DDA_BEFORE","(c desc,a desc,d asc)");
        insertData(3,threeOutOfOrderNonContiguousDescDescAscColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonContiguousDescDescAscColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousDescDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonContiguousDescDescAscColumnsAfter.toString());
        createIndex(threeOutOfOrderNonContiguousDescDescAscColumnsAfter,"THREE_OO_NCTG_DDA_AFTER","(c desc,a desc,d asc)");
        assertCorrectScan(3,threeOutOfOrderNonContiguousDescDescAscColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousDescAscDescColumns() throws Exception {
        createIndex(threeOutOfOrderNonContiguousDescAscDescColumns,"THREE_OO_NCTG_DAD_BEFORE","(c desc,a asc,d desc)");
        insertData(3,threeOutOfOrderNonContiguousDescAscDescColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonContiguousDescAscDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousDescAscDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonContiguousDescAscDescColumnsAfter.toString());
        createIndex(threeOutOfOrderNonContiguousDescAscDescColumnsAfter,"THREE_OO_NCTG_DAD_AFTER","(c desc,a asc,d desc)");
        assertCorrectScan(3,threeOutOfOrderNonContiguousDescAscDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousAscDescDescColumns() throws Exception {
        createIndex(threeOutOfOrderNonContiguousAscDescDescColumns,"THREE_OO_NCTG_ADD_BEFORE","(c asc,a desc,d desc)");
        insertData(3,threeOutOfOrderNonContiguousAscDescDescColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonContiguousAscDescDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousAscDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonContiguousAscDescDescColumnsAfter.toString());
        createIndex(threeOutOfOrderNonContiguousAscDescDescColumnsAfter,"THREE_OO_NCTG_ADD_AFTER","(c asc,a desc,d desc)");
        assertCorrectScan(3,threeOutOfOrderNonContiguousAscDescDescColumnsAfter.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousDescDescDescColumns() throws Exception {
        createIndex(threeOutOfOrderNonContiguousDescDescDescColumns,"THREE_OO_NCTG_DDD_BEFORE","(c desc,a desc,d desc)");
        insertData(3,threeOutOfOrderNonContiguousDescDescDescColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonContiguousDescDescDescColumns.toString());
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonContiguousDescDescDescColumnsAfterInsertion() throws Exception {
        insertData(3,threeOutOfOrderNonContiguousDescDescDescColumnsAfter.toString());
        createIndex(threeOutOfOrderNonContiguousDescDescDescColumnsAfter,"THREE_OO_NCTG_DDD_AFTER","(c desc,a desc,d desc)");
        assertCorrectScan(3,threeOutOfOrderNonContiguousDescDescDescColumnsAfter.toString());
    }
}
