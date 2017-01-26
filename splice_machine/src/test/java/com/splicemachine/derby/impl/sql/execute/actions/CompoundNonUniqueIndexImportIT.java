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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Tests that ensure that data imported will be correctly indexed in multiple
 * circumstances.
 *
 * @author Scott Fines
 * Created on: 8/4/13
 */
@Ignore("Ignored because setup seems to break something")
public class CompoundNonUniqueIndexImportIT extends AbstractIndexTest{
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static String CLASS_NAME = CompoundNonUniqueIndexImportIT.class.getSimpleName().toUpperCase();
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static SpliceTableWatcher twoCtgColumns                          = new SpliceTableWatcher("TWO_CTG",               spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgColumnsAfter                     = new SpliceTableWatcher("TWO_CTG_AFTER",         spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgAscDescColumns                   = new SpliceTableWatcher("TWO_CTG_ASC_DESC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgAscDescColumnsAfter              = new SpliceTableWatcher("TWO_CTG_ASC_DESC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgDescAscColumns                   = new SpliceTableWatcher("TWO_CTG_DESC_ASC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoCtgDescAscColumnsAfter              = new SpliceTableWatcher("TWO_CTG_DESC_ASC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher twoNonCtgColumns                       = new SpliceTableWatcher("TWO_NONCTG",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgColumnsAfter                  = new SpliceTableWatcher("TWO_NONCTG_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgAscDescColumns                = new SpliceTableWatcher("TWO_NONCTG_ASC_DESC",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgAscDescColumnsAfter           = new SpliceTableWatcher("TWO_NONCTG_ASC_DESC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgDescAscColumns                = new SpliceTableWatcher("TWO_NONCTG_DESC_ASC",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoNonCtgDescAscColumnsAfter           = new SpliceTableWatcher("TWO_NONCTG_DESC_ASC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

    private static SpliceTableWatcher twoOutOfOrderNonCtgColumns             = new SpliceTableWatcher("TWO_NONCTG_OO",               spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgColumnsAfter        = new SpliceTableWatcher("TWO_NONCTG_OO_AFTER",         spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgAscDescColumns      = new SpliceTableWatcher("TWO_NONCTG_OO_ASC_DESC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgAscDescColumnsAfter = new SpliceTableWatcher("TWO_NONCTG_OO_ASC_DESC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgDescAscColumns      = new SpliceTableWatcher("TWO_NONCTG_OO_DESC_ASC",      spliceSchemaWatcher.schemaName,tableSchema);
    private static SpliceTableWatcher twoOutOfOrderNonCtgDescAscColumnsAfter = new SpliceTableWatcher("TWO_NONCTG_OO_DESC_ASC_AFTER",spliceSchemaWatcher.schemaName,tableSchema);

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

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCanInsertIntoTwoIndexColumns() throws Exception {
        createIndex(twoCtgColumns,"TWO_CTG_BEFORE","(b,c)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoCtgColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(twoCtgColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoCtgColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(twoCtgColumnsAfter,"TWO_CTG_AFTER","(b,c)");
        assertImportedDataCorrect(twoCtgColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAscDesc() throws Exception {
        createIndex(twoCtgAscDescColumns,"TWO_CTG_ASC_DESC_BEFORE","(b asc,c desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoCtgAscDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(twoCtgAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAscDescAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoCtgAscDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(twoCtgAscDescColumnsAfter,"TWO_CTG_ASC_DESC_AFTER","(b asc,c desc)");
        assertImportedDataCorrect(twoCtgAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsDescAsc() throws Exception {
        createIndex(twoCtgDescAscColumns,"TWO_CTG_DESC_ASC_BEFORE","(b desc,c asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoCtgDescAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(twoCtgAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsDescAscAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoCtgDescAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(twoCtgDescAscColumnsAfter,"TWO_CTG_DESC_ASC_AFTER","(b desc,c asc)");
        assertImportedDataCorrect(twoCtgAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumns() throws Exception {
        createIndex(twoNonCtgColumns,"TWO_NCTG_BEFORE","(a,c)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoNonCtgColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(twoNonCtgColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoNonCtgColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(twoNonCtgColumnsAfter,"TWO_NCTG_AFTER","(a,c)");
        assertImportedDataCorrect(twoNonCtgColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsAscDesc() throws Exception {
        createIndex(twoNonCtgAscDescColumns,"TWO_NCTG_ASC_DESC_BEFORE","(a asc,c desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoNonCtgAscDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(twoNonCtgAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsAscDescAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoNonCtgAscDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(twoNonCtgAscDescColumnsAfter,"TWO_NCTG_ASC_DESC_AFTER","(a asc,c desc)");
        assertImportedDataCorrect(twoNonCtgAscDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsDescAsc() throws Exception {
        createIndex(twoNonCtgDescAscColumns,"TWO_NCTG_DESC_ASC_BEFORE","(a desc,c asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoNonCtgDescAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(twoNonCtgDescAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsDescAscAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoNonCtgDescAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(twoNonCtgDescAscColumnsAfter,"TWO_NCTG_DESC_ASC_AFTER","(a desc,c asc)");
        assertImportedDataCorrect(twoNonCtgDescAscColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrder() throws Exception {
        createIndex(twoOutOfOrderNonCtgColumns,"TWO_NCTG_OO_BEFORE","(d,b)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoOutOfOrderNonCtgColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(twoOutOfOrderNonCtgColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoOutOfOrderNonCtgColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(twoOutOfOrderNonCtgColumnsAfter,"TWO_NCTG_OO_AFTER","(d,b)");
        assertImportedDataCorrect(twoOutOfOrderNonCtgColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderAscDesc() throws Exception {
        createIndex(twoOutOfOrderNonCtgAscDescColumns,"TWO_NCTG_OO_ASC_DESC_BEFORE","(d asc,b desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoOutOfOrderNonCtgAscDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(twoOutOfOrderNonCtgColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderAscDescAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoOutOfOrderNonCtgAscDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(twoOutOfOrderNonCtgAscDescColumnsAfter,"TWO_NCTG_OO_ASC_DESC_AFTER","(d asc,b desc)");
        assertImportedDataCorrect(twoOutOfOrderNonCtgAscDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderDescAsc() throws Exception {
        createIndex(twoOutOfOrderNonCtgDescAscColumns,"TWO_NCTG_OO_ASC_DESC_BEFORE","(d desc,b asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoOutOfOrderNonCtgDescAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(twoOutOfOrderNonCtgDescAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrderDescAscAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,twoOutOfOrderNonCtgDescAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(twoOutOfOrderNonCtgDescAscColumnsAfter,"TWO_NCTG_OO_DESC_ASC_AFTER","(d desc,b asc)");
        assertImportedDataCorrect(twoOutOfOrderNonCtgDescAscColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgIndexColumns() throws Exception {
        createIndex(threeCtgColumns,"THREE_CTG_BEFORE","(a,b,c)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeCtgColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgIndexColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeCtgColumnsAfter,"THREE_CTG_AFTER","(a,b,c)");
        assertImportedDataCorrect(threeCtgColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgAscAscDescColumns() throws Exception {
        createIndex(threeCtgAscAscDescColumns,"THREE_CTG_AAD_BEFORE","(a asc,b asc,c desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgAscAscDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeCtgAscAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgAscAscDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgAscAscDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeCtgAscAscDescColumnsAfter,"THREE_CTG_AAD_AFTER","(a asc,b asc ,c desc)");
        assertImportedDataCorrect(threeCtgAscAscDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgAscDescAscColumns() throws Exception {
        createIndex(threeCtgAscDescAscColumns,"THREE_CTG_ADA_BEFORE","(a asc,b desc,c asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgAscDescAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeCtgAscDescAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgAscDescAscColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgAscDescAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeCtgAscDescAscColumnsAfter,"THREE_CTG_ADA_AFTER","(a asc,b desc,c asc)");
        assertImportedDataCorrect(threeCtgAscAscDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgDescAscAscColumns() throws Exception {
        createIndex(threeCtgDescAscAscColumns,"THREE_CTG_DAA_BEFORE","(a desc,b asc,c asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgDescAscAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeCtgDescAscAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgDescAscAscColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgDescAscAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeCtgDescAscAscColumnsAfter,"THREE_CTG_DAA_AFTER","(a desc,b asc,c asc)");
        assertImportedDataCorrect(threeCtgDescAscAscColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgDescDescAscColumns() throws Exception {
        createIndex(threeCtgDescDescAscColumns,"THREE_CTG_DDA_BEFORE","(a desc,b desc,c asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgDescDescAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeCtgDescDescAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgDescDescAscColumnsAfterInsertion() throws Exception {
        insertData(3,threeCtgDescDescAscColumnsAfter.tableName);
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgDescDescAscColumnsAfter.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeCtgDescDescAscColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgDescAscDescColumns() throws Exception {
        createIndex(threeCtgDescAscDescColumns,"THREE_CTG_DAD_BEFORE","(a desc,b asc,c desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgDescAscDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeCtgDescAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgDescAscDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgDescAscDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeCtgDescAscDescColumnsAfter,"THREE_CTG_DAD_AFTER","(a desc,b asc,c desc)");
        assertImportedDataCorrect(threeCtgDescAscDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgAscDescDescColumns() throws Exception {
        createIndex(threeCtgAscDescDescColumns,"THREE_CTG_ADD_BEFORE","(a asc,b desc,c desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgAscDescDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeCtgAscDescDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgAscDescDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgAscDescDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeCtgAscDescDescColumnsAfter,"THREE_CTG_ADD_AFTER","(a asc,b desc,c desc)");
        assertImportedDataCorrect(threeCtgAscDescDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgDescDescDescColumns() throws Exception {
        createIndex(threeCtgDescDescDescColumns,"THREE_CTG_DDD_BEFORE","(a desc,b desc,c desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgDescDescDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeCtgDescDescDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeCtgDescDescDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeCtgDescDescDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeCtgDescDescDescColumnsAfter,"THREE_CTG_DDD_AFTER","(a desc,b desc,c desc)");
        assertImportedDataCorrect(threeCtgDescDescDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgIndexColumns() throws Exception {
        createIndex(threeNonCtgColumns,"THREE_NCTG_BEFORE","(a,c,d)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeNonCtgColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgIndexColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeNonCtgColumnsAfter,"THREE_NCTG_AFTER","(a,c,d)");
        assertImportedDataCorrect(threeNonCtgColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscAscDescColumns() throws Exception {
        createIndex(threeNonCtgAscAscDescColumns,"THREE_NCTG_AAD_BEFORE","(a asc,c asc,d desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgAscAscDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeNonCtgAscAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscAscDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgAscAscDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeNonCtgAscAscDescColumnsAfter,"THREE_NCTG_AAD_AFTER","(a asc,c asc,d desc)");
        assertImportedDataCorrect(threeNonCtgAscAscDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscDescAscColumns() throws Exception {
        createIndex(threeNonCtgAscDescAscColumns,"THREE_NCTG_ADA_BEFORE","(a asc,c desc,d asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgAscDescAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeNonCtgAscDescAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscDescAscColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgAscDescAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeNonCtgAscDescAscColumnsAfter,"THREE_NCTG_ADA_AFTER","(a asc,c desc,d asc)");
        assertImportedDataCorrect(threeNonCtgAscDescAscColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescAscAscColumns() throws Exception {
        createIndex(threeNonCtgDescAscAscColumns,"THREE_NCTG_DAA_BEFORE","(a desc,c asc,d asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgDescAscAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeNonCtgDescAscAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescAscAscColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgDescAscAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeNonCtgDescAscAscColumnsAfter,"THREE_NCTG_DAA_AFTER","(a desc,c asc,d asc)");
        assertImportedDataCorrect(threeNonCtgDescAscAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescDescAscColumns() throws Exception {
        createIndex(threeNonCtgDescDescAscColumns,"THREE_NCTG_DDA_BEFORE","(a desc,c desc,d asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgDescDescAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeNonCtgDescDescAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescDescAscColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgDescDescAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeNonCtgDescDescAscColumnsAfter,"THREE_NCTG_DDA_AFTER","(a desc,c desc,d asc)");
        assertImportedDataCorrect(threeNonCtgDescDescAscColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescAscDescColumns() throws Exception {
        createIndex(threeNonCtgDescAscDescColumns,"THREE_NCTG_DAD_BEFORE","(a desc,c asc,d desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgDescAscDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeNonCtgDescAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescAscDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgDescAscDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeNonCtgDescAscDescColumnsAfter,"THREE_NCTG_DAD_AFTER","(a desc,c asc,d desc)");
        assertImportedDataCorrect(threeNonCtgDescAscDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscDescDescColumns() throws Exception {
        createIndex(threeNonCtgAscDescDescColumns,"THREE_NCTG_ADD_BEFORE","(a asc,c desc,d desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgAscDescDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeNonCtgAscDescDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgAscDescDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgAscDescDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeNonCtgAscDescDescColumnsAfter,"THREE_NCTG_ADD_AFTER","(a asc,c desc,d desc)");
        assertImportedDataCorrect(threeNonCtgAscDescDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescDescDescColumns() throws Exception {
        createIndex(threeNonCtgDescDescDescColumns,"THREE_NCTG_DDD_BEFORE","(a desc,c desc,d desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgDescDescDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeNonCtgDescDescDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeNonCtgDescDescDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeNonCtgDescDescDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeNonCtgDescDescDescColumnsAfter,"THREE_NCTG_DDD_AFTER","(a desc,c desc,d desc)");
        assertImportedDataCorrect(threeNonCtgDescDescDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgIndexColumns() throws Exception {
        createIndex(threeOutOfOrderNonCtgColumns,"THREE_OO_NCTG_BEFORE","(c,a,d)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeOutOfOrderNonCtgColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgIndexColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeOutOfOrderNonCtgColumnsAfter,"THREE_OO_NCTG_AFTER","(c,a,d)");
        assertImportedDataCorrect(threeOutOfOrderNonCtgColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscAscDescColumns() throws Exception {
        createIndex(threeOutOfOrderNonCtgAscAscDescColumns,"THREE_OO_NCTG_AAD_BEFORE","(c asc,a asc,d desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgAscAscDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeOutOfOrderNonCtgAscAscDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscAscDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgAscAscDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeOutOfOrderNonCtgAscAscDescColumnsAfter,"THREE_OO_NCTG_AAD_AFTER","(c asc,a asc,d desc)");
        assertImportedDataCorrect(threeOutOfOrderNonCtgAscAscDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscDescAscColumns() throws Exception {
        createIndex(threeOutOfOrderNonCtgAscDescAscColumns,"THREE_OO_NCTG_ADA_BEFORE","(c asc,a desc,d asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgAscDescAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeOutOfOrderNonCtgAscDescAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscDescAscColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgAscDescAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeOutOfOrderNonCtgAscDescAscColumnsAfter,"THREE_OO_NCTG_ADA_AFTER","(c asc,a desc,d asc)");
        assertImportedDataCorrect(threeOutOfOrderNonCtgAscDescAscColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescAscAscColumns() throws Exception {
        createIndex(threeOutOfOrderNonCtgDescAscAscColumns,"THREE_OO_NCTG_DAA_BEFORE","(c desc,a asc,d asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgDescAscAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeOutOfOrderNonCtgDescAscAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescAscAscColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgDescAscAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeOutOfOrderNonCtgDescAscAscColumnsAfter,"THREE_OO_NCTG_DAA_AFTER","(c desc,a asc,d asc)");
        assertImportedDataCorrect(threeOutOfOrderNonCtgDescAscAscColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescDescAscColumns() throws Exception {
        createIndex(threeOutOfOrderNonCtgDescDescAscColumns,"THREE_OO_NCTG_DDA_BEFORE","(c desc,a desc,d asc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgDescDescAscColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeOutOfOrderNonCtgDescDescAscColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescDescAscColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgDescDescAscColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeOutOfOrderNonCtgDescDescAscColumnsAfter,"THREE_OO_NCTG_DDA_AFTER","(c desc,a desc,d asc)");
        assertImportedDataCorrect(threeOutOfOrderNonCtgDescDescAscColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescAscDescColumns() throws Exception {
        createIndex(threeOutOfOrderNonCtgDescAscDescColumns,"THREE_OO_NCTG_DAD_BEFORE","(c desc,a asc,d desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgDescAscDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeOutOfOrderNonCtgDescAscDescColumns.tableName,"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescAscDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgDescAscDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeOutOfOrderNonCtgDescAscDescColumnsAfter,"THREE_OO_NCTG_DAD_AFTER","(c desc,a asc,d desc)");
        assertImportedDataCorrect(threeOutOfOrderNonCtgDescAscDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscDescDescColumns() throws Exception {
        createIndex(threeOutOfOrderNonCtgAscDescDescColumns,"THREE_OO_NCTG_ADD_BEFORE","(c asc,a desc,d desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgAscDescDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeOutOfOrderNonCtgAscDescDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgAscDescDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgAscDescDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeOutOfOrderNonCtgAscDescDescColumnsAfter,"THREE_OO_NCTG_ADD_AFTER","(c asc,a desc,d desc)");
        assertImportedDataCorrect(threeOutOfOrderNonCtgAscDescDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescDescDescColumns() throws Exception {
        createIndex(threeOutOfOrderNonCtgDescDescDescColumns,"THREE_OO_NCTG_DDD_BEFORE","(c desc,a desc,d desc)");
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgDescDescDescColumns.tableName,"test_data/one_unique.csv");
        assertImportedDataCorrect(threeOutOfOrderNonCtgDescDescDescColumns.toString(),"test_data/one_unique.csv");
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgDescDescDescColumnsAfterInsertion() throws Exception {
        importData(methodWatcher,spliceSchemaWatcher.schemaName,threeOutOfOrderNonCtgDescDescDescColumnsAfter.tableName,"test_data/one_unique.csv");
        createIndex(threeOutOfOrderNonCtgDescDescDescColumnsAfter,"THREE_OO_NCTG_DDD_AFTER","(c desc,a desc,d desc)");
        assertImportedDataCorrect(threeOutOfOrderNonCtgDescDescDescColumnsAfter.toString(),"test_data/one_unique.csv");
    }
}

