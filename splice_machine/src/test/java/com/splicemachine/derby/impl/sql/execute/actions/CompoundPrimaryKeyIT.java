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

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * @author Scott Fines
 *         Created on: 8/4/13
 */
public class CompoundPrimaryKeyIT extends AbstractIndexTest {
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static String CLASS_NAME = CompoundPrimaryKeyIT.class.getSimpleName().toUpperCase();
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static SpliceTableWatcher twoCtgColumns                = new SpliceTableWatcher("TWO_CONTIGUOUS",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(a,b))");
    private static SpliceTableWatcher twoNonCtgColumns             = new SpliceTableWatcher("TWO_NONCONTIGUOUS",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double,PRIMARY KEY(a,c))");
    private static SpliceTableWatcher twoOutOfOrderNonCtgColumns   = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(c,a))");
    private static SpliceTableWatcher threeCtgColumns              = new SpliceTableWatcher("THREE_CTG",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(b,c,d))");
    private static SpliceTableWatcher threeNonCtgColumns           = new SpliceTableWatcher("THREE_NCTG",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(a,c,d))");
    private static SpliceTableWatcher threeOutOfOrderNonCtgColumns = new SpliceTableWatcher("THREE_OO_NCTG",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(c,a,d))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(twoCtgColumns)
            .around(twoNonCtgColumns)
            .around(twoOutOfOrderNonCtgColumns)
            .around(threeCtgColumns)
            .around(threeNonCtgColumns)
            .around(threeOutOfOrderNonCtgColumns);


    @Test
    public void testCanInsertIntoTwoIndexColumns() throws Exception {
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
    public void testCanInsertIntoTwoNonCtgIndexColumns() throws Exception {
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
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrder() throws Exception {
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
    public void testCanInsertIntoThreeCtgIndexColumns() throws Exception {
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
    public void testCanInsertIntoThreeNonCtgIndexColumns() throws Exception {
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
    public void testCanInsertIntoThreeOutOfOrderNonCtgIndexColumns() throws Exception {
        insertData(3,threeOutOfOrderNonCtgColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }
}
