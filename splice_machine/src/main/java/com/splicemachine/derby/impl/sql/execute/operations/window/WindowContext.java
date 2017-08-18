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

package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.Externalizable;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.WarningCollector;

/**
 * @author  jyuan on 7/25/14.
 */
public interface WindowContext extends WarningCollector,Externalizable {

    void init(SpliceOperationContext context) throws StandardException;

    /**
     * The list of window functions that will be executed on the columns
     * of each row.<br/>
     * If there is more than one <code>WindowAggregator</code> int this
     * list, it is because they all share identical <code>over()</code>
     * clauses.  They were batched up this way so that they can all be
     * applied to the same <code>ExecRow</code>.
     * @return the list of window functions to be applied to a given row.
     */
    WindowAggregator[] getWindowFunctions();

    ExecRow getSortTemplateRow() throws StandardException;

    ExecRow getSourceIndexRow();

    /**
     * All aggregators in this list of window functions will
     * use the same key columns.
     * @return the key column array for all functions in this collection.
     */
    int[] getKeyColumns();

    /**
     * All aggregators in this list of window functions will
     * use the same key orders.
     * @return the key orders array for all functions in this collection.
     */
    boolean[] getKeyOrders();

    /**
     * All aggregators in this list of window functions will
     * use the same partition.
     * @return the partition array for all functions in this collection.
     */
    int[] getPartitionColumns();

    FrameDefinition getFrameDefinition();

    int[] getSortColumns();
}
