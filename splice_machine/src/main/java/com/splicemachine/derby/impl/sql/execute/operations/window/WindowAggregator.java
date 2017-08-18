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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction;
import static com.splicemachine.db.iapi.sql.compile.AggregateDefinition.*;

/**
 * @author Jeff Cunningham
 *         Date: 9/15/14
 */
public interface WindowAggregator {
    void accumulate(ExecRow nextRow, ExecRow accumulatorRow) throws StandardException;

    void finish(ExecRow row) throws StandardException;

    boolean initialize(ExecRow row) throws StandardException;

    int getResultColumnId();

    int getFunctionColumnId();

    int[] getPartitionColumns();

    int[] getKeyColumns();

    int[] getSortColumns();

    boolean[] getKeyOrders();

    FrameDefinition getFrameDefinition();

    String getName();

    FunctionType getType();

    SpliceGenericWindowFunction getCachedAggregator();

    int[] getInputColumnIds();

    String getFunctionName();

    ColumnOrdering[] getOrderings();

    ColumnOrdering[] getPartitions();

    FormatableHashtable getFunctionSpecificArgs();
}
