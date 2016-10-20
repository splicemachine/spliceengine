/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
