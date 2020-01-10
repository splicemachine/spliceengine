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

package com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLBit;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.AggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.WarningCollector;

import java.io.Externalizable;

/**
 * Provides information about a GroupedAggregate operation
 *
 * @author Scott Fines
 * Created on: 11/4/13
 */
public interface GroupedAggregateContext extends WarningCollector,Externalizable{

    void init(SpliceOperationContext context,AggregateContext genericAggregateContext) throws StandardException;

    int[] getGroupingKeys();

    boolean[] getGroupingKeyOrder();

    int[] getNonGroupedUniqueColumns();

    int getNumDistinctAggregates();

    SQLBit[] getGroupingIdVals();

    int getGroupingIdColumnPosition();
}
