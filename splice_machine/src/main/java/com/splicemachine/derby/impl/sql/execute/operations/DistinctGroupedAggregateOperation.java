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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Pass through class to GroupedAggregateOperation...
 *
 * @see GroupedAggregateOperation
 *
 */
public class DistinctGroupedAggregateOperation extends GroupedAggregateOperation {
	private static Logger LOG = Logger.getLogger(DistinctGroupedAggregateOperation.class);

	/**
	 *
	 * No arg constructor, required for Serde.
	 *
	 */
    public DistinctGroupedAggregateOperation() {
		super();
	}

    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	isInSortedOrder	true if the source results are in sorted order
	 * @param	aggregateItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		AggregatorInfoList used by this routine.  
	 * @param	orderingItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		ColumOrdering array used by this routine
	 * @param	a				activation
	 * @param	ra				generated method to build an empty
	 *	 	output row 
	 * @param	maxRowSize		approx row size, passed to sorter
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 * @param 	nativeSparkMode Is native spark processing on, off, or should we
	 *                              use the system setting (from SConfiguration)?
	 * @exception StandardException Thrown on error
	 */
    public DistinctGroupedAggregateOperation(SpliceOperation s,
					boolean isInSortedOrder,
					int	aggregateItem,
					int	orderingItem,
					Activation a,
					GeneratedMethod ra,
					int maxRowSize,
					int resultSetNumber,
					double optimizerEstimatedRowCount,
					double optimizerEstimatedCost,
					boolean isRollup,
					int groupingIdColPosition,
					int groupingIdArrayItem,
					CompilerContext.NativeSparkModeType nativeSparkMode) throws StandardException {

		super(s, isInSortedOrder, aggregateItem, orderingItem,a, ra, maxRowSize, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost, isRollup,
				groupingIdColPosition, groupingIdArrayItem, nativeSparkMode);
		SpliceLogUtils.trace(LOG, "instance");
    }
}
