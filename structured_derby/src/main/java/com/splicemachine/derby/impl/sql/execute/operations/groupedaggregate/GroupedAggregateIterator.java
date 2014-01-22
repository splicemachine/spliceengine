package com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate;

import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.utils.StandardIterator;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/23/14
 */
public abstract class GroupedAggregateIterator implements StandardIterator<GroupedRow> {
		protected final StandardIterator<ExecRow> source;
		protected final boolean isRollup;
		protected final int[] groupColumns;
		protected boolean completed = false;
		protected List<GroupedRow> evictedRows;
		protected long rowsRead;
		protected ExecRow[] rollupRows;

		protected GroupedAggregateIterator(StandardIterator<ExecRow> source,
																			 boolean isRollup,
																			 int[] groupColumns) {
				this.source = source;
				this.isRollup = isRollup;
				this.groupColumns = groupColumns;
		}

		public abstract long getRowsMerged();
		public abstract double getMaxFillRatio();
}
