package com.splicemachine.derby.impl.sql.execute.operations.framework;

import java.io.IOException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.utils.StandardIterator;
/**
 * Iterator over the source provided utilizing the sources nextRow(SpliceRuntimeContext) method.
 * 
 * The opens and closes are no-ops.
 *
 */
public class SourceIterator implements StandardIterator<ExecRow> {
		private SpliceOperation source;
		private long rowsRead;
		public SourceIterator(SpliceOperation source) {
			this.source = source;
		}
		/**
		 * No-op
		 */
        @Override public void open() throws StandardException, IOException { }
        /**
         * No-Op
         */
        @Override public void close() throws StandardException, IOException { }
        /**
         * Retrieve the nextrow from the source.
         */
        @Override
        public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
            ExecRow execRow= source.getNextRowCore();
            rowsRead++;
			SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
            return execRow;
        }
}
