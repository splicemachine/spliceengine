package com.splicemachine.derby.impl.sql.execute.operations.framework;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

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
		private SpliceRuntimeContext spliceRuntimeContext;
		private SpliceOperation source;
		
		public SourceIterator(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation source) {
			this.spliceRuntimeContext = spliceRuntimeContext;
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
        public ExecRow next() throws StandardException, IOException {
            ExecRow execRow= source.nextRow(spliceRuntimeContext);
			SpliceBaseOperation.checkInterrupt();
            return execRow;
        }
}
