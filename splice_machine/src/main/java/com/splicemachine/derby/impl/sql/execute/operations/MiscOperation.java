package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.actions.DDLConstantOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;

import org.apache.log4j.Logger;

/**
 * This is a wrapper class which invokes the Execution-time logic for
 * Misc statements. The real Execution-time logic lives inside the
 * executeConstantAction() method. Note that when re-using the
 * language result set tree across executions (DERBY-827) it is not
 * possible to store the ConstantAction as a member variable, because
 * a re-prepare of the statement will invalidate the stored
 * ConstantAction. Re-preparing a statement does not create a new
 * Activation unless the GeneratedClass has changed, so the existing
 * result set tree may survive a re-prepare.
 *
 * @author jessiezhang
 */

public class MiscOperation extends NoRowsOperation {
		private static Logger LOG = Logger.getLogger(MiscOperation.class);
	    protected static final String NAME = MiscOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		/**
		 * Construct a MiscResultSet
		 *
		 *  @param activation		Describes run-time environment.
		 */
		public MiscOperation(Activation activation) throws StandardException {
				super(activation);
				recordConstructorTime();
		}

    @Override
    public void close() throws StandardException {
        super.close();
        SpliceLogUtils.trace(LOG, "close for miscRowProvider, isOpen=%s", isOpen);
        if (!isOpen)
            return;
        try {
            int staLength = (subqueryTrackingArray == null) ? 0 : subqueryTrackingArray.length;

            for (int index = 0; index < staLength; index++) {
                if (subqueryTrackingArray[index] == null || subqueryTrackingArray[index].isClosed())
                    continue;

                subqueryTrackingArray[index].close();
            }

            isOpen = false;
            if (activation.isSingleExecution())
                activation.close();
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw Exceptions.parseException(e);
        }
    }

		@Override
		public String toString() {
				return "ConstantActionOperation";
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "ConstantAction" + super.prettyPrint(indentLevel);
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) {
				return null;
		}

		@Override
		public boolean isReferencingTable(long tableNumber) {
				return false;
		}

        @Override
        public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
            setup();
            activation.getConstantAction().executeConstantAction(activation);

            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int) activation.getRowsSeen()));
            
            // For DDL statements, use ControlDataSetProcessor even if
            // SparkDataSetProcessor was passed in as the 'dsp' argument
            // for the main operation logic. For DDL operation, this is
            // the end point and the value (typically a zero representing
            // number of rows) doesn't need to pass up the chain as
            // distributed data. Otherwise, you end up with an unnecessary
            // spark job which also appears in the UI.
            
            if (activation.getConstantAction() instanceof DDLConstantOperation)
                return StreamUtils.getControlDataSetProcessor().singleRowDataSet(new LocatedRow(valueRow));
            else
                return dsp.singleRowDataSet(new LocatedRow(valueRow));
        }
}
