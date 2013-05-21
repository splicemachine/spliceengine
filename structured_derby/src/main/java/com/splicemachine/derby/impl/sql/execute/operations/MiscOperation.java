package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.job.JobStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.utils.SpliceLogUtils;


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

public class MiscOperation extends NoRowsOperation
{
	private static Logger LOG = Logger.getLogger(MiscOperation.class);
	
	/**
     * Construct a MiscResultSet
	 *
	 *  @param activation		Describes run-time environment.
     */
	public MiscOperation(Activation activation) throws StandardException 
    {
		super(activation);
		recordConstructorTime(); 
	}
	
	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeScan");
		return new SpliceNoPutResultSet(activation,this,miscRowProvider,false);
	}
	
	private final RowProvider miscRowProvider = new RowProvider(){
		@Override public boolean hasNext() { return false; }

		@Override public ExecRow next() { return null; }

		@Override
		public void open() {
			SpliceLogUtils.trace(LOG, "open");
			try {
				setup();
				activation.getConstantAction().executeConstantAction(activation);
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, e);
			}
		}

		@Override
		public void close() {
			SpliceLogUtils.trace(LOG, "close for miscRowProvider, isOpen="+isOpen);
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
			}
		}

		@Override public RowLocation getCurrentRowLocation() { return null; }
        @Override public byte[] getTableName() { return null; }

        @Override
        public JobStats shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
            throw new UnsupportedOperationException();
        }

        @Override
		public int getModifiedRowCount() {
			return 0;
		}

		@Override
		public String toString(){
			return "MiscRowProvider";
		}
	};

    @Override
    public String toString() {
        return "ConstantActionOperation";
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "ConstantAction" + super.prettyPrint(indentLevel);
    }
}
