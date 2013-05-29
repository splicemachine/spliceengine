package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;

public class WindowOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(WindowOperation.class);
    private GeneratedMethod restriction = null;
    private GeneratedMethod row;


    /**
     * Source result set,
     */
    public SpliceOperation source = null;


    /**
     * Cumulative time needed to evalute any restriction on this result set.
     */
    public long restrictionTime;

    private FormatableBitSet referencedColumns;
    private ExecRow allocatedRow;
    private long rownumber;

    /**
     *  Constructor
     *
     *  @param  activation          The activation
     *  @param  source              Source result set
     *  @param  rowAllocator
     *  @param  resultSetNumber     The resultSetNumber
     *  @param  erdNumber           Int for ResultDescription
	                                (so it can be turned back into an object)
     *  @param  restriction         Restriction
     *  @param  optimizerEstimatedRowCount  The optimizer's estimated number
     *                                      of rows.
     *  @param  optimizerEstimatedCost      The optimizer's estimated cost
     * @throws StandardException 
     */

    public WindowOperation(Activation activation,
        NoPutResultSet         source,
        GeneratedMethod        rowAllocator,
        int                    resultSetNumber,
        int                    erdNumber,
        GeneratedMethod        restriction,
        double                 optimizerEstimatedRowCount,
        double                 optimizerEstimatedCost) throws StandardException {
        super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
        this.restriction = restriction;
        this.source = (SpliceOperation) source;
        this.row = rowAllocator;
        this.allocatedRow = null;
        this.rownumber = 0;
        if (erdNumber != -1) {
            this.referencedColumns = (FormatableBitSet)(activation.getPreparedStatement().getSavedObject(erdNumber));
        }
        recordConstructorTime(); 
    }


    /**
     * Open this ResultSet.
     *
     * @exception StandardException thrown if cursor finished.
     */
    public void openCore() throws StandardException {
        super.openCore();
        /* Call into the source openCore() */
        if(source!=null) source.openCore();
        rownumber = 0;
    }

    /**
     * Reopen this ResultSet.
     *
     * @exception StandardException thrown if cursor finished.
     */
    public void reopenCore() throws StandardException {
        /* Reopen the source */
        source.reopenCore();
        rownumber = 0;
    }

    /**
     * Return the requested values computed from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * Restriction and projection parameters are evaluated for each row.
     *
     * @exception StandardException thrown on failure.
     * @exception StandardException ResultSetNotOpen thrown if not yet open.
     *
     * @return the next row in the result
     */
    public ExecRow getNextRowCore() throws StandardException {
        ExecRow sourceRow = null;
        ExecRow retval = null;
        boolean restrict = false;
        DataValueDescriptor restrictBoolean;
        long beginRT = 0;

        /*
         * Loop until we get a row from the source that qualifies, or there are
         * no more rows to qualify. For each iteration fetch a row from the
         * source, and evaluate against the restriction if any.
         */
        ExecRow tmpRow = null;

        do {
            sourceRow = source.getNextRowCore();
            if (sourceRow != null) {
                this.rownumber++;
                tmpRow = getAllocatedRow();
                populateFromSourceRow(sourceRow, tmpRow);
                setCurrentRow(tmpRow);

                /* Evaluate any restrictions */
                restrictBoolean = (DataValueDescriptor) ((restriction == null) ? null : restriction.invoke(activation));

                // if the result is null, we make it false --
                // so the row won't be returned.
                restrict = (restrictBoolean == null) ||
                    ((!restrictBoolean.isNull()) &&
                    restrictBoolean.getBoolean());

                if (!restrict) {
                    clearCurrentRow();
                }
                /* Update the run time statistics */
                retval = currentRow;
            } else {
                clearCurrentRow();
                retval = null;
            }
        } while ((sourceRow != null) && (!restrict));
        return retval;
    }

    /**
     * If the result set has been opened, close the open scan, else throw.
     *
     * @exception StandardException thrown on error
     */
    public void close() throws StandardException {
    	beginTime = getCurrentTimeMillis();

    	if (isOpen) {
    		clearCurrentRow();
    		source.close();
    		super.close();

    	} 

    	closeTime += getElapsedMillis(beginTime);
    }

    /**
     * Copy columns from srcrow into destrow, or insert ROW_NUMBER.
     * <p/>
     * <b>FIXME</b>
     * This is temporary. Window function treatment needs to generalized to
     * work for other window functions.
     *
     * @exception StandardException thrown on failure to open
     */
    public void populateFromSourceRow(ExecRow srcrow, ExecRow destrow) throws StandardException {
        int srcindex = 1;
        try {
            DataValueDescriptor[] columns = destrow.getRowArray();
            for (int index = 0; index < columns.length; index++) {

                if (referencedColumns != null &&
                        !referencedColumns.get(index)) {
                    columns[index].setValue((long)this.rownumber);
                } else {
                    destrow.setColumn(index+1, srcrow.getColumn(srcindex));
                    srcindex++;
                }
            }
        } catch (StandardException se) {
            throw se;
        } catch (Throwable t) {
            throw StandardException.unexpectedUserException(t);
        }
    }

    /**
     * Cache the ExecRow for this result set.
     *
     * @return The cached ExecRow for this ResultSet
     *
     * @exception StandardException thrown on failure.
     */
    private ExecRow getAllocatedRow()
        throws StandardException {

        if (allocatedRow == null) {
            allocatedRow = (ExecRow) row.invoke(activation);
        }

        return allocatedRow;
    }


	@Override
	public List<NodeType> getNodeTypes() {
		SpliceLogUtils.trace(LOG, "getNodeTypes");
		return Collections.singletonList(NodeType.SCAN);
	}
	
	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG, "getSubOperations");
		List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
		operations.add(source);
		return operations;
	}

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        if(source.isReferencingTable(tableNumber))
            return source.getRootAccessedCols(tableNumber);
        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber){
        return source.isReferencingTable(tableNumber);
    }
	
	@Override
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		else
			return totTime;
	}

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return new StringBuilder("Window:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("referenceColumns").append(referencedColumns)
                .append(indent).append("ronumber:").append(rownumber)
                .toString();
    }
}
