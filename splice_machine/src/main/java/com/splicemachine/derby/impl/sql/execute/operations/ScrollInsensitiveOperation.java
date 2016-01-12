package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.sql.execute.RowChanger;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.CursorActivation;
import com.splicemachine.derby.stream.function.ScrollInsensitiveFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;

/**
 *
 * TODO Implement Cursor Functionality JL
 *
 * Provide insensitive scrolling functionality for the underlying
 * result set.  We build a disk backed hash table of rows as the
 * user scrolls forward, with the position as the key.
 *
 * For read-only result sets the hash table will containg the
 * following columns:
 *<pre>
 *  +-------------------------------+
 *  | KEY                           |
 *  +-------------------------------+
 *  | Row                           |
 *  +-------------------------------+
 *</pre>
 * where key is the position of the row in the result set and row is the data.
 *
 * And for updatable result sets it will contain:
 * <pre>
 *  +-------------------------------+
 *  | KEY                           | [0]
 *  +-------------------------------+
 *  | RowLocation                   | [POS_ROWLOCATION]
 *  +-------------------------------+
 *  | Deleted                       | [POS_ROWDELETED]
 *  +-------------------------------+
 *  | Updated                       | [POS_ROWUPDATED]
 *  +-------------------------------+
 *  | Row                           | [extraColumns ... n]
 *  +-------------------------------+
 *</pre>
 * where key is the position of the row in the result set, rowLocation is
 * the row location of that row in the Heap, Deleted indicates whether the
 * row has been deleted, Updated indicates whether the row has been updated,
 * and row is the data.
 *
 */

public class ScrollInsensitiveOperation extends SpliceBaseOperation {
    private static Logger LOG = Logger.getLogger(ScrollInsensitiveOperation.class);
	protected int sourceRowWidth;
	protected SpliceOperation source;
	protected boolean scrollable;
    protected boolean keepAfterCommit;
    private int maxRows;
    protected StatementContext statementContext;
    private int positionInSource;
    private int currentPosition;
    private int lastPosition;
    private	boolean seenLast;
    private	boolean beforeFirst = true;
    private	boolean afterLast;


    /* Reference to the target result set. Target is used for updatable result
    * sets in order to keep the target result set on the same row as the
    * ScrollInsensitiveResultSet.
     */
    private CursorResultSet target;

    protected static final String NAME = ScrollInsensitiveOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}


    public ScrollInsensitiveOperation () {
    	super();
    }
    public ScrollInsensitiveOperation(SpliceOperation source,
			  Activation activation, int resultSetNumber,
			  int sourceRowWidth,
			  boolean scrollable,
			  double optimizerEstimatedRowCount,
			  double optimizerEstimatedCost) throws StandardException {
		super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.keepAfterCommit = activation.getResultSetHoldability();
        this.maxRows = activation.getMaxRows();
		this.sourceRowWidth = sourceRowWidth;
		this.source = source;
		this.scrollable = scrollable;
		recordConstructorTime();
        if (isForUpdate()) {
            target = ((CursorActivation)activation).getTargetResultSet();
        } else {
            target = null;
        }
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		if (LOG.isTraceEnabled())
			LOG.trace("readExternal");
		super.readExternal(in);
		sourceRowWidth = in.readInt();
		scrollable = in.readBoolean();
        keepAfterCommit = in.readBoolean();
        maxRows = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		if (LOG.isTraceEnabled())
			LOG.trace("writeExternal");
		super.writeExternal(out);
		out.writeInt(sourceRowWidth);
		out.writeBoolean(scrollable);
        out.writeBoolean(keepAfterCommit);
        out.writeInt(maxRows);
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		if (LOG.isTraceEnabled())
			LOG.trace("getSubOperations");
		List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
		operations.add((SpliceOperation) source);
		return operations;
	}

	@Override
	public SpliceOperation getLeftOperation() {
		if (LOG.isTraceEnabled())
			LOG.trace("getLeftOperation");
		return (SpliceOperation) source;
	}

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return ((SpliceOperation)source).getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return ((SpliceOperation)source).isReferencingTable(tableNumber);
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "ScrollInsensitive"; //this class is never used
    }

	public NoPutResultSet getSource() {
		return this.source;
	}

    public boolean isForUpdate() {
        return source.isForUpdate();
    }

    public void reopenCore() throws StandardException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"reopenCore");
        openCore();
    }

    public ExecRow getAbsoluteRow(int row) throws StandardException {
        checkIsOpen("absolute");
        attachStatementContext();
        return null;
    }

    public ExecRow getRelativeRow(int row)  throws StandardException {
        checkIsOpen("relative");
        attachStatementContext();
        return null;
    }

    public ExecRow	setBeforeFirstRow() {
        currentPosition = 0;
        beforeFirst = true;
        afterLast = false;
        currentRow = null;
        return null;
    }
    public ExecRow	getFirstRow() throws StandardException {
        checkIsOpen("first");
        attachStatementContext();
        return null;
    }

    public ExecRow	getNextRowCore() throws StandardException {
        checkIsOpen("next");
        return super.getNextRowCore();
    }


    public ExecRow	getPreviousRow() throws StandardException {
        checkIsOpen("previous");
        return null;
    }
    public ExecRow	getLastRow() throws StandardException {
        checkIsOpen("last");
        return null;
    }
    public ExecRow	setAfterLastRow() throws StandardException {
        return null;
    }

    public int getRowNumber() {
        return currentRow == null ? 0 : currentPosition;
    }

    protected void checkIsOpen(String name) throws StandardException {
        if (!isOpen)
            throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, name);
    }
    public RowLocation getRowLocation() throws StandardException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(source instanceof CursorResultSet, "source not CursorResultSet");
        return ( (CursorResultSet)source ).getRowLocation();
    }
    public void updateRow(ExecRow row, RowChanger rowChanger)
            throws StandardException {

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet<LocatedRow> sourceSet = source.getDataSet(dsp);
        try {
            operationContext.pushScope();
            dsp.setSchedulerPool("query");
            return sourceSet.map(new ScrollInsensitiveFunction(operationContext), true);
        }
        finally {
            operationContext.popScope();
        }
    }
    
    @Override
    public String getScopeName() {
        return "Produce Result Set";
    }
    

}
