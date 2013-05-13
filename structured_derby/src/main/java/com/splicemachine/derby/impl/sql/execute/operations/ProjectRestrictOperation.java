package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class ProjectRestrictOperation extends SpliceBaseOperation {

	private static Logger LOG = Logger.getLogger(ProjectRestrictOperation.class);
	protected String restrictionMethodName;
	protected String projectionMethodName;
	protected String constantRestrictionMethodName;
	protected int mapRefItem;
	protected int cloneMapItem;
	protected boolean reuseResult;
	protected boolean doesProjection;
	protected int[] projectMapping;
	protected ExecRow mappedResultRow;
	protected boolean[] cloneMap;
	protected boolean shortCircuitOpen;
	protected SpliceOperation source;
	protected static List<NodeType> nodeTypes;
    private boolean alwaysFalse;
	
	// Set in init method
	protected GeneratedMethod restriction;
	protected GeneratedMethod projection;
	
	public long restrictionTime;
	public long projectionTime;
	
	static {
		nodeTypes = Collections.singletonList(NodeType.MAP);
	}
	public ProjectRestrictOperation() {
		super();
	}

	public ProjectRestrictOperation(NoPutResultSet source,
																	Activation activation,
																	GeneratedMethod restriction,
																	GeneratedMethod projection,
																	int resultSetNumber,
																	GeneratedMethod cr,
																	int mapRefItem,
																	int cloneMapItem,
																	boolean reuseResult,
																	boolean doesProjection,
																	double optimizerEstimatedRowCount,
																	double optimizerEstimatedCost) throws StandardException {
		super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
		this.restrictionMethodName = (restriction == null) ? null : restriction.getMethodName();
		this.projectionMethodName = (projection == null) ? null : projection.getMethodName();
		this.constantRestrictionMethodName = (cr == null) ? null : cr.getMethodName();
		this.mapRefItem = mapRefItem;
		this.cloneMapItem = cloneMapItem;
		this.reuseResult = reuseResult;
		this.doesProjection = doesProjection;
		this.source = (SpliceOperation) source;
		init(SpliceOperationContext.newContext(activation));
		SpliceLogUtils.trace(LOG, "statisticsTimingOn="+statisticsTimingOn+",isTopResultSet="+isTopResultSet);
		recordConstructorTime();
    }
    
	public String getRestrictionMethodName() {
		return restrictionMethodName;
	}
 
	public boolean doesProjection() {
		return doesProjection;
	}
	
	@Override
    public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
        super.readExternal(in);
        restrictionMethodName = readNullableString(in);
        projectionMethodName = readNullableString(in);
        constantRestrictionMethodName = readNullableString(in);
        mapRefItem = in.readInt();
        cloneMapItem = in.readInt();
        reuseResult = in.readBoolean();
        doesProjection = in.readBoolean();
        source = (SpliceOperation) in.readObject();
        restrictionTime = in.readLong();
        projectionTime = in.readLong();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
        super.writeExternal(out);
        writeNullableString(restrictionMethodName, out);
        writeNullableString(projectionMethodName, out);
        writeNullableString(constantRestrictionMethodName, out);
        out.writeInt(mapRefItem);
        out.writeInt(cloneMapItem);
        out.writeBoolean(reuseResult);
        out.writeBoolean(doesProjection);
        out.writeObject(source);
        out.writeLong(restrictionTime);
        out.writeLong(projectionTime);
    }

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG, "init");
		super.init(context);
		source.init(context);

			GenericStorablePreparedStatement statement = context.getPreparedStatement();
			projectMapping = ((ReferencedColumnsDescriptorImpl) statement.getSavedObject(mapRefItem)).getReferencedColumnPositions();
			if (projectionMethodName == null) {
				mappedResultRow = activation.getExecutionFactory().getValueRow(projectMapping.length);
			}
			cloneMap = ((boolean[])statement.getSavedObject(cloneMapItem));
			if (this.constantRestrictionMethodName != null) {
				GeneratedMethod constantRestriction = statement.getActivationClass().getMethod(this.constantRestrictionMethodName);
				DataValueDescriptor restrictBoolean = (DataValueDescriptor) constantRestriction.invoke(activation);
				shortCircuitOpen  = (restrictBoolean == null) || ((restrictBoolean!=null)&&(! restrictBoolean.isNull()) && restrictBoolean.getBoolean());

                if(restrictBoolean != null && !restrictBoolean.isNull()){
                    alwaysFalse = !restrictBoolean.getBoolean();
                }else{
                    alwaysFalse = false;
                }

            }
			if (restrictionMethodName != null)
				restriction = statement.getActivationClass().getMethod(restrictionMethodName);
			if (projectionMethodName != null)
				projection = statement.getActivationClass().getMethod(projectionMethodName);
	}


	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG, "getSubOperations");
		return Arrays.asList(source);
	}


	@Override
	public SpliceOperation getLeftOperation() {
		SpliceLogUtils.trace(LOG, "getLeftOperation %s",source);
		return source;
	}
	
	@Override
	public List<NodeType> getNodeTypes() {
		SpliceLogUtils.trace(LOG, "getNodeTypes");
		return nodeTypes;
	}
	
	private ExecRow doProjection(ExecRow sourceRow) throws StandardException {
		SpliceLogUtils.trace(LOG, "doProjection");
		ExecRow result;
			if (projection != null) {
				result = (ExecRow) projection.invoke(activation);
			}
			else {
				result = mappedResultRow;
			}				
			// Copy any mapped columns from the source
			for (int index = 0; index < projectMapping.length; index++) {
				if (projectMapping[index] != -1) {
					DataValueDescriptor dvd = sourceRow.getColumn(projectMapping[index]);
					// See if the column has been marked for cloning.
					// If the value isn't a stream, don't bother cloning it.
					if (cloneMap[index] && dvd.hasStream()) {
						dvd = dvd.cloneValue(false);
					}
					result.setColumn(index + 1, dvd);
				}
			}

			/* We need to reSet the current row after doing the projection */
			setCurrentRow(result);
			/* Remember the result if reusing it */
			return result;
	}
	
	@Override
	public void cleanup() {
		if (LOG.isTraceEnabled())
			LOG.trace("cleanup");
	}
	
	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG, "executeScan");
		beginTime = getCurrentTimeMillis();
		final List<SpliceOperation> operationStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(operationStack);
		SpliceLogUtils.trace(LOG, "operationStack=%s",operationStack);
		SpliceOperation regionOperation = operationStack.get(0);
		LOG.trace("regionOperation="+regionOperation);
		RowProvider provider;
        ExecRow fromResults = getExecRowDefinition();
        if (regionOperation.getNodeTypes().contains(NodeType.REDUCE) && this != regionOperation) {
			SpliceLogUtils.trace(LOG,"scanning Temp Table");
			provider = regionOperation.getReduceRowProvider(this,fromResults);
		} else {
			SpliceLogUtils.trace(LOG,"scanning Map Table");
			provider = regionOperation.getMapRowProvider(this,fromResults);
		}
        SpliceNoPutResultSet rs =  new SpliceNoPutResultSet(activation,this, provider);
		nextTime += getCurrentTimeMillis() - beginTime;
		return rs;
	}


	private ExecRow candidateRow;
	private ExecRow result;
	private boolean restrict;
	private DataValueDescriptor restrictBoolean;

    @Override
	public ExecRow getNextRowCore() throws StandardException {

        if(alwaysFalse){
            return null;
        }

		candidateRow = null;
		result = null;
		restrict = false;
		restrictBoolean = null;
		long beginRT = 0;

		beginTime = getCurrentTimeMillis();
		do {
			candidateRow = source.getNextRowCore();

			SpliceLogUtils.trace(LOG, "candidateRow=%s",candidateRow);


			if (candidateRow != null) {
				beginRT = getCurrentTimeMillis();
				/* If restriction is null, then all rows qualify */
				if (restriction == null) {
					restrict = true;
				}
				else {
					setCurrentRow(candidateRow);
					restrictBoolean = (DataValueDescriptor) restriction.invoke(activation);
					restrictionTime += getElapsedMillis(beginRT);

					// if the result is null, we make it false --
					// so the row won't be returned.
					restrict = ((! restrictBoolean.isNull()) && restrictBoolean.getBoolean());
					if (! restrict)
					{
						rowsFiltered++;
					}
				}
				rowsSeen++;
				SpliceLogUtils.trace(LOG,"restricting row %s?%b",candidateRow,restrict);
			}
		} while ( (candidateRow != null) && (! restrict ) );
		if (candidateRow != null)  {
			beginRT = getCurrentTimeMillis();
			result = doProjection(candidateRow);
			projectionTime += getElapsedMillis(beginRT);
		}
		/* Clear the current row, if null */
		else {
			clearCurrentRow();
		}
		currentRow = result;
		setCurrentRow(currentRow);
		if (statisticsTimingOn)
		{
			if (! isTopResultSet)
			{
				/* This is simply for RunTimeStats */
				//TODO: need to getStatementContext() from somewhere
				if (activation.getLanguageConnectionContext().getStatementContext() == null) 
					SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
				else
					subqueryTrackingArray = activation.getLanguageConnectionContext().getStatementContext().getSubqueryTrackingArray();
			}
			nextTime += getElapsedMillis(beginTime);
		}
		return result;
	}

	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
//		SpliceLogUtils.trace(LOG, "getExecRowDefinition with source %s",source);
		ExecRow def = source.getExecRowDefinition();
        try {
            SpliceUtils.populateDefaultValues(def.getRowArray());
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
        source.setCurrentRow(def);
        return doProjection(def);
	}
	
	@Override
	public String toString() {
		return String.format("ProjectRestrictOperation {source=%s,resultSetNumber=%d}",source,resultSetNumber);
	}

	@Override
	public void openCore() throws StandardException {
        super.openCore();
		if(source!=null) source.openCore();
	}
	@Override
	public int[] getRootAccessedCols(long tableNumber) {
		return source.getRootAccessedCols(tableNumber);
	}

    @Override
    public boolean isReferencingTable(long tableNumber){
        return source.isReferencingTable(tableNumber);
    }

	public NoPutResultSet getSource() {
		return this.source;
	}
	@Override
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == CURRENT_RESULTSET_ONLY)
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		else
			return totTime;
	}
	@Override
	public void	close() throws StandardException
	{
		SpliceLogUtils.trace(LOG, "close in ProjectRestrict");
		/* Nothing to do if open was short circuited by false constant expression */
		if (shortCircuitOpen)
		{
			isOpen = false;
			shortCircuitOpen = false;
			source.close();
			return;
		}

		beginTime = getCurrentTimeMillis();
	    if ( isOpen ) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();

	        source.close();

			super.close();
	    }
		closeTime += getElapsedMillis(beginTime);
	}
}
