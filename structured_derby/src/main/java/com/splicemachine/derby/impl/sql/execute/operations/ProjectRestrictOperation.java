package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
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
	
	// Set in init method
	protected GeneratedMethod restriction;
	protected GeneratedMethod projection;
	
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
    }
    
 
	@Override
    public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
        if (LOG.isTraceEnabled())
            LOG.trace("readExternal");
        super.readExternal(in);
        restrictionMethodName = readNullableString(in);
        projectionMethodName = readNullableString(in);
        constantRestrictionMethodName = readNullableString(in);
        mapRefItem = in.readInt();
        cloneMapItem = in.readInt();
        reuseResult = in.readBoolean();
        doesProjection = in.readBoolean();
        source = (SpliceOperation) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (LOG.isTraceEnabled())
            LOG.trace("writeExternal");
        super.writeExternal(out);
        writeNullableString(restrictionMethodName, out);
        writeNullableString(projectionMethodName, out);
        writeNullableString(constantRestrictionMethodName, out);
        out.writeInt(mapRefItem);
        out.writeInt(cloneMapItem);
        out.writeBoolean(reuseResult);
        out.writeBoolean(doesProjection);
        out.writeObject(source);
    }

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init called");
		super.init(context);
		source.init(context);
		try {
			// Allocate a result row if all of the columns are mapped from the source			

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
			}
			if (restrictionMethodName != null)
				restriction = statement.getActivationClass().getMethod(restrictionMethodName);
			if (projectionMethodName != null)
				projection = statement.getActivationClass().getMethod(projectionMethodName);

		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "init operation failed",e);
		}
	}


	@Override
	public List<SpliceOperation> getSubOperations() {
		if (LOG.isTraceEnabled())
			LOG.trace("getSubOperations");
		return Arrays.asList(source);
	}


	@Override
	public SpliceOperation getLeftOperation() {
		if (LOG.isTraceEnabled())
			LOG.trace("getLeftOperation " + (this.source).getClass());
		return this.source;
	}
	
	@Override
	public List<NodeType> getNodeTypes() {
		if (LOG.isTraceEnabled())
			LOG.trace("getNodeTypes");
		return nodeTypes;
	}

	private ExecRow doProjection(ExecRow sourceRow) {
		SpliceLogUtils.trace(LOG, "doProjection for %s utilizing projection method %s",sourceRow,projectionMethodName);
		ExecRow result;
		try {
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
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,"Error performing projection",e);
			return null;
		}
	}
	
	@Override
	public void cleanup() {
		if (LOG.isTraceEnabled())
			LOG.trace("cleanup");
	}
	@Override
	public NoPutResultSet executeScan() {
		SpliceLogUtils.trace(LOG, "executeScan");
		final List<SpliceOperation> operationStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(operationStack);
		SpliceLogUtils.trace(LOG, "operationStack=%s",operationStack);
		SpliceOperation regionOperation = operationStack.get(0);
		LOG.trace("regionOperation="+regionOperation);
		RowProvider provider;
        ExecRow fromResults = getExecRowDefinition();
//        ExecRow fromResults = null;
//        try{
//            fromResults = getFromResultDescription(activation.getResultDescription());
//        }catch(StandardException se){
//            SpliceLogUtils.logAndThrowRuntime(LOG,se);
//        }
        if (regionOperation.getNodeTypes().contains(NodeType.REDUCE) && this != regionOperation) {
			SpliceLogUtils.trace(LOG,"scanning Temp Table");
			provider = regionOperation.getReduceRowProvider(this,fromResults);
		} else {
			SpliceLogUtils.trace(LOG,"scanning Map Table");
			provider = regionOperation.getMapRowProvider(this,fromResults);
		}
		return new SpliceNoPutResultSet(activation,this, provider);
	}



    @Override
	public ExecRow getNextRowCore() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("getNextRowCore");
		ExecRow candidateRow = null;
		ExecRow result = null;
		boolean restrict = false;
		DataValueDescriptor restrictBoolean;

		/* Return null if open was short circuited by false constant expression */
//		if (shortCircuitOpen)
//		{
//			SpliceLogUtils.trace(LOG,"short circuited");
//			return result;
//		}

		do {
			candidateRow = source.getNextRowCore();

			SpliceLogUtils.trace(LOG, "candidateRow=%s",candidateRow);


			if (candidateRow != null) {
				/* If restriction is null, then all rows qualify */
				if (restriction == null) {
					restrict = true;
				}
				else {
					setCurrentRow(candidateRow);
					restrictBoolean = (DataValueDescriptor) restriction.invoke(activation);

					// if the result is null, we make it false --
					// so the row won't be returned.
					restrict = ((! restrictBoolean.isNull()) && restrictBoolean.getBoolean());
				}
				SpliceLogUtils.trace(LOG,"restricting row %s?%b",candidateRow,restrict);
			}
		} while ( (candidateRow != null) && (! restrict ) );
		if (candidateRow != null)  {
			result = doProjection(candidateRow);
		}
		/* Clear the current row, if null */
		else {
			clearCurrentRow();
		}
		currentRow = result;
		setCurrentRow(currentRow);
		return result;
	}

	@Override
	public ExecRow getExecRowDefinition() {
		SpliceLogUtils.trace(LOG, "getExecRowDefinition with source %s",source);
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
		if(source!=null) source.openCore();
	}
	@Override
	public int[] getRootAccessedCols() {
		if (source instanceof SpliceBaseOperation)
			return ((SpliceBaseOperation) source).getRootAccessedCols();
		throw new RuntimeException("Source of merge join not a SpliceBaseOperation, it is this " + source);
	}
	
}
