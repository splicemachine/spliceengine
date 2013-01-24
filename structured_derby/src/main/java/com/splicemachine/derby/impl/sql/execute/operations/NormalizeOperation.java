package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;

public class NormalizeOperation extends SpliceBaseOperation {
	private static final Logger LOG = Logger.getLogger(NormalizeOperation.class);
	protected NoPutResultSet source;
	private ExecRow normalizedRow;
	private int numCols;
	private int startCol;
	private boolean forUpdate;
	private int erdNumber;
	
	private DataValueDescriptor[] cachedDestinations;
	
	private ResultDescription resultDescription;
	
	private DataTypeDescriptor[] desiredTypes;
	
	public NormalizeOperation(){
		super();
		SpliceLogUtils.trace(LOG,"instantiating without parameters");
	}
	
	public NormalizeOperation(NoPutResultSet source,
							 Activation activaation,
							 int resultSetNumber,
							 int erdNumber,
							 double optimizerEstimatedRowCount,
							 double optimizerEstimatedCost,
							 boolean forUpdate) throws StandardException{
		super(activaation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
		this.source = source;
        init(SpliceOperationContext.newContext(activation));
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		forUpdate = in.readBoolean();
		erdNumber = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeBoolean(forUpdate);
		out.writeInt(erdNumber);
	}

	@Override
	public void init(SpliceOperationContext context){
		super.init(context);
		((SpliceOperation)source).init(context);
		this.resultDescription = 
				(ResultDescription)activation.getPreparedStatement().getSavedObject(erdNumber);
		numCols = resultDescription.getColumnCount();
		normalizedRow = activation.getExecutionFactory().getValueRow(numCols);
		cachedDestinations = new DataValueDescriptor[numCols];
		startCol = computeStartColumn(forUpdate,resultDescription);
	}

	private int computeStartColumn(boolean forUpdate,
			ResultDescription resultDescription) {
		int count = resultDescription.getColumnCount();
		return forUpdate ? ((count-1)/2)+1 : 1;
	}

	@Override
	public List<NodeType> getNodeTypes() {
		return Collections.singletonList(NodeType.MAP);
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return Collections.singletonList((SpliceOperation)source);
	}

	@Override
	public SpliceOperation getLeftOperation() {
		return (SpliceOperation)source;
	}

	@Override
	public ExecRow getExecRowDefinition() {
		try {
			return normalizeRow(((SpliceOperation)source).getExecRowDefinition());
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
			return null;
		}
	}
	
	@Override
	public ExecRow getNextRowCore() throws StandardException {
		ExecRow sourceRow = null;
		ExecRow result = null;
		
		sourceRow = source.getNextRowCore();
		if(sourceRow!=null){
			result = normalizeRow(sourceRow);
		}
		setCurrentRow(result);
		return result;
	}

	private ExecRow normalizeRow(ExecRow sourceRow) throws StandardException {
		int colCount = resultDescription.getColumnCount();
		for(int i=1;i<=colCount;i++){
			DataValueDescriptor sourceCol = sourceRow.getColumn(i);
			if(sourceCol!=null){
				DataValueDescriptor normalizedCol;
				if(i < startCol){
					normalizedCol = sourceCol;
				}else{
					normalizedCol = normalizeColumn(getDesiredType(i),sourceRow,i,
											getCachedDesgination(i),resultDescription);
				}
				normalizedRow.setColumn(i,normalizedCol);
			}
		}
		return normalizedRow;
	}

	private DataValueDescriptor normalizeColumn(DataTypeDescriptor desiredType,
			ExecRow sourceRow, int sourceColPos, DataValueDescriptor cachedDesgination,
			ResultDescription resultDescription) throws StandardException{
		DataValueDescriptor sourceCol = sourceRow.getColumn(sourceColPos);
		try{
			return desiredType.normalize(sourceCol, cachedDesgination);
		}catch(StandardException se){
			if(se.getMessageId().startsWith(SQLState.LANG_NULL_INTO_NON_NULL)){
				ResultColumnDescriptor colDesc = resultDescription.getColumnDescriptor(sourceColPos);
				throw StandardException.newException(SQLState.LANG_NULL_INTO_NON_NULL,colDesc.getName());
			}
			throw se;
		}
	}

	private DataValueDescriptor getCachedDesgination(int col) throws StandardException {
		int index =col-1;
		if(cachedDestinations[index]==null)
			cachedDestinations[index] = getDesiredType(col).getNull();
		return cachedDestinations[index];
	}

	private DataTypeDescriptor getDesiredType(int i) {
		if(desiredTypes ==null)
			desiredTypes = fetchResultTypes(resultDescription);
		return desiredTypes[i-1];
	}

	private DataTypeDescriptor[] fetchResultTypes(
			ResultDescription resultDescription) {
		int colCount = resultDescription.getColumnCount();
		DataTypeDescriptor[] result =  new DataTypeDescriptor[colCount];
		for(int i=1;i<=colCount;i++){
			ResultColumnDescriptor colDesc = resultDescription.getColumnDescriptor(i);
			DataTypeDescriptor dtd = colDesc.getType();
			
			result[i-1] = dtd;
		}
		return result;
	}

	@Override
	public String toString() {
		return "NormalizeOperation {source="+source+"}";
	}

	@Override
	public void openCore() throws StandardException {
		if(source!=null) source.openCore();
	}

}
