package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;

public class NormalizeOperation extends SpliceBaseOperation {
	private static final long serialVersionUID = 2l;
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
        this.erdNumber = erdNumber;
		init(SpliceOperationContext.newContext(activation));
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		forUpdate = in.readBoolean();
		erdNumber = in.readInt();
		source = (SpliceOperation)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeBoolean(forUpdate);
		out.writeInt(erdNumber);
		out.writeObject(source);
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
            return getFromResultDescription(resultDescription);
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
			result = normalizeRow(sourceRow,true);
		}
		setCurrentRow(result);
		return result;
	}

	private ExecRow normalizeRow(ExecRow sourceRow,boolean requireNotNull) throws StandardException {
		int colCount = resultDescription.getColumnCount();
		for(int i=1;i<=colCount;i++){
			DataValueDescriptor sourceCol = sourceRow.getColumn(i);
			if(sourceCol!=null){
				DataValueDescriptor normalizedCol;
				if(i < startCol){
					normalizedCol = sourceCol;
				}else{
					normalizedCol = normalizeColumn(getDesiredType(i),sourceRow,i,
											getCachedDesgination(i),resultDescription,requireNotNull);
				}
				normalizedRow.setColumn(i,normalizedCol);
			}
		}
		return normalizedRow;
	}

	private static DataValueDescriptor normalize(DataValueDescriptor source,
																			 DataTypeDescriptor destType,DataValueDescriptor cachedDest, boolean requireNonNull)
			throws StandardException
	{
		if (SanityManager.DEBUG) {
			if (cachedDest != null) {
				if (!destType.getTypeId().isUserDefinedTypeId()) {
					String t1 = destType.getTypeName();
					String t2 = cachedDest.getTypeName();
					if (!t1.equals(t2)) {

						if (!(((t1.equals("DECIMAL") || t1.equals("NUMERIC"))
								&& (t2.equals("DECIMAL") || t2.equals("NUMERIC"))) ||
								(t1.startsWith("INT") && t2.startsWith("INT"))))  //INT/INTEGER

							SanityManager.THROWASSERT(
									"Normalization of " + t2 + " being asked to convert to " + t1);
					}
				}
			} else {
				SanityManager.THROWASSERT("cachedDest is null");
			}
		}

		if (source.isNull())
		{
			if (requireNonNull && !destType.isNullable())
				throw StandardException.newException(org.apache.derby.iapi.reference.SQLState.LANG_NULL_INTO_NON_NULL,"");

			cachedDest.setToNull();
		} else {

			int jdbcId = destType.getJDBCTypeId();

			cachedDest.normalize(destType, source);
			//doing the following check after normalize so that normalize method would get called on long varchs and long varbinary
			//Need normalize to be called on long varchar for bug 5592 where we need to enforce a lenght limit in db2 mode
			if ((jdbcId == Types.LONGVARCHAR) || (jdbcId == Types.LONGVARBINARY)) {
				// special case for possible streams
				if (source.getClass() == cachedDest.getClass())
					return source;
			}
		}
		return cachedDest;
	}

	private DataValueDescriptor normalizeColumn(DataTypeDescriptor desiredType,
			ExecRow sourceRow, int sourceColPos, DataValueDescriptor cachedDesgination,
			ResultDescription resultDescription,boolean requireNotNull) throws StandardException{
		DataValueDescriptor sourceCol = sourceRow.getColumn(sourceColPos);
		try{
			return normalize(sourceCol,desiredType, cachedDesgination,requireNotNull);
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
