package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.store.access.AutoCastedQualifier;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.execute.SelectConstantAction;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class ScanOperation extends SpliceBaseOperation implements CursorResultSet{
	private static Logger LOG = Logger.getLogger(ScanOperation.class);
	private static long serialVersionUID=6l;

	public int lockMode;
	public int isolationLevel;
	protected ExecRow candidate;
	protected FormatableBitSet accessedCols;
	protected String resultRowAllocatorMethodName;
	protected String scanQualifiersField;
	protected int startSearchOperator;
	protected int stopSearchOperator;
	protected String startKeyGetterMethodName;
	protected String stopKeyGetterMethodName;
	protected boolean sameStartStopPosition;
	protected Qualifier[][] scanQualifiers;
	protected ExecIndexRow stopPosition;
	protected ExecIndexRow startPosition;
	protected SpliceConglomerate conglomerate;
	protected long conglomId;
	protected boolean isKeyed;
	protected GeneratedMethod startKeyGetter;
	protected GeneratedMethod stopKeyGetter;
	protected String tableName;
	protected String indexName;
	public boolean isConstraint;
	public boolean forUpdate;
	
	private int colRefItem;
	protected GeneratedMethod resultRowAllocator;
    protected ExecRow currentTemplate;
    protected FormatableBitSet pkCols;
    

    public ScanOperation () {
		super();
	}

	public ScanOperation (long conglomId, Activation activation, int resultSetNumber,
												GeneratedMethod startKeyGetter, int startSearchOperator,
												GeneratedMethod stopKeyGetter, int stopSearchOperator,
												boolean sameStartStopPosition,
												String scanQualifiersField,
												GeneratedMethod resultRowAllocator,
												int lockMode, boolean tableLocked, int isolationLevel,
												int colRefItem,
												double optimizerEstimatedRowCount,
												double optimizerEstimatedCost) throws StandardException {
		super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.lockMode = lockMode;
		this.isolationLevel = isolationLevel;
		this.resultRowAllocatorMethodName = resultRowAllocator.getMethodName();
		this.colRefItem = colRefItem;
		this.scanQualifiersField = scanQualifiersField;
		this.startKeyGetterMethodName = (startKeyGetter!= null) ? startKeyGetter.getMethodName() : null;
		this.stopKeyGetterMethodName = (stopKeyGetter!= null) ? stopKeyGetter.getMethodName() : null;
		this.startSearchOperator = startSearchOperator;
		this.stopSearchOperator = stopSearchOperator;
		this.sameStartStopPosition = sameStartStopPosition;
		this.startKeyGetterMethodName = (startKeyGetter!= null) ? startKeyGetter.getMethodName() : null;
		this.stopKeyGetterMethodName = (stopKeyGetter!= null) ? stopKeyGetter.getMethodName() : null;
		this.sameStartStopPosition = sameStartStopPosition;
		this.conglomId = conglomId;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//		SpliceLogUtils.trace(LOG, "readExternal");
		super.readExternal(in);
		lockMode = in.readInt();
		isolationLevel = in.readInt();
		resultRowAllocatorMethodName = in.readUTF();
		scanQualifiersField = readNullableString(in);
		startKeyGetterMethodName = readNullableString(in);
		stopKeyGetterMethodName = readNullableString(in);
		stopSearchOperator = in.readInt();
		startSearchOperator = in.readInt();
		sameStartStopPosition = in.readBoolean();
		conglomId = in.readLong();
		colRefItem = in.readInt();
        if(in.readBoolean())
            pkCols = (FormatableBitSet) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
//		SpliceLogUtils.trace(LOG, "writeExternal");
		super.writeExternal(out);
		out.writeInt(lockMode);
		out.writeInt(isolationLevel);
		out.writeUTF(resultRowAllocatorMethodName);
		writeNullableString(scanQualifiersField, out);
		writeNullableString(startKeyGetterMethodName, out);
		writeNullableString(stopKeyGetterMethodName, out);
		out.writeInt(stopSearchOperator);
		out.writeInt(startSearchOperator);
		out.writeBoolean(sameStartStopPosition);
		out.writeLong(conglomId);
		out.writeInt(colRefItem);
        out.writeBoolean(pkCols!=null);
        if(pkCols!=null){
            out.writeObject(pkCols);
        }
	}
	
	@Override
    public void init(SpliceOperationContext context) throws StandardException{
        SpliceLogUtils.trace(LOG, "init called");
        super.init(context);
        GenericStorablePreparedStatement statement = context.getPreparedStatement();
        this.accessedCols = colRefItem != -1 ? (FormatableBitSet)(statement.getSavedObject(colRefItem)) : null;
        SpliceLogUtils.trace(LOG,"<%d> colRefItem=%d,accessedCols=%s",conglomId,colRefItem,accessedCols);
        try {
            resultRowAllocator = statement.getActivationClass()
                    .getMethod(resultRowAllocatorMethodName);
            this.conglomerate = (SpliceConglomerate)((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomId);
            
            this.isKeyed = conglomerate.getTypeFormatId() == IndexConglomerate.FORMAT_NUMBER;
            if (startKeyGetterMethodName != null) {
                startKeyGetter = statement.getActivationClass().getMethod(startKeyGetterMethodName);
            }
            if (stopKeyGetterMethodName != null) {
                stopKeyGetter = statement.getActivationClass().getMethod(stopKeyGetterMethodName);
            }
            candidate = (ExecRow) resultRowAllocator.invoke(activation);
            currentRow = getCompactRow(context.getLanguageConnectionContext(), candidate,
                    accessedCols, isKeyed);
            currentTemplate = currentRow.getClone();

            if(activation.getConstantAction() instanceof SelectConstantAction){
                SelectConstantAction action = (SelectConstantAction) activation.getConstantAction();
                int[] pks = action.getKeyColumns();
                pkCols = new FormatableBitSet(pks.length);
                for(int pk:pks){
                    pkCols.grow(pk+1);
                    pkCols.set(pk-1);
                }
            }
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!", e);
        }
    }

    @Override
    public NoPutResultSet executeScan() throws StandardException {
        SpliceLogUtils.trace(LOG, "executeScan");
        return new SpliceNoPutResultSet(activation,this, getMapRowProvider(this,getExecRowDefinition()));
    }
	@Override
	public SpliceOperation getLeftOperation() {
//		SpliceLogUtils.trace(LOG, "getLeftOperation");
		return null;
	}
	@Override
	public RowLocation getRowLocation() throws StandardException {
		SpliceLogUtils.trace(LOG, "getRowLocation %s",currentRowLocation);
		return currentRowLocation;
	}

	@Override
	public ExecRow getCurrentRow() throws StandardException {
		SpliceLogUtils.trace(LOG, "getCurrentRow %s",currentRow);
		return currentRow;
	}
	
	protected void initIsolationLevel() {
		SpliceLogUtils.trace(LOG, "initIsolationLevel");
	}

	protected Scan buildScan() {
		try{
            populateStartAndStopPositions();
            populateQualifiers();
            return getScan();
        } catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
		return null;
	}

    private boolean isFloatType(DataValueDescriptor dvd){
        if(dvd==null) return false;
        int typeId = dvd.getTypeFormatId();

        return typeId == StoredFormatIds.SQL_DOUBLE_ID
                || typeId == StoredFormatIds.SQL_DECIMAL_ID
                || typeId == StoredFormatIds.SQL_REAL_ID;
    }

    private boolean isIntegerType(DataValueDescriptor dvd){
        if(dvd==null) return false;
        int typeId = dvd.getTypeFormatId();

        return typeId == StoredFormatIds.SQL_INTEGER_ID
                || typeId == StoredFormatIds.SQL_LONGINT_ID
                || typeId == StoredFormatIds.SQL_TINYINT_ID
                || typeId == StoredFormatIds.SQL_SMALLINT_ID;
    }

    private boolean isCastingNeeded(Qualifier[][] scanQualifiers, ExecRow candidateRow) throws StandardException{

        boolean isNeeded = false;

        if(scanQualifiers != null){

            DataValueDescriptor dvds[] = candidateRow.getRowArray();

            for(int i=0; i < scanQualifiers.length && !isNeeded; i++ ){
                for(int j=0; j< scanQualifiers[i].length && !isNeeded; j++){
                    int column = scanQualifiers[i][j].getColumnId();
                    DataValueDescriptor columnDvd = dvds[column];
                    DataValueDescriptor filterDvd = scanQualifiers[i][j].getOrderable();
                    if((isIntegerType(columnDvd) && isFloatType(filterDvd))
                            || (isIntegerType(filterDvd) && isFloatType(columnDvd))){
                        isNeeded = true;
                    }
                }

            }
        }

        return isNeeded;
    }

    private Qualifier[][] createAutoCastedQualifiers(Qualifier[][] scanQualifiers, ExecRow candidateRow) throws StandardException{
        DataValueDescriptor[] dvds = candidateRow.getRowArray();

        Qualifier[][] castedQualifiers = new Qualifier[scanQualifiers.length][];

        for(int i = 0; i < scanQualifiers.length; i++){

            castedQualifiers[i] = new Qualifier[scanQualifiers[i].length];

            for(int j = 0; j < scanQualifiers[i].length; j++){
                int column = scanQualifiers[i][j].getColumnId();
                DataValueDescriptor newDvd = dvds[column].cloneValue(false);
                DataValueDescriptor filterDvd = scanQualifiers[i][j].getOrderable();

                if(isIntegerType(newDvd) && isFloatType(filterDvd)){
                    newDvd.setValue(filterDvd.getInt());
                    castedQualifiers[i][j] = new AutoCastedQualifier(scanQualifiers[i][j],newDvd);
                }else if(isIntegerType(filterDvd) && isFloatType(newDvd)){
                    newDvd.setValue(filterDvd.getDouble());
                    castedQualifiers[i][j] = new AutoCastedQualifier(scanQualifiers[i][j],newDvd);
                }else{
                    castedQualifiers[i][j] = scanQualifiers[i][j];
                }
            }
        }

        return castedQualifiers;
    }

    protected Scan getScan() throws IOException {

        Qualifier[][] autoCastedQuals = null;

        try{
            if(isCastingNeeded(scanQualifiers, candidate)){
                autoCastedQuals = createAutoCastedQualifiers(scanQualifiers, candidate);
            }else{
                autoCastedQuals = scanQualifiers;
            }
        }catch(StandardException e){
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        }

        return Scans.setupScan(startPosition == null ? null : startPosition.getRowArray(), startSearchOperator,
                stopPosition == null ? null : stopPosition.getRowArray(), stopSearchOperator,
                autoCastedQuals, conglomerate.getAscDescInfo(), pkCols,accessedCols,
                getTransactionID());
    }

    protected void populateQualifiers()  {
        if (scanQualifiersField != null){
            try {
                scanQualifiers = (Qualifier[][]) activation.getClass().getField(scanQualifiersField).get(activation);
            } catch (IllegalAccessException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            } catch (NoSuchFieldException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            }
        }
    }

    protected void populateStartAndStopPositions() throws StandardException {
        if(startKeyGetter!=null){
            startPosition = (ExecIndexRow)startKeyGetter.invoke(activation);
            if(sameStartStopPosition){
                /*
                 * if the stop position is the same as the start position, we are
                 * right at the position where we should return values, and so we need to make sure that
                 * we only return values which match an equals filter. Otherwise, we'll need
                 * to scan between the start and stop keys and pull back the values which are greater than
                 * or equals to the start (e.g. leave startSearchOperator alone).
                 */
                stopPosition = startPosition;
                startSearchOperator= ScanController.NA; //ensure that we put in an EQUALS filter
            }
        }
        if(stopKeyGetter!=null){
            stopPosition = (ExecIndexRow)stopKeyGetter.invoke(activation);
        }
    }

    @Override
	public int[] getRootAccessedCols(long tableNumber) {
        return baseColumnMap;
	}

    @Override
    public boolean isReferencingTable(long tableNumber){
        return tableName.equals(String.valueOf(tableNumber));
    }
    
    public FormatableBitSet getAccessedCols() {
    	return this.accessedCols;
    }
    
    public Qualifier[][] getScanQualifiers() {
    	return this.scanQualifiers;
    }
    
    public String getTableName(){
    	return this.tableName;
    }
    
    public String getIndexName() {
    	return this.indexName;
    }
    
    @Override
    public void close() throws StandardException {
        SpliceLogUtils.trace(LOG, "closing");
        super.close();
    }
    
    public String printStartPosition()
   	{
   		return printPosition(startSearchOperator, startKeyGetter, startPosition);
   	}

   	public String printStopPosition()
   	{
   		if (sameStartStopPosition)
   			return printPosition(stopSearchOperator, startKeyGetter, startPosition);
   		else
   			return printPosition(stopSearchOperator, stopKeyGetter, stopPosition);
   	}

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);
        return new StringBuilder("Scan:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("accessedCols:").append(accessedCols)
                .append(indent).append("resultRowAllocatorMethodName:").append(resultRowAllocatorMethodName)
                .append(indent).append("startSearchOperator:").append(startSearchOperator)
                .append(indent).append("stopSearchOperator:").append(stopSearchOperator)
                .append(indent).append("startKeyGetterMethodName:").append(startKeyGetterMethodName)
                .append(indent).append("stopKeyGetterMethodName:").append(stopKeyGetterMethodName)
                .append(indent).append("sameStartStopPosition:").append(sameStartStopPosition)
                .append(indent).append("conglomId:").append(conglomId)
                .append(indent).append("isKeyed:").append(isKeyed)
                .append(indent).append("tableName:").append(tableName)
                .toString();
    }

    /**
   	 * Return a start or stop positioner as a String.
   	 *
   	 * If we already generated the information, then use
   	 * that.  Otherwise, invoke the activation to get it.
   	 */
   	private String printPosition(int searchOperator,
   								 GeneratedMethod positionGetter,
   								 ExecIndexRow positioner)
   	{
   		String output = "";
   		if (positionGetter == null)
   			return "\t" + MessageService.getTextMessage(SQLState.LANG_NONE) + "\n";
   		
   		if (positioner == null)
   		{
   			if (numOpens == 0)
   				return "\t" + MessageService.getTextMessage(
   					SQLState.LANG_POSITION_NOT_AVAIL) +
                                       "\n";
   			try {
   				positioner = (ExecIndexRow)positionGetter.invoke(activation);
   			} catch (StandardException e) {
   				return "\t" + MessageService.getTextMessage(
   						SQLState.LANG_UNEXPECTED_EXC_GETTING_POSITIONER,
   						e.toString());
   			}
   		}
   		if (positioner == null)
   			return "\t" + MessageService.getTextMessage(SQLState.LANG_NONE) + "\n";
   		String searchOp = null;

   		switch (searchOperator)
   		{
   			case ScanController.GE:
   				searchOp = ">=";
   				break;

   			case ScanController.GT:
   				searchOp = ">";
   				break;

   			default:

   				// NOTE: This does not have to be internationalized because
   				// this code should never be reached.
   				searchOp = "unknown value (" + searchOperator + ")";
   				break;
   		}

   		output = output + "\t" +
   						MessageService.getTextMessage(
   							SQLState.LANG_POSITIONER,
   							searchOp,
   							String.valueOf(positioner.nColumns())) +
   						"\n";

   		output = output + "\t" +
   					MessageService.getTextMessage(
   						SQLState.LANG_ORDERED_NULL_SEMANTICS) +
   					"\n";
   		boolean colSeen = false;
   		for (int position = 0; position < positioner.nColumns(); position++)
   		{
   			if (positioner.areNullsOrdered(position))
   			{
   				output = output + position + " ";
   				colSeen = true;
   			}

   			if (colSeen && position == positioner.nColumns() - 1)
   				output = output +  "\n";
   		}

   		return output;
   	}
}
