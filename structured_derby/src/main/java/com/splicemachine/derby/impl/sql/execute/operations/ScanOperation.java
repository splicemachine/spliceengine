package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.store.access.AutoCastedQualifier;
import com.splicemachine.derby.utils.ByteDataOutput;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.*;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataType;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.execute.GenericScanQualifier;
import org.apache.derby.impl.sql.execute.SelectConstantAction;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
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
            if (currentRowLocation == null)
            	currentRowLocation = new HBaseRowLocation();
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!", e);
        }
    }

    @Override
    public NoPutResultSet executeScan() throws StandardException {
        SpliceLogUtils.trace(LOG, "executeScan");
        return new SpliceNoPutResultSet(activation,this, getMapRowProvider(this,getRowEncoder().getDual(getExecRowDefinition())));
    }
	@Override
	public SpliceOperation getLeftOperation() {
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

        //TODO -sf- push in the column information so that we can do scan casting for correctness
        return Scans.setupScan(startPosition == null ? null : startPosition.getRowArray(), startSearchOperator,
                stopPosition == null ? null : stopPosition.getRowArray(), stopSearchOperator,
                autoCastedQuals, conglomerate.getAscDescInfo(), pkCols,accessedCols,
                getTransactionID());
    }

    protected void populateQualifiers() throws StandardException {
        if (scanQualifiersField != null){
            try {
                scanQualifiers = (Qualifier[][]) activation.getClass().getField(scanQualifiersField).get(activation);
            } catch (IllegalAccessException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            } catch (NoSuchFieldException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            }
            //convert types of filters against column type
            if(scanQualifiers!=null){
                int[] format_ids = conglomerate.getFormat_ids();
                for(Qualifier[] qualifiers:scanQualifiers){
                    for(int qualPos=0;qualPos<qualifiers.length;qualPos++){
                        Qualifier qualifier = qualifiers[qualPos];
                        int columnFormat = format_ids[qualifier.getColumnId()];
                        DataValueDescriptor dvd = qualifier.getOrderable();
                        if(dvd.getTypeFormatId()!=columnFormat){
                            //we need to convert the types to match
                            qualifier = adjustQualifier(qualifier, columnFormat);
                            qualifiers[qualPos] = qualifier;
                        }
                    }
                }
            }
        }
    }

    private Qualifier adjustQualifier(Qualifier qualifier, int columnFormat) throws StandardException {
        if(isFloatType(columnFormat)){
            return convertFloatingPoint(qualifier,columnFormat);
        }else if(isScalarType(columnFormat)){
            return convertScalar(qualifier,columnFormat);
        }else return qualifier; //nothing to do
    }

    private Qualifier convertScalar(Qualifier qualifier, int columnFormat) throws StandardException {
        DataValueDescriptor dvd = qualifier.getOrderable();
        /*
         * Technically, all Scalar types encode the same way. However, that's an implementation
         * detail which may change in the future (particularly with regards to small data types,
         * like TINYINT which can serialize more compactly while retaining order characteristics).
         * Thus, this method does two things:
         *
         * 1. Convert decimal types into the correct Scalar type (truncation)
         * 2. Convert into correct Scalar types to adjust for potential overflow issues (ints bigger
         * than Short.MAX_VALUE, etc).
         */
        DataValueDescriptor correctType = activation.getDataValueFactory().getNull(columnFormat, -1);
        double value;
        int currentTypeFormatId = dvd.getTypeFormatId();
        switch(currentTypeFormatId){
            case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                value = dvd.getDouble();
                break;
            case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
                value = dvd.getByte();
                break;
            case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
                value = dvd.getShort();
                break;
            case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
                value = dvd.getInt();
                break;
            case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
                value = dvd.getFloat();
                break;
            case StoredFormatIds.SQL_DECIMAL_ID:
                BigDecimal val = (BigDecimal)dvd.getObject();
                value = val.doubleValue();
                break;
            default:
                value = dvd.getLong();
        }
        double maxValue = Long.MAX_VALUE;
        double minValue = Long.MIN_VALUE;
        if(columnFormat==StoredFormatIds.SQL_INTEGER_ID){
            maxValue = Integer.MAX_VALUE;
            minValue = Integer.MIN_VALUE;
        }else if(columnFormat==StoredFormatIds.SQL_SMALLINT_ID){
            maxValue = Short.MAX_VALUE;
            minValue = Short.MIN_VALUE;
        }else if(columnFormat==StoredFormatIds.SQL_TINYINT_ID){
            maxValue = Byte.MAX_VALUE;
            minValue = Byte.MIN_VALUE;
        }

        if(value > maxValue)
            value = maxValue;
        else if(value < minValue)
            value = minValue;
        correctType.setValue(value);

        if(qualifier instanceof ScanQualifier){
            ((ScanQualifier)qualifier).setQualifier(qualifier.getColumnId(),
                correctType,
                qualifier.getOperator(),
                qualifier.negateCompareResult(),
                qualifier.getOrderedNulls(),
                qualifier.getUnknownRV());
        }else{
            //make it an instanceof ScanQualifier
            ScanQualifier qual = new GenericScanQualifier();
            qual.setQualifier(qualifier.getColumnId(),
                    correctType,
                    qualifier.getOperator(),
                    qualifier.negateCompareResult(),
                    qualifier.getOrderedNulls(),
                    qualifier.getUnknownRV());
            qualifier = qual;
        }
        return qualifier;
    }


    private boolean isFloatType(int columnFormat){
        return (columnFormat==StoredFormatIds.SQL_REAL_ID
                ||columnFormat==StoredFormatIds.SQL_DECIMAL_ID
                || columnFormat==StoredFormatIds.SQL_DOUBLE_ID);
    }

    private boolean isScalarType(int columnFormat){
        return (columnFormat==StoredFormatIds.SQL_TINYINT_ID
        || columnFormat==StoredFormatIds.SQL_SMALLINT_ID
        || columnFormat==StoredFormatIds.SQL_INTEGER_ID
        || columnFormat==StoredFormatIds.SQL_LONGINT_ID);
    }

    private Qualifier convertFloatingPoint(Qualifier qualifier, int columnFormat) throws StandardException {
        DataValueDescriptor dvd = qualifier.getOrderable();
        /*
         * We must check for overflow amongst decimal types, but we don't need to worry about overflowing
         * from scalar types.
         */
        DataValueDescriptor correctType = activation.getDataValueFactory().getNull(columnFormat,-1);

        int currentTypeFormatId= dvd.getTypeFormatId();
        if(isScalarType(currentTypeFormatId)){
            //getLong() runs no risk of overflow from a scalar type, so we can set it and be done
            correctType.setValue(dvd.getLong());
        }else{
            /*
             * Since floats, doubles, and BigDecimals all serialize differently, we have to be
             * concerned about upcasting as well as overflow. That is, if we are scanning a double
             * column, but we have a float scan qualifier, we have to upcast that float into a double.
             */
            if(currentTypeFormatId==StoredFormatIds.SQL_REAL_ID){
                correctType.setValue(dvd.getFloat());
            }else if(currentTypeFormatId==StoredFormatIds.SQL_DOUBLE_ID){
                if(columnFormat==StoredFormatIds.SQL_REAL_ID){
                    //check for float overflow
                    double value = dvd.getDouble();
                    if(value > Float.MAX_VALUE){
                        value = Float.MAX_VALUE;
                    }else if(value < Float.MIN_VALUE){
                        value = Float.MIN_VALUE;
                    }
                    correctType.setValue(value);
                }else if(columnFormat==StoredFormatIds.SQL_DECIMAL_ID){
                    correctType.setValue(dvd.getDouble());
                }
            }else if(currentTypeFormatId==StoredFormatIds.SQL_DECIMAL_ID){
                BigDecimal val = (BigDecimal)dvd.getObject();
                double value = 0;
                if(columnFormat==StoredFormatIds.SQL_REAL_ID){
                    if(val.compareTo(BigDecimal.valueOf(Float.MAX_VALUE))>0)
                        value = Float.MAX_VALUE;
                    else if(val.compareTo(BigDecimal.valueOf(Float.MIN_VALUE))<0)
                        value = Float.MIN_VALUE;
                    else
                        value = val.floatValue();
                }else if(columnFormat==StoredFormatIds.SQL_DOUBLE_ID){
                    if(val.compareTo(BigDecimal.valueOf(Double.MAX_VALUE))>0)
                        value = Double.MAX_VALUE;
                    else if(val.compareTo(BigDecimal.valueOf(Double.MIN_VALUE))<0)
                        value = Double.MIN_VALUE;
                    else
                        value = val.doubleValue();
                }else{
                    return qualifier;
                }
                correctType.setValue(value);
            }
        }
        if(qualifier instanceof ScanQualifier){
            ((ScanQualifier)qualifier).setQualifier(qualifier.getColumnId(),
                    correctType,
                    qualifier.getOperator(),
                    qualifier.negateCompareResult(),
                    qualifier.getOrderedNulls(),
                    qualifier.getUnknownRV());
        }else{
            //make it an instanceof ScanQualifier
            ScanQualifier qual = new GenericScanQualifier();
            qual.setQualifier(qualifier.getColumnId(),
                    correctType,
                    qualifier.getOperator(),
                    qualifier.negateCompareResult(),
                    qualifier.getOrderedNulls(),
                    qualifier.getUnknownRV());
            qualifier = qual;
        }
        return qualifier;
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
