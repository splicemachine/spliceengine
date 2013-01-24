package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class ScanOperation extends SpliceBaseOperation implements CursorResultSet{
	private static Logger LOG = Logger.getLogger(ScanOperation.class);
    protected int lockMode;
    protected int isolationLevel;
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
    private GeneratedMethod startKeyGetter;
    private GeneratedMethod stopKeyGetter;


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
        this.candidate = (ExecRow) resultRowAllocator.invoke(activation);
        this.accessedCols = colRefItem != -1 ? (FormatableBitSet)(activation.getPreparedStatement().getSavedObject(colRefItem)) : null; 
		this.scanQualifiersField = scanQualifiersField;
		this.startKeyGetterMethodName = (startKeyGetter!= null) ? startKeyGetter.getMethodName() : null;
		this.stopKeyGetterMethodName = (stopKeyGetter!= null) ? stopKeyGetter.getMethodName() : null;
		this.startSearchOperator = startSearchOperator;
		this.stopSearchOperator = stopSearchOperator;
		this.sameStartStopPosition = sameStartStopPosition;
		this.startKeyGetterMethodName = (startKeyGetter!= null) ? startKeyGetter.getMethodName() : null;
		this.stopKeyGetterMethodName = (stopKeyGetter!= null) ? stopKeyGetter.getMethodName() : null;
		this.startSearchOperator = startSearchOperator;
		this.stopSearchOperator = stopSearchOperator;
		this.sameStartStopPosition = sameStartStopPosition;
		this.conglomId = conglomId;
        this.conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomId);
        this.isKeyed = conglomerate.getTypeFormatId() == IndexConglomerate.FORMAT_NUMBER;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		super.readExternal(in);
		accessedCols = (FormatableBitSet) in.readObject();
		lockMode = in.readInt();
		isolationLevel = in.readInt();
		resultRowAllocatorMethodName = in.readUTF();
		scanQualifiersField = readNullableString(in);
		startKeyGetterMethodName = readNullableString(in);
		stopKeyGetterMethodName = readNullableString(in);
		stopSearchOperator = in.readInt();
		startSearchOperator = in.readInt();
		sameStartStopPosition = in.readBoolean();
		conglomerate = (SpliceConglomerate) in.readObject();
		isKeyed = in.readBoolean();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		super.writeExternal(out);
		out.writeObject(accessedCols);
		out.writeInt(lockMode);
		out.writeInt(isolationLevel);
		out.writeUTF(resultRowAllocatorMethodName);
		writeNullableString(scanQualifiersField, out);
		writeNullableString(startKeyGetterMethodName, out);
		writeNullableString(stopKeyGetterMethodName, out);
		out.writeInt(stopSearchOperator);
		out.writeInt(startSearchOperator);
		out.writeBoolean(sameStartStopPosition);
		out.writeObject(conglomerate);
		out.writeBoolean(isKeyed);
	}
	
	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG, "init called");
		super.init(context);
		try {
            GenericStorablePreparedStatement statement = context.getPreparedStatement();
		    if (startKeyGetterMethodName != null) {
		    	startKeyGetter = statement.getActivationClass().getMethod(startKeyGetterMethodName);
//				startPosition = (ExecIndexRow) startKeyGetter.invoke(activation);
//				if (sameStartStopPosition)
//					stopPosition = startPosition;
			}
			if (stopKeyGetterMethodName != null) {
				stopKeyGetter = statement.getActivationClass().getMethod(stopKeyGetterMethodName);
//				stopPosition = (ExecIndexRow) stopKeyGetter.invoke(activation);
			}
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!", e);
		} 
	}
	
	@Override
	public NoPutResultSet executeScan() {
		SpliceLogUtils.trace(LOG, "executeScan");
		return new SpliceNoPutResultSet(activation,this, getMapRowProvider(this,getExecRowDefinition()));
	}
	@Override
	public SpliceOperation getLeftOperation() {
		SpliceLogUtils.trace(LOG, "getLeftOperation");
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
            if(startKeyGetter!=null){
                startPosition = (ExecIndexRow)startKeyGetter.invoke(activation);
                if(sameStartStopPosition)
                    stopPosition = startPosition;
            }
            if(stopKeyGetter!=null){
                stopPosition = (ExecIndexRow)stopKeyGetter.invoke(activation);
            }
	        if (scanQualifiersField != null)
	            scanQualifiers = (Qualifier[][]) activation.getClass().getField(scanQualifiersField).get(activation);
	        return SpliceUtils.setupScan(Bytes.toBytes(transactionID),accessedCols,scanQualifiers,
	                startPosition==null? null: startPosition.getRowArray(), startSearchOperator,
	                stopPosition==null? null: stopPosition.getRowArray(),stopSearchOperator,
	                conglomerate.getAscDescInfo());
        } catch (NoSuchFieldException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        } catch (IllegalAccessException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
        return null;
    }
}
