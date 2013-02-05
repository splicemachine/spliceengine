package com.splicemachine.derby.hbase;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.utils.SerializingExecRow;
import com.splicemachine.derby.utils.SerializingIndexRow;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.GenericActivationHolder;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.*;
/**
 * 
 * Class utilized to serialize the Splice Operation onto the scan for hbase.  It attaches the 
 * GenericStorablePreparedStatement and the Top Operation.
 * 
 * @author johnleach
 *
 */
public class SpliceObserverInstructions implements Externalizable {
	private static final long serialVersionUID = 3l;
	private static Logger LOG = Logger.getLogger(SpliceObserverInstructions.class);
	protected GenericStorablePreparedStatement statement;
	protected SpliceOperation topOperation;
	protected ExecRow[] currentRows;
	private Map<String,Integer> setOps;

	public SpliceObserverInstructions() {
		super();
		SpliceLogUtils.trace(LOG, "instantiated");
	}

	public SpliceObserverInstructions(GenericStorablePreparedStatement statement, ExecRow[] currentRows,
																		SpliceOperation topOperation, Map<String,Integer> setOps) {
		SpliceLogUtils.trace(LOG, "instantiated with statement " + statement);
		this.statement = statement;
		this.topOperation = topOperation;
		this.currentRows= currentRows;
		this.setOps = setOps;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		this.statement = (GenericStorablePreparedStatement) in.readObject();
		this.topOperation = (SpliceOperation) in.readObject();
		if(in.readBoolean()){
			this.currentRows = new ExecRow[in.readInt()];
			ArrayUtil.readArrayItems(in,currentRows);
		}
		this.setOps = (Map<String,Integer>)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		out.writeObject(statement);
		out.writeObject(topOperation);
		if(currentRows!=null){
			out.writeBoolean(true);
			ArrayUtil.writeArray(out,currentRows);
		}
		out.writeObject(setOps);
	}
	/**
	 * Retrieve the GenericStorablePreparedStatement: This contains the byte code for the activation.
	 * 
	 * @return
	 */
	public GenericStorablePreparedStatement getStatement() {
		SpliceLogUtils.trace(LOG, "getStatement " + statement);
		return statement;
	}

	public Activation getActivation(LanguageConnectionContext lcc){
		try {
			Activation activation = ((GenericActivationHolder)statement.getActivation(lcc,false)).ac;
		/*
		 * Set any currently populated rows back on to the activation
		 */
			if(currentRows!=null){
				for(int i=0;i<currentRows.length;i++){
					ExecRow row = currentRows[i];
					if(row!=null){
						activation.setCurrentRow(row,i);
					}
				}
			}

			/*
			 * Set the populated operations with their comparable operation
			 */
			List<SpliceOperation> ops = new ArrayList<SpliceOperation>();
			topOperation.generateLeftOperationStack(ops);
			for(String setField:setOps.keySet()){
				SpliceOperation op = ops.get(setOps.get(setField));
				Field fieldToSet = activation.getClass().getDeclaredField(setField);
				if(!fieldToSet.isAccessible())fieldToSet.setAccessible(true);
				fieldToSet.set(activation, op);
			}
			return activation;
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		} catch (NoSuchFieldException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		} catch (IllegalAccessException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}
		return null;
	}

	public ExecRow[] getCurrentRows(){
		return currentRows;
	}

	/**
	 * Retrieve the Top Operation: This can recurse to creat the operational stack for hbase for the scan.
	 * 
	 * @return
	 */
	public SpliceOperation getTopOperation() {
		SpliceLogUtils.trace(LOG, "getTopOperation " + topOperation);
		return topOperation;
	}

	public static SpliceObserverInstructions create(Activation activation,
																									SpliceOperation topOperation) {
		List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
		Map<String,Integer> setOps = new HashMap<String,Integer>(operations.size());
		topOperation.generateLeftOperationStack(operations);
		for(Field field:activation.getClass().getDeclaredFields()){
			if(ResultSet.class.isAssignableFrom(field.getType())){
				try {
					if(!field.isAccessible())field.setAccessible(true); //make it accessible to me
					SpliceOperation op = (SpliceOperation)field.get(activation);
					for(int i=0;i<operations.size();i++){
						if(op == operations.get(i)){
							setOps.put(field.getName(),i);
							break;
						}
					}
				} catch (IllegalAccessException e) {
					SpliceLogUtils.logAndThrowRuntime(LOG,e);
				}
			}
		}

		/*
		 * Serialize out any non-null result rows that are currently stored in the activation.
		 *
		 * This is necessary if you are pushing out a set of Operation to a Table inside of a Sink.
		 */
		int rowPos=1;
		Map<Integer,ExecRow> rowMap = new HashMap<Integer,ExecRow>();
		boolean shouldContinue=true;
		while(shouldContinue){
			try{
				ExecRow row = (ExecRow)activation.getCurrentRow(rowPos);
				if(row!=null){
					rowMap.put(rowPos,row);
				}
				rowPos++;
			}catch(IndexOutOfBoundsException ie){
				//we've reached the end of the row group in activation, so stop
				shouldContinue=false;
			}
		}
		ExecRow[] currentRows = new ExecRow[rowPos];
		for(Integer rowPosition:rowMap.keySet()){
			ExecRow row = new SerializingExecRow(rowMap.get(rowPosition));
			if(row instanceof ExecIndexRow)
				currentRows[rowPosition] = new SerializingIndexRow(row);
			else
				currentRows[rowPosition] =  row;
		}
		SpliceLogUtils.trace(LOG,"serializing current rows: %s", Arrays.toString(currentRows));

		return new SpliceObserverInstructions(
				(GenericStorablePreparedStatement) activation.getPreparedStatement(),
				currentRows,
				topOperation,setOps );
	}
}