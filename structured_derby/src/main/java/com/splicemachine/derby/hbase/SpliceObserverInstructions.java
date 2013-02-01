package com.splicemachine.derby.hbase;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;
/**
 * 
 * Class utilized to serialize the Splice Operation onto the scan for hbase.  It attaches the 
 * GenericStorablePreparedStatement and the Top Operation.
 * 
 * @author johnleach
 *
 */
public class SpliceObserverInstructions implements Externalizable {
	private static Logger LOG = Logger.getLogger(SpliceObserverInstructions.class);
	protected GenericStorablePreparedStatement statement;
	protected SpliceOperation topOperation;
	protected ExecRow[] currentRows;

	public SpliceObserverInstructions() {
		super();
		SpliceLogUtils.trace(LOG, "instantiated");
	}

	public SpliceObserverInstructions(GenericStorablePreparedStatement statement,ExecRow[] currentRows,SpliceOperation topOperation) {
		SpliceLogUtils.trace(LOG, "instantiated with statement " + statement);
		this.statement = statement;
		this.topOperation = topOperation;
		this.currentRows= currentRows;
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
}