package com.splicemachine.derby.hbase;

import com.splicemachine.derby.iapi.sql.execute.OperationResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManagerContext;
import com.splicemachine.derby.utils.SerializingExecRow;
import com.splicemachine.derby.utils.SerializingIndexRow;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
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
	private static final long serialVersionUID = 4l;
	private static Logger LOG = Logger.getLogger(SpliceObserverInstructions.class);
	protected GenericStorablePreparedStatement statement;
	protected SpliceOperation topOperation;
    private ActivationContext activationContext;

    // propagate transactionId to all co-processors running operations for this SQL statement
    private String transactionId;

	public SpliceObserverInstructions() {
		super();
		SpliceLogUtils.trace(LOG, "instantiated");
	}

	public SpliceObserverInstructions(GenericStorablePreparedStatement statement,  SpliceOperation topOperation,
                                      ActivationContext activationContext, String transactionId ) {
		SpliceLogUtils.trace(LOG, "instantiated with statement " + statement);
		this.statement = statement;
		this.topOperation = topOperation;
        this.activationContext = activationContext;
        this.transactionId = transactionId;
	}

    public String getTransactionId() {
        return transactionId;
    }

    @Override
	public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		this.statement = (GenericStorablePreparedStatement) in.readObject();
		this.topOperation = (SpliceOperation) in.readObject();
        this.activationContext = (ActivationContext)in.readObject();
        this.transactionId = (String) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		out.writeObject(statement);
		out.writeObject(topOperation);
        out.writeObject(activationContext);
        out.writeObject(transactionId);
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
        try{
            Activation activation = ((GenericActivationHolder)statement.getActivation(lcc,false)).ac;
            return activationContext.populateActivation(activation,statement,topOperation);
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
            return null; //never happen
        }
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
        ActivationContext activationContext = ActivationContext.create(activation,topOperation);
        final String transactionID = getTransactionId(activation);

        return new SpliceObserverInstructions(
				(GenericStorablePreparedStatement) activation.getPreparedStatement(),
				topOperation,activationContext, transactionID);
	}

    private static String getTransactionId(Activation activation) {
        final LanguageConnectionContext languageConnectionContext = activation.getLanguageConnectionContext();
        return getTransactionId(languageConnectionContext);
    }

    public static String getTransactionId(LanguageConnectionContext languageConnectionContext) {
        final ContextManager contextManager = languageConnectionContext.getContextManager();
        SpliceTransactionManagerContext stmc = (SpliceTransactionManagerContext) contextManager.getContext(AccessFactoryGlobals.RAMXACT_CONTEXT_ID);
        final SpliceTransactionManager transactionManager = stmc.getTransactionManager();
        final SpliceTransaction transaction = (SpliceTransaction) transactionManager.getRawStoreXact();
        return transaction.getTransactionId().getTransactionIdString();
    }

    private static class ActivationContext implements Externalizable{
        private static final long serialVersionUID = 1l;
        private ExecRow[] currentRows;
        private Map<String,Integer> setOps;
        private boolean statementAtomic;
        private boolean statementReadOnly;
        private String stmtText;
        private boolean stmtRollBackParentContext;
        private long stmtTimeout;
        private ParameterValueSet pvs;


        /**
         * Used only for serialization. DO NOT USE
         */
        @Deprecated
        public ActivationContext() { }

        public ActivationContext(ExecRow[] currentRows, ParameterValueSet pvs, Map<String, Integer> setOps,
                                 boolean statementAtomic, boolean statementReadOnly,
                                 String stmtText, boolean stmtRollBackParentContext, long stmtTimeout) {
            this.currentRows = currentRows;
            this.pvs = pvs;
            this.setOps = setOps;
            this.statementAtomic = statementAtomic;
            this.statementReadOnly = statementReadOnly;
            this.stmtText = stmtText;
            this.stmtRollBackParentContext = stmtRollBackParentContext;
            this.stmtTimeout = stmtTimeout;
        }

        public static ActivationContext create(Activation activation,SpliceOperation topOperation){
            List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
            Map<String,Integer> setOps = new HashMap<String,Integer>(operations.size());
            topOperation.generateLeftOperationStack(operations);
            for(Field field:activation.getClass().getDeclaredFields()){
                if(ResultSet.class.isAssignableFrom(field.getType())){
                    try {
                        if(!field.isAccessible())field.setAccessible(true); //make it accessible to me

                        final Object fieldValue = field.get(activation);
                        SpliceOperation op;
                        if (fieldValue instanceof OperationResultSet) {
                            op = ((OperationResultSet) fieldValue).getTopOperation();
                        } else {
                            op = (SpliceOperation) fieldValue;
                        }
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

        /*
         * Serialize out all the pieces of the StatementContext so that it can be recreated on the
         * other side
         */
            StatementContext context = activation.getLanguageConnectionContext().getStatementContext();
            boolean statementAtomic = context.isAtomic();
            boolean statementReadOnly = context.isForReadOnly();
            String stmtText = context.getStatementText();
            boolean stmtRollBackParentContext = true; //todo -sf- this is wrong, but okay for now
            long stmtTimeout = 0; //timeouts handled by RPC --probably wrong, but also okay for now

            ParameterValueSet pvs = activation.getParameterValueSet().getClone();
            return new ActivationContext(currentRows,pvs,setOps,statementAtomic,statementReadOnly,
                    stmtText,stmtRollBackParentContext,stmtTimeout);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(setOps);
            out.writeBoolean(statementAtomic);
            out.writeBoolean(statementReadOnly);
            out.writeUTF(stmtText);
            out.writeBoolean(stmtRollBackParentContext);
            out.writeLong(stmtTimeout);
            out.writeBoolean(pvs!=null);
            if(pvs!=null)
                out.writeObject(pvs);
            out.writeBoolean(currentRows!=null);
            if(currentRows!=null){
                ArrayUtil.writeArray(out,currentRows);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.setOps = (Map<String,Integer>)in.readObject();
            this.statementAtomic = in.readBoolean();
            this.statementReadOnly = in.readBoolean();
            this.stmtText = in.readUTF();
            this.stmtRollBackParentContext = in.readBoolean();
            this.stmtTimeout = in.readLong();
            if(in.readBoolean()){
                this.pvs = (ParameterValueSet)in.readObject();
            }
            if(in.readBoolean()){
                this.currentRows = new ExecRow[in.readInt()];
                ArrayUtil.readArrayItems(in,currentRows);
            }
        }

        public Activation populateActivation(Activation activation,GenericStorablePreparedStatement statement,SpliceOperation topOperation) throws StandardException {
		/*
		 * Set any currently populated rows back on to the activation
		 */
            try{
                if(currentRows!=null){
                    for(int i=0;i<currentRows.length;i++){
                        SerializingExecRow row = (SerializingExecRow)currentRows[i];
                        if(row!=null){
                            row.populateNulls(activation.getDataValueFactory());
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

                if(pvs!=null)activation.setParameters(pvs,statement.getParameterTypes());
                /*
                 * Push the StatementContext
                 */
                activation.getLanguageConnectionContext().pushStatementContext(statementAtomic,
                        statementReadOnly,stmtText,pvs,stmtRollBackParentContext,stmtTimeout);
                return activation;
            }catch (NoSuchFieldException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            } catch (IllegalAccessException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            }
            return null;
        }
    }
}