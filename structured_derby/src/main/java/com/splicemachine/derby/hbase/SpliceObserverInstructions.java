package com.splicemachine.derby.hbase;

import com.splicemachine.derby.iapi.sql.execute.OperationResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManagerContext;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.ByteDataOutput;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.impl.sql.GenericActivationHolder;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    protected SchemaDescriptor defaultSchemaDescriptor;
    protected String sessionUserName;

    // propagate transactionId to all co-processors running operations for this SQL statement
    private String transactionId;

	public SpliceObserverInstructions() {
		super();
		SpliceLogUtils.trace(LOG, "instantiated");
	}

	public SpliceObserverInstructions(GenericStorablePreparedStatement statement,  SpliceOperation topOperation,
                                      ActivationContext activationContext, String transactionId, String sessionUserName, SchemaDescriptor defaultSchemaDescriptor) {
		SpliceLogUtils.trace(LOG, "instantiated with statement %s", statement);
		this.statement = statement;
		this.topOperation = topOperation;
        this.activationContext = activationContext;
        this.transactionId = transactionId;
        this.sessionUserName = sessionUserName;
        this.defaultSchemaDescriptor = defaultSchemaDescriptor;
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
        this.transactionId = in.readUTF();
        this.sessionUserName = in.readUTF();
        this.defaultSchemaDescriptor = (SchemaDescriptor) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		out.writeObject(statement);
		out.writeObject(topOperation);
        out.writeObject(activationContext);
        out.writeUTF(transactionId);
        out.writeUTF(sessionUserName);
        out.writeObject(defaultSchemaDescriptor);
	}
	/**
	 * Retrieve the GenericStorablePreparedStatement: This contains the byte code for the activation.
	 *
	 * @return
	 */
	public GenericStorablePreparedStatement getStatement() {
		SpliceLogUtils.trace(LOG, "getStatement %s",statement);
		return statement;
	}

    public Activation getActivation(LanguageConnectionContext lcc) throws StandardException {
        try{
        	GenericActivationHolder gah = (GenericActivationHolder)statement.getActivation(lcc,false);
        	this.statement.setActivationClass(gah.gc);
            Activation activation = gah.ac;
            return activationContext.populateActivation(activation,statement,topOperation);
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrow(LOG,e);
            return null; //never happen
        }
    }

	/**
	 * Retrieve the Top Operation: This can recurse to creat the operational stack for hbase for the scan.
	 *
	 * @return
	 */
	public SpliceOperation getTopOperation() {
		SpliceLogUtils.trace(LOG, "getTopOperation %s",topOperation);
		return topOperation;
    }

    public static SpliceObserverInstructions create(Activation activation,
                                                    SpliceOperation topOperation) {
        ActivationContext activationContext = ActivationContext.create(activation,topOperation);
        final String transactionID = getTransactionId(activation);

        return new SpliceObserverInstructions(
				(GenericStorablePreparedStatement) activation.getPreparedStatement(),
				topOperation,activationContext, transactionID, activation.getLanguageConnectionContext().getSessionUserId(), activation.getLanguageConnectionContext().getDefaultSchema() );
	}

    private static String getTransactionId(Activation activation) {
        final LanguageConnectionContext languageConnectionContext = activation.getLanguageConnectionContext();
        return getTransactionId(languageConnectionContext);
    }

    public static String getTransactionId(LanguageConnectionContext languageConnectionContext) {
        final ContextManager contextManager = languageConnectionContext.getContextManager();
        return getTransactionId(contextManager);
    }

    public static String getTransactionId(ContextManager contextManager) {
        SpliceTransactionManagerContext stmc = (SpliceTransactionManagerContext) contextManager.getContext(AccessFactoryGlobals.RAMXACT_CONTEXT_ID);
        if (stmc != null) {
            final SpliceTransactionManager transactionManager = stmc.getTransactionManager();
            final SpliceTransaction transaction = (SpliceTransaction) transactionManager.getRawStoreXact();
            return transaction.getTransactionId().getTransactionIdString();
        } else {
            return null;
        }
    }

    /*
     * Serializer class for Activation objects, to ensure that they are properly sent over the wire
     * while retaining their state completely.
     *
     * A lot of this crap is reflective Field setting, because there's currently no other way to make
     * the activation properly serialize. Someday, it would be nice to move all of this into making
     * Activation properly serializable.
     */
    private static class ActivationContext implements Externalizable{
        private static final long serialVersionUID = 5l;
        private Map<String,Integer> setOps;
        private boolean statementAtomic;
        private boolean statementReadOnly;
        private String stmtText;
        private boolean stmtRollBackParentContext;
        private long stmtTimeout;
        private ParameterValueSet pvs;
        private byte[] activationData;


        @SuppressWarnings("unused")
		public ActivationContext() { 
        	
        }

        public ActivationContext(ParameterValueSet pvs, Map<String, Integer> setOps,
                                 boolean statementAtomic, boolean statementReadOnly,
                                 String stmtText, boolean stmtRollBackParentContext, long stmtTimeout,
                                 byte[] activationData) {
            this.pvs = pvs;
            this.setOps = setOps;
            this.statementAtomic = statementAtomic;
            this.statementReadOnly = statementReadOnly;
            this.stmtText = stmtText;
            this.stmtRollBackParentContext = stmtRollBackParentContext;
            this.stmtTimeout = stmtTimeout;
            this.activationData = activationData;
        }

        public static ActivationContext create(Activation activation,SpliceOperation topOperation){
            List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
            Map<String,Integer> setOps = new HashMap<String,Integer>(operations.size());
            topOperation.generateLeftOperationStack(operations);
            for(Field field:activation.getClass().getDeclaredFields()){
                if(!field.isAccessible())field.setAccessible(true); //make it accessible to me
                try{
                    if(ResultSet.class.isAssignableFrom(field.getType())){

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
                    }
                } catch (IllegalAccessException e) {
                    SpliceLogUtils.logAndThrowRuntime(LOG,e);
                }
            }

//		/*
//		 * Serialize out any non-null result rows that are currently stored in the activation.
//		 *
//		 * This is necessary if you are pushing out a set of Operation to a Table inside of a Sink.
//		 */
//            int rowPos=1;
//            Map<Integer,ExecRow> rowMap = new HashMap<Integer,ExecRow>();
//            boolean shouldContinue=true;
//            while(shouldContinue){
//                try{
//                    ExecRow row = (ExecRow)activation.getCurrentRow(rowPos);
//                    if(row!=null){
//                        rowMap.put(rowPos,row);
//                    }
//                    rowPos++;
//                }catch(IndexOutOfBoundsException ie){
//                    //we've reached the end of the row group in activation, so stop
//                    shouldContinue=false;
//                }
//            }
//            ExecRow[] currentRows = new ExecRow[rowPos];
//            for(Integer rowPosition:rowMap.keySet()){
//                ExecRow row = new SerializingExecRow(rowMap.get(rowPosition));
//                if(row instanceof ExecIndexRow)
//                    currentRows[rowPosition] = new SerializingIndexRow(row);
//                else
//                    currentRows[rowPosition] =  row;
//            }
//            SpliceLogUtils.trace(LOG,"serializing current rows: %s", Arrays.toString(currentRows));

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

            ByteDataOutput bdo = new ByteDataOutput();
            try {
                ActivationSerializer.write(activation,bdo);
                return new ActivationContext(pvs,setOps,statementAtomic,statementReadOnly,
                        stmtText,stmtRollBackParentContext,stmtTimeout,bdo.toByteArray());
            } catch (IOException e) {
                throw new RuntimeException(e); //should never happen
            }
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
            out.writeObject(activationData);
        }

        @SuppressWarnings("unchecked")
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
            activationData = (byte[])in.readObject();
        }

        public Activation populateActivation(Activation activation,GenericStorablePreparedStatement statement,SpliceOperation topOperation) throws StandardException {

            try{
                ActivationSerializer.readInto(new ByteDataInput(activationData),activation);
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
            } catch (IOException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            }
            return null;
        }

    }

	public SchemaDescriptor getDefaultSchemaDescriptor() {
		return defaultSchemaDescriptor;
	}

	public void setDefaultSchemaDescriptor(SchemaDescriptor defaultSchemaDescriptor) {
		this.defaultSchemaDescriptor = defaultSchemaDescriptor;
	}

	public String getSessionUserName() {
		return sessionUserName;
	}

	public void setSessionUserName(String sessionUserName) {
		this.sessionUserName = sessionUserName;
	}    
}