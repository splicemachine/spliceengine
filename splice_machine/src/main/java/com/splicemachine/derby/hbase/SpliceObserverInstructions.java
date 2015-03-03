package com.splicemachine.derby.hbase;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.iapi.sql.execute.ConversionResultSet;
import com.splicemachine.derby.iapi.sql.execute.ConvertedResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.impl.sql.GenericActivationHolder;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
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
		protected SpliceRuntimeContext spliceRuntimeContext;


		public SpliceObserverInstructions() {
				super();
				SpliceLogUtils.trace(LOG, "instantiated");
		}


		public SpliceObserverInstructions(GenericStorablePreparedStatement statement,
                                      SpliceOperation topOperation,
                                      ActivationContext activationContext,
                                      String sessionUserName, SchemaDescriptor defaultSchemaDescriptor,
                                      SpliceRuntimeContext spliceRuntimeContext) {
				SpliceLogUtils.trace(LOG, "instantiated with statement %s", statement);
				this.statement = statement;
				this.topOperation = topOperation;
				this.activationContext = activationContext;
				this.sessionUserName = sessionUserName;
				this.defaultSchemaDescriptor = defaultSchemaDescriptor;
				this.spliceRuntimeContext = spliceRuntimeContext;
		}

		public TxnView getTxn(){
        return spliceRuntimeContext.getTxn();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
				SpliceLogUtils.trace(LOG, "readExternal");
				this.statement = (GenericStorablePreparedStatement) in.readObject();
				this.topOperation = (SpliceOperation) in.readObject();
				this.activationContext = (ActivationContext)in.readObject();

				this.sessionUserName = in.readUTF();
				this.defaultSchemaDescriptor = (SchemaDescriptor) in.readObject();
				this.spliceRuntimeContext = (SpliceRuntimeContext) in .readObject();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				SpliceLogUtils.trace(LOG, "writeExternal");
				out.writeObject(statement);
				out.writeObject(topOperation);
				out.writeObject(activationContext);
				out.writeUTF(sessionUserName);
				out.writeObject(defaultSchemaDescriptor);
				out.writeObject(spliceRuntimeContext);
		}


		public SpliceRuntimeContext getSpliceRuntimeContext() {
				return spliceRuntimeContext;
		}

		public void setSpliceRuntimeContext(SpliceRuntimeContext spliceRuntimeContext) {
				this.spliceRuntimeContext = spliceRuntimeContext;
		}

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

		public SpliceOperation getTopOperation() {
				SpliceLogUtils.trace(LOG, "getTopOperation %s",topOperation);
				return topOperation;
		}

		public static SpliceObserverInstructions create(Activation activation,
																										SpliceOperation topOperation,
																										SpliceRuntimeContext spliceRuntimeContext){
				TxnView txn = ((SpliceTransactionManager)activation.getTransactionController()).getRawTransaction().getActiveStateTxn();
				return create(activation, topOperation, spliceRuntimeContext, txn);
		}

		public static SpliceObserverInstructions create(Activation activation,
																										SpliceOperation topOperation,
																										SpliceRuntimeContext spliceRuntimeContext,
																										TxnView txn) {
				ActivationContext activationContext = ActivationContext.create(activation, topOperation);

				return new SpliceObserverInstructions(
								(GenericStorablePreparedStatement) activation.getPreparedStatement(),
								topOperation,activationContext,
                activation.getLanguageConnectionContext().getSessionUserId(),
								activation.getLanguageConnectionContext().getDefaultSchema(),spliceRuntimeContext);
		}

		/*
		 * Serializer class for Activation objects, to ensure that they are properly sent over the wire
		 * while retaining their state completely.
		 *
		 * A lot of this crap is reflective Field setting, because there's currently no other way to make
		 * the activation properly serialize. Someday, it would be nice to move all of this into making
		 * Activation properly serializable.
		 */
		public static class ActivationContext implements Externalizable{
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
						topOperation.generateAllOperationStack(operations);
                        for(Field field:activation.getClass().getDeclaredFields()){
								if(!field.isAccessible())field.setAccessible(true); //make it accessible to me
								try{
										if(ResultSet.class.isAssignableFrom(field.getType())){

												final Object fieldValue = field.get(activation);
												SpliceOperation op;
												if (fieldValue instanceof ConvertedResultSet) {
														op = ((ConvertedResultSet) fieldValue).getOperation();
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

        /*
         * Serialize out all the pieces of the StatementContext so that it can be recreated on the
         * other side
         */
						StatementContext context = activation.getLanguageConnectionContext().getStatementContext();
						boolean statementAtomic = context.isAtomic();
						boolean statementReadOnly = context.isForReadOnly();
						String stmtText = context.getStatementText();
//						boolean stmtRollBackParentContext = true; //todo -sf- this is wrong, but okay for now
						long stmtTimeout = 0; //timeouts handled by RPC --probably wrong, but also okay for now

						ParameterValueSet pvs = activation.getParameterValueSet().getClone();

						Kryo kryo = SpliceDriver.getKryoPool().get();
						Output output = new Output(4096,-1);
						try {
								KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
								ActivationSerializer.write(activation,koo);
						} catch (IOException e) {
								throw new RuntimeException(e); //should never happen
						}finally{
								output.flush();
								SpliceDriver.getKryoPool().returnInstance(kryo);
						}
						return new ActivationContext(pvs,setOps,statementAtomic,statementReadOnly,
										stmtText, true,stmtTimeout,output.toBytes());
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
						out.writeInt(activationData.length);
						out.write(activationData);
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
						activationData = new byte[in.readInt()];
						in.readFully(activationData);
				}

				public Activation populateActivation(Activation activation,GenericStorablePreparedStatement statement,SpliceOperation topOperation) throws StandardException {
						try{
								Kryo kryo = SpliceDriver.getKryoPool().get();
								try{
										Input input = new Input(activationData);
										KryoObjectInput koi = new KryoObjectInput(input,kryo);
										ActivationSerializer.readInto(koi,activation);
								}finally{
										SpliceDriver.getKryoPool().returnInstance(kryo);
								}
			/*
			 * Set the populated operations with their comparable operation
			 */
								List<SpliceOperation> ops = new ArrayList<SpliceOperation>();
								topOperation.generateAllOperationStack(ops);
								for(String setField:setOps.keySet()){
										SpliceOperation op = ops.get(setOps.get(setField));
										Field fieldToSet = activation.getClass().getDeclaredField(setField);
										if(!fieldToSet.isAccessible())fieldToSet.setAccessible(true);
										ConversionResultSet crs = new ConversionResultSet(op,activation);
										fieldToSet.set(activation, crs);
								}


								if(pvs!=null)activation.setParameters(pvs,statement.getParameterTypes());
                /*
                 * Push the StatementContext
                 */
								activation.getLanguageConnectionContext().pushStatementContext(statementAtomic,
												statementReadOnly,stmtText,pvs,stmtRollBackParentContext,stmtTimeout);
								return activation;
						}catch (IOException e) {
								SpliceLogUtils.logAndThrowRuntime(LOG,e);
						} catch (IllegalAccessException e) {
								SpliceLogUtils.logAndThrowRuntime(LOG,e);
						} catch (NoSuchFieldException e) {
								SpliceLogUtils.logAndThrowRuntime(LOG,e);
						}
						return null;
				}

		}

		public SchemaDescriptor getDefaultSchemaDescriptor() {
				return defaultSchemaDescriptor;
		}

    public String getSessionUserName() {
				return sessionUserName;
		}

    public void setTxn(Txn txn){
        this.spliceRuntimeContext.setTxn(txn);
		}

}