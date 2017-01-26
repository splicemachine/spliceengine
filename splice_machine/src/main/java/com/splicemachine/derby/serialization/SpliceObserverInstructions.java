/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.EngineDriver;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedConnectionContext;
import com.splicemachine.db.impl.sql.GenericActivationHolder;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionStack;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Class utilized to serialize the Activation and related elments for Spark.  It attaches the
 * GenericStorablePreparedStatement and the Top Operation.
 *
 */
public class SpliceObserverInstructions implements Externalizable{
    private static final long serialVersionUID=4l;
    private static Logger LOG=Logger.getLogger(SpliceObserverInstructions.class);
    protected GenericStorablePreparedStatement statement;
    private ActivationContext activationContext;
    protected SchemaDescriptor defaultSchemaDescriptor;
    protected String sessionUserName;
    private TriggerExecutionStack triggerStack;

    public SpliceObserverInstructions(){
        super();
        SpliceLogUtils.trace(LOG,"instantiated");
    }


    public SpliceObserverInstructions(GenericStorablePreparedStatement statement,
                                      ActivationContext activationContext,
                                      String sessionUserName,SchemaDescriptor defaultSchemaDescriptor,
                                      TriggerExecutionStack triggerStack){
        SpliceLogUtils.trace(LOG,"instantiated with statement %s",statement);
        this.statement=statement;
        this.activationContext=activationContext;
        this.sessionUserName=sessionUserName;
        this.defaultSchemaDescriptor=defaultSchemaDescriptor;
        this.triggerStack=triggerStack;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        SpliceLogUtils.trace(LOG,"readExternal");
        this.statement=(GenericStorablePreparedStatement)in.readObject();
        this.activationContext=(ActivationContext)in.readObject();
        this.sessionUserName=in.readUTF();
        this.defaultSchemaDescriptor=(SchemaDescriptor)in.readObject();
        this.triggerStack=(TriggerExecutionStack)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        SpliceLogUtils.trace(LOG,"writeExternal");
        out.writeObject(statement);
        out.writeObject(activationContext);
        out.writeUTF(sessionUserName);
        out.writeObject(defaultSchemaDescriptor);
        out.writeObject(triggerStack);
    }

    public GenericStorablePreparedStatement getStatement(){
        return statement;
    }

    public Activation getActivation(ActivationHolder holder,LanguageConnectionContext lcc) throws StandardException{
        try{
            GenericActivationHolder gah=(GenericActivationHolder)statement.getActivation(lcc,false);
            holder.setActivation(gah.ac);
            statement.setActivationClass(gah.gc);
            Activation activation=activationContext.populateActivation(holder,statement);
            if(triggerStack!=null){
                activation.getLanguageConnectionContext().setTriggerStack(triggerStack);
            }
            return activation;
        }catch(StandardException e){
            SpliceLogUtils.logAndThrow(LOG,e);
            return null; //never happen
        }
    }

    public static SpliceObserverInstructions create(ActivationHolder holder){
        ActivationContext activationContext=ActivationContext.create(holder);
        LanguageConnectionContext lcc=holder.getActivation().getLanguageConnectionContext();
        TriggerExecutionStack triggerExecutionStack=null;
        if(lcc.hasTriggers()){
            triggerExecutionStack=lcc.getTriggerStack();
        }

        return new SpliceObserverInstructions(
                (GenericStorablePreparedStatement)holder.getActivation().getPreparedStatement(),
                activationContext,
                lcc.getSessionUserId(),
                lcc.getDefaultSchema(),
                triggerExecutionStack);
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
        private static final long serialVersionUID=5l;
        private boolean statementAtomic;
        private boolean statementReadOnly;
        private String stmtText;
        private boolean stmtRollBackParentContext;
        private long stmtTimeout;
        private ParameterValueSet pvs;
        private byte[] activationData;


        @SuppressWarnings("unused")
        public ActivationContext(){

        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
        public ActivationContext(ParameterValueSet pvs,
                                 boolean statementAtomic,boolean statementReadOnly,
                                 String stmtText,boolean stmtRollBackParentContext,long stmtTimeout,
                                 byte[] activationData){
            this.pvs=pvs;
            this.statementAtomic=statementAtomic;
            this.statementReadOnly=statementReadOnly;
            this.stmtText=stmtText;
            this.stmtRollBackParentContext=stmtRollBackParentContext;
            this.stmtTimeout=stmtTimeout;
            this.activationData=activationData;
        }

        public static ActivationContext create(ActivationHolder holder){
            /*
             * Serialize out all the pieces of the StatementContext so that it can be recreated on the
             * other side
             */
            StatementContext context=holder.getActivation().getLanguageConnectionContext().getStatementContext();
            boolean statementAtomic=context.isAtomic();
            boolean statementReadOnly=context.isForReadOnly();
            String stmtText=context.getStatementText();
//						boolean stmtRollBackParentContext = true; //todo -sf- this is wrong, but okay for now
            long stmtTimeout=0; //timeouts handled by RPC --probably wrong, but also okay for now

            ParameterValueSet pvs=holder.getActivation().getParameterValueSet().getClone();

            KryoPool kryoPool=SpliceKryoRegistry.getInstance();
            Kryo kryo=kryoPool.get();
            Output output=new Output(4096,-1);
            try{
                KryoObjectOutput koo=new KryoObjectOutput(output,kryo);
                ActivationSerializer.write(holder,koo);
            }catch(IOException e){
                throw new RuntimeException(e); //should never happen
            }finally{
                output.flush();
                kryoPool.returnInstance(kryo);
            }
            return new ActivationContext(pvs,statementAtomic,statementReadOnly,
                    stmtText,true,stmtTimeout,output.toBytes());
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException{
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
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
            this.statementAtomic=in.readBoolean();
            this.statementReadOnly=in.readBoolean();
            this.stmtText=in.readUTF();
            this.stmtRollBackParentContext=in.readBoolean();
            this.stmtTimeout=in.readLong();
            if(in.readBoolean()){
                this.pvs=(ParameterValueSet)in.readObject();
            }
            activationData=new byte[in.readInt()];
            in.readFully(activationData);
        }

        public Activation populateActivation(ActivationHolder holder,GenericStorablePreparedStatement statement) throws StandardException{
            try{
                KryoPool kryoPool=SpliceKryoRegistry.getInstance();
                Kryo kryo=kryoPool.get();
                try{
                    Input input=new Input(activationData);
                    KryoObjectInput koi=new KryoObjectInput(input,kryo);
                    ActivationSerializer.readInto(koi,holder);
                }finally{
                    kryoPool.returnInstance(kryo);
                }

                Activation activation = holder.getActivation();
                if(pvs!=null) activation.setParameters(pvs,statement.getParameterTypes());
                /*
                 * Push the StatementContext
                 */
                StatementContext statementContext = activation.getLanguageConnectionContext().pushStatementContext(statementAtomic,
                        statementReadOnly,stmtText,pvs,stmtRollBackParentContext,stmtTimeout);
                statementContext.setSQLAllowed(RoutineAliasInfo.MODIFIES_SQL_DATA, false);

                //EmbedConnection internalConnection=(EmbedConnection) new EmbedConnectionMaker().createNew(new Properties());
                EmbedConnection internalConnection=(EmbedConnection)EngineDriver.driver().getInternalConnection();
                Context connectionContext = new EmbedConnectionContext(activation.getLanguageConnectionContext().getContextManager(),
                        (EmbedConnection)internalConnection);

                return activation;
            }catch(Exception e){
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            }
            return null;
        }

    }

    public SchemaDescriptor getDefaultSchemaDescriptor(){
        return defaultSchemaDescriptor;
    }

    public String getSessionUserName(){
        return sessionUserName;
    }

}