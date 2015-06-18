package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.actions.WriteCursorConstantOperation;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 10/4/13
 */
public class DerbyDMLWriteInfo implements DMLWriteInfo {
    private transient Activation activation;
    private String tableVersion;
    private TxnView txn;

    @Override
    public void initialize(SpliceOperationContext opCtx) throws StandardException {
        this.activation = opCtx.getActivation();
        if(tableVersion==null){
            final long conglomId = getConglomerateId();
            try {
                this.tableVersion = DerbyScanInformation.tableVersionCache.get(conglomId,new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        DataDictionary dataDictionary = activation.getLanguageConnectionContext().getDataDictionary();
                        UUID tableID = dataDictionary.getConglomerateDescriptor(conglomId).getTableID();
                        TableDescriptor td = dataDictionary.getTableDescriptor(tableID);
                        return td.getVersion();
                    }
                });
            } catch (ExecutionException e) {
                throw Exceptions.parseException(e);
            }
        }
        this.txn = opCtx.getTxn();
    }

    @Override
    public ConstantAction getConstantAction() {
        return activation.getConstantAction();
    }

    @Override
    public FormatableBitSet getPkColumns() {
        return fromIntArray(getPkColumnMap());
    }

    @Override
    public int[] getPkColumnMap() {
        ConstantAction action = getConstantAction();
        return ((WriteCursorConstantOperation)action).getPkColumns();
    }

    @Override
    public long getConglomerateId() {
        return ((WriteCursorConstantOperation)getConstantAction()).getConglomerateId();
    }

		@Override
    public SpliceObserverInstructions buildInstructions(SpliceOperation operation) {
        return SpliceObserverInstructions.create(activation,operation,new SpliceRuntimeContext(txn));
    }

		@Override
		public ResultDescription getResultDescription() {
				return activation.getResultDescription();
		}

		@Override
		public String getTableVersion() {
				return tableVersion;
		}

		@Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(tableVersion!=null);
        if(tableVersion!=null)
            out.writeUTF(tableVersion);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean())
            tableVersion = in.readUTF();
    }

    public static FormatableBitSet fromIntArray(int[] values){
        if(values ==null) return null;
        FormatableBitSet fbt = new FormatableBitSet(values.length);
        for(int value:values){
            fbt.grow(value);
            fbt.set(value-1);
        }
        return fbt;
    }
}
