package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.execute.FKInfo;
import org.apache.derby.impl.sql.execute.RISetChecker;
import org.apache.derby.impl.sql.execute.TriggerEventActivator;
import org.apache.derby.impl.sql.execute.TriggerInfo;
import org.apache.derby.impl.sql.execute.UpdateConstantAction;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;

/**
 * Represents and Update SQL Operation
 * @author Scott Fines
 * Date: 1/16/13 2:16PM
 */
public class UpdateOperation extends SpliceBaseOperation{
    private TransactionController tc;
    private ExecRow newBaseRow;
    private ExecRow row;
    private ExecRow deferredSparseRow;

    UpdateConstantAction constants;

    private ResultDescription resultDescription;
    private NoPutResultSet source;
    NoPutResultSet savedSource;

    protected ConglomerateController deferredBaseCC;

    protected long[] deferredUniqueCCs;
    protected boolean[] deferredUniqueCreated;
    protected ConglomerateController[] deferredUniqueCC;
    protected ScanController[] deferredUniqueScans;

    private RISetChecker riChecker;
    private TriggerInfo triggerInfo;
    private TriggerEventActivator triggerActivator;
    private boolean					updatingReferencedKey;
    private boolean					updatingForeignKey;
    private	int						numOpens;
    private long					heapConglom;
    private FKInfo[]				fkInfoArray;
    private FormatableBitSet baseRowReadList;
    private GeneratedMethod generationClauses;
    private GeneratedMethod			checkGM;
    private int						resultWidth;
    private int						numberOfBaseColumns;
    private ExecRow					deferredTempRow;
    private ExecRow					deferredBaseRow;
    private ExecRow					oldDeletedRow;
    private ResultDescription		triggerResultDescription;

    int lockMode;
    boolean deferred;
    boolean beforeUpdateCopyRequired = false;

    public UpdateOperation(NoPutResultSet source, GeneratedMethod generationClauses,
			GeneratedMethod checkGM, Activation activation) {
    	this.source = source;
    	this.generationClauses = generationClauses;
    	this.checkGM = checkGM;
    	this.activation = activation;
    }
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void openCore() throws StandardException {
        if(source!=null)source.openCore();
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
