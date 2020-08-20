/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.catalog.types.UserDefinedTypeIdImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.execute.TriggerInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.sql.execute.actions.WriteCursorConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.DMLWriteInfo;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Strings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import static com.splicemachine.derby.impl.sql.execute.operations.DMLTriggerEventMapper.getAfterEvent;
import static com.splicemachine.derby.impl.sql.execute.operations.DMLTriggerEventMapper.getBeforeEvent;


/**
 * @author Scott Fines
 */
public abstract class DMLWriteOperation extends SpliceBaseOperation {
    private static final long serialVersionUID=2l;
    private static final Logger LOG=Logger.getLogger(DMLWriteOperation.class);
    protected SpliceOperation source;
    protected long heapConglom;
    protected DataDictionary dd;
    protected TableDescriptor td;
    private boolean isScan=true;
    protected DMLWriteInfo writeInfo;
    protected TriggerHandler triggerHandler;
    private SpliceMethod<ExecRow> generationClauses;
    private String generationClausesFunMethodName;
    private SpliceMethod<ExecRow> checkGM;
    private String checkGMFunMethodName;
    protected String tableVersion;
    protected DataSet<ExecRow> sourceSet;
    protected boolean isSpark;
    protected Txn nestedTxn = null;
    protected boolean exceptionHit = false;

    public DMLWriteOperation(){
        super();
    }

    public DMLWriteOperation(SpliceOperation source,Activation activation,
                             double optimizerEstimatedRowCount,
                             double optimizerEstimatedCost,String tableVersion) throws StandardException{
        super(activation,-1,optimizerEstimatedRowCount,optimizerEstimatedCost);
        this.source=source;
        this.activation=activation;
        this.tableVersion=tableVersion;
        this.writeInfo=new DerbyDMLWriteInfo();
    }

    public DMLWriteOperation(SpliceOperation source,
                             GeneratedMethod generationClauses,
                             GeneratedMethod checkGM,
                             Activation activation,double optimizerEstimatedRowCount,
                             double optimizerEstimatedCost,String tableVersion) throws StandardException{
        this(source,activation,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);

        if(generationClauses!=null){
            this.generationClausesFunMethodName=generationClauses.getMethodName();
            this.generationClauses=new SpliceMethod<>(generationClausesFunMethodName,activation);
        }
        if(checkGM!=null){
            this.checkGMFunMethodName=checkGM.getMethodName();
            this.checkGM=new SpliceMethod<>(checkGMFunMethodName,activation);
        }

    }

    public boolean hasGenerationClause() {
        return generationClausesFunMethodName != null || checkGMFunMethodName != null;
    }

    public long getTriggerConglomID() {
        if (triggerHandler != null) {
            return triggerHandler.getTriggerConglomID();
        }
        return 0;
    }

    // If executing on control, the nested transaction is
    // allocated in ControlDataSetWriter, otherwise it
    // is allocated here.  If we don't get a nested transaction,
    // any data written to tables that needs to be accessed by
    // AFTER statement triggers won't be visible.  Spark begins the
    // nested transaction for the write operation separate from the
    // read operation, so the read in the trigger looks like it is a child transaction
    // directly under the upper-level statement's transaction instead of under the
    // transaction of the trigger substatement.  For example:
    //
    //    create table t1 (a int, b int);
    //    create table t2 (a int, b int);
    //    create table t3 (a int, b int);
    //    CREATE TRIGGER mytrig
    //       AFTER INSERT
    //       ON t2
    //       REFERENCING NEW_TABLE AS NEW
    //       FOR EACH STATEMENT
    //    -- triggered statement
    //    insert into t3 select * from t2;
    //
    //    insert into t2 select * from t1;
    //
    // The write into t2 occurs in a child transaction of a parent transaction
    // that is also the parent of the read from t2 in the triggered statement, making
    // the write to t2 invisible to the read (see AbstractTxnView::canSee).
    // By making a global child transaction for each write operation we can
    // ensure that the read and the write don't share the same
    // parent transaction.
    protected TxnView getTransactionForWrite(DataSetProcessor dsp) throws StandardException{
        if (nestedTxn != null)
            return nestedTxn;
        TxnView txn = null;
        TxnView parent = getCurrentTransaction();

        try {
            if (dsp.getType() == DataSetProcessor.Type.SPARK || isOlapServer())
            {
                nestedTxn =
                    SIDriver.driver().lifecycleManager().beginChildTransaction(
                            parent,
                            parent.getIsolationLevel(),
                            parent.isAdditive(),
                            Bytes.toBytes(Long.toString(heapConglom)),
                            false);

                txn = nestedTxn;
            }
            else
                txn = parent;
        }
        catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        return txn;
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE", justification = "DB-9844")
     public void finalizeNestedTransaction() throws StandardException {
        try {
             if (nestedTxn != null) {
                 if (exceptionHit)
                     nestedTxn.rollback();
                 else {
                     nestedTxn.commit();
                 }
             }
         }
         catch (Exception e) {
            try {
                if (nestedTxn != null &&
                    nestedTxn.getState() != Txn.State.COMMITTED &&
                    nestedTxn.getState() != Txn.State.ROLLEDBACK)
                    nestedTxn.rollback();
                exceptionHit = true;
            }
            catch (IOException ioE) {
                throw Exceptions.parseException(ioE);
            }
            throw Exceptions.parseException(e);
         }
     }

    public DataSet<ExecRow> getSourceSet() {
        return sourceSet;
    }

    public void setSourceSet(DataSet<ExecRow> sourceSet) {
        this.sourceSet = sourceSet;
    }

    public String getTableVersion() { return tableVersion; }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        super.readExternal(in);
        source=(SpliceOperation)in.readObject();
        writeInfo=(DMLWriteInfo)in.readObject();
        generationClausesFunMethodName=readNullableString(in);
        checkGMFunMethodName=readNullableString(in);
        heapConglom=in.readLong();
        tableVersion=in.readUTF();
        isSpark = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeObject(source);
        out.writeObject(writeInfo);
        writeNullableString(generationClausesFunMethodName,out);
        writeNullableString(checkGMFunMethodName,out);
        out.writeLong(heapConglom);
        out.writeUTF(tableVersion);
        out.writeBoolean(isOlapServer());
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"DMLWriteOperation#init");
        super.init(context);
        source.init(context);
        writeInfo.initialize(context);
        if (isOlapServer())
            isSpark = true;

        WriteCursorConstantOperation constantAction=(WriteCursorConstantOperation)writeInfo.getConstantAction();

        TriggerInfo triggerInfo=constantAction.getTriggerInfo();

        if(triggerInfo!=null && !(this instanceof UpdateOperation)){
            this.triggerHandler=new TriggerHandler(
                    triggerInfo,
                    writeInfo,
                    getActivation(),
                    getBeforeEvent(getClass()),
                    getAfterEvent(getClass()),
                    null,
                    getExecRowDefinition(),
                    getTableVersion()
            );
            this.triggerHandler.setIsSpark(isSpark);
        }
    }

    public byte[] getDestinationTable(){
        return Bytes.toBytes(Long.toString(heapConglom));
    }

    @Override
    public SpliceOperation getLeftOperation(){
        return source;
    }

    @Override
    public List<SpliceOperation> getSubOperations(){
        return Collections.singletonList(source);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException{
                /*
				 * Typically, we just call down to our source and then pass that along
				 * unfortunately, with autoincrement columns this can lead to a
				 * StackOverflow, so we can't do that(see DB-1098 for more info)
				 *
				 * Luckily, DML operations are the top of their stack, so we can
				 * just form our exec row from our result description.
				 */
        ResultDescription description=writeInfo.getResultDescription();
        ResultColumnDescriptor[] rcd=description.getColumnInfo();
        DataValueDescriptor[] dvds=new DataValueDescriptor[rcd.length];
        for(int i=0;i<rcd.length;i++){
            dvds[i]=rcd[i].getType().getNull();
            TypeId typeId=rcd[i].getType().getTypeId();
            if(typeId.getTypeFormatId()==StoredFormatIds.USERDEFINED_TYPE_ID_V3){
                UserDefinedTypeIdImpl udt=(UserDefinedTypeIdImpl)typeId.getBaseTypeId();
                try{
                    if(udt!=null){
                        LanguageConnectionContext lcc=activation.getLanguageConnectionContext();
                        ClassFactory cf=lcc.getLanguageConnectionFactory().getClassFactory();
                        Class UDTBaseClazz=cf.loadApplicationClass(ClassName.UDTBase);
                        Class clazz=cf.loadApplicationClass(udt.getClassName());
                        if(UDTBaseClazz.isAssignableFrom(clazz)){
                            // For UDTs, create an instance of concrete type, so that UDTSerializer will
                            // be pick to serialize/deserialize it.
                            Object o=clazz.newInstance();
                            dvds[i].setValue(o);
                        }
                    }
                }catch(Exception e){
                    throw StandardException.newException(e.getLocalizedMessage());
                }
            }
        }
        ExecRow row=new ValueRow(dvds.length);
        row.setRowArray(dvds);
        SpliceLogUtils.trace(LOG,"execRowDefinition=%s",row);
        return row;
    }

    public String[] getColumnNames() {
        ResultDescription description=writeInfo.getResultDescription();
        ResultColumnDescriptor[] rcd=description.getColumnInfo();
        String[] colNames = new String[rcd.length];
        for (int i=0 ; i<rcd.length ; i++) {
            colNames[i] = rcd[i].getName();
        }
        return colNames;
    }

    public String prettyPrint(int indentLevel){
        String indent="\n"+ Strings.repeat("\t", indentLevel);

        return indent+"resultSetNumber:"+resultSetNumber+indent
                +"heapConglom:"+heapConglom+indent
                +"isScan:"+isScan+indent
                +"writeInfo:"+writeInfo+indent
                +"source:"+source.prettyPrint(indentLevel+1);
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException{
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber){
        return source.isReferencingTable(tableNumber);
    }

    public void fireBeforeStatementTriggers() throws StandardException{
        if(triggerHandler!=null)
            triggerHandler.fireBeforeStatementTriggers();
    }

    public void fireAfterStatementTriggers() throws StandardException{
        if(triggerHandler!=null) {
            triggerHandler.fireAfterStatementTriggers();
        }
    }

    public void fireBeforeRowTriggers() throws StandardException{
        TriggerHandler.fireBeforeRowTriggers(triggerHandler,getCurrentRow());
    }

    @Override
    public TriggerHandler getTriggerHandler(){
        return triggerHandler;
    }

    /**
     * Compute the generation clauses, if any, on the current row in order to fill in
     * computed columns.
     *
     * @param newRow the base row being evaluated
     */
    public void evaluateGenerationClauses(ExecRow newRow) throws StandardException{
        if(generationClausesFunMethodName==null && checkGMFunMethodName==null)
            return;
        if(generationClausesFunMethodName!=null){
            if(generationClauses==null)
                this.generationClauses=new SpliceMethod<>(generationClausesFunMethodName,activation);
        }
        if(checkGMFunMethodName!=null){
            if(checkGM==null || source.getActivation() != checkGM.getActivation())
                this.checkGM=new SpliceMethod<>(checkGMFunMethodName,activation);
        }
        ExecRow oldRow=(ExecRow)activation.getCurrentRow(source.resultSetNumber());
        //
        // The generation clause may refer to other columns in this row.
        //
        try{
            source.setCurrentRow(newRow);
            // this is where the magic happens
            if(generationClausesFunMethodName!=null)
                generationClauses.invoke();
            if(checkGMFunMethodName!=null)
                checkGM.invoke();
        }finally{
            //
            // We restore the Activation to its state before we ran the generation
            // clause. This may not be necessary but I don't understand all of
            // the paths through the Insert and Update operations. This
            // defensive coding seems prudent to me.
            //
            if(oldRow==null){
                source.clearCurrentRow();
            }else{
                source.setCurrentRow(oldRow);
            }
        }
    }

    @Override
    public void close() throws StandardException {
        if (triggerHandler!=null) {
            triggerHandler.cleanup();

            // If we have triggers, wrap the next operations into another transaction to prevent the next transaction from ignoring
            // our writes, see SPLICE-1625
            TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
            Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
            BaseSpliceTransaction rawTxn = (BaseSpliceTransaction) rawStoreXact;
            if (rawTxn instanceof SpliceTransaction) {
                rawTxn.setSavePoint("triggers", null);
                ((SpliceTransaction) rawTxn).elevate(getDestinationTable());
            }
        }
        super.close();
    }

    @Override
    public TxnView getCurrentTransaction() throws StandardException{
        return elevateTransaction();
    }

    @Override
    public void openCore() throws StandardException {
        super.openCore();
        /*
        We have to compute modifiedRowCount and badRecords here because if there's an Exception it has to
        propagate from here, otherwise Derby code down the line won't clean up things properly, see SPLICE-1470
         */
        computeModifiedRows();
    }

    protected Pair<DataSet,int[]> getBatchedDataset(DataSetProcessor dsp) throws StandardException {
        DataSet set;
        OperationContext operationContext=dsp.createOperationContext(this);
        int[] expectedUpdatecounts = null;
        if (activation.isBatched() && !dsp.isSparkExplain()) {
            /*
             If we are executing batched operations we gather all modified rows into a single dataset by collecting
             one dataset for each original batched statement and then unioning them all together
              */
            List<DataSet> sets = new LinkedList<>();
            List<Integer> counts = new ArrayList<>();
            do {
                Pair<DataSet, Integer> pair = source.getDataSet(dsp).shufflePartitions().materialize();
                sets.add(pair.getFirst());
                counts.add(pair.getSecond());
            } while (activation.nextBatchElement()); // Iterate over each batched statement

            /*
            When we update each row (or insert/delete them) we do them all at once, so we can't map how many rows were affected
            per original statement. That's why we need expectedUpdatecounts, which keeps track of how many rows were affected per statement
             */
            expectedUpdatecounts = new int[sets.size()];
            Iterator<Integer> it = counts.iterator();
            for(int i = 0; i < expectedUpdatecounts.length; ++i) {
                expectedUpdatecounts[i] = it.next();
            }

            while(sets.size() > 1) {
                DataSet left = sets.remove(0);
                DataSet right = sets.remove(0);
                sets.add(left.union(right, operationContext));
            }

            set = sets.get(0);
        } else {
            dsp.incrementOpDepth();
            set = source.getDataSet(dsp);
            if (!dsp.isSparkExplain())
                set=set.shufflePartitions();
            dsp.decrementOpDepth();
        }
        setSourceSet(set);
        return Pair.newPair(set, expectedUpdatecounts);
    }

    public boolean hasTriggers() { return triggerHandler != null; }

    public boolean hasStatementTriggerWithReferencingClause() {
        if (triggerHandler != null)
            return triggerHandler.hasStatementTriggerWithReferencingClause();
        else
            return false;
    }
}
