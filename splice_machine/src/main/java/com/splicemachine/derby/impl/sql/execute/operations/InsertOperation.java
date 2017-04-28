/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.si.api.server.ClusterHealth;
import com.splicemachine.storage.Partition;
import com.splicemachine.derby.stream.output.HBaseBulkImporter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.HasIncrement;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.compile.InsertNode;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.actions.InsertConstantOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SequenceKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.function.InsertPairFunction;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.stream.output.insert.InsertPipelineWriter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;

import static com.splicemachine.derby.utils.BaseAdminProcedures.getDefaultConn;


/**
 * Operation that handles inserts into Splice Machine.
 *
 * @author Scott Fines
 */
public class InsertOperation extends DMLWriteOperation implements HasIncrement{
    private static final long serialVersionUID=1l;
    private static final Logger LOG=Logger.getLogger(InsertOperation.class);
    private ExecRow rowTemplate;
    private int[] pkCols;
    private long nextIncrement=-1;
    private RowLocation[] autoIncrementRowLocationArray;
    private SpliceSequence[] spliceSequences;
    protected static final String NAME=InsertOperation.class.getSimpleName().replaceAll("Operation","");
    public InsertPipelineWriter tableWriter;
    public Pair<Long, Long>[] defaultAutoIncrementValues;
    public InsertNode.InsertMode insertMode;
    public String statusDirectory;
    private int failBadRecordCount;
    private boolean skipConflictDetection;
    private boolean skipWAL;

    protected String bulkImportDirectory;
    protected boolean samplingOnly;
    protected boolean outputKeysOnly;
    protected boolean skipSampling;
    protected String indexName;

    @Override
    public String getName(){
        return NAME;
    }

    @SuppressWarnings("UnusedDeclaration")
    public InsertOperation(){
        super();
    }

    public InsertOperation(SpliceOperation source,
                           GeneratedMethod generationClauses,
                           GeneratedMethod checkGM,
                           String insertMode,
                           String statusDirectory,
                           int failBadRecordCount,
                           boolean skipConflictDetection,
                           boolean skipWAL,
                           double optimizerEstimatedRowCount,
                           double optimizerEstimatedCost,
                           String tableVersion,
                           String bulkImportDirectory,
                           boolean samplingOnly,
                           boolean outputKeysOnly,
                           boolean skipSampling,
                           String indexName) throws StandardException{
        super(source,generationClauses,checkGM,source.getActivation(),optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
        this.insertMode=InsertNode.InsertMode.valueOf(insertMode);
        this.statusDirectory=statusDirectory;
        this.skipConflictDetection=skipConflictDetection;
        this.skipWAL=skipWAL;
        this.failBadRecordCount = (failBadRecordCount >= 0 ? failBadRecordCount : -1);
        this.bulkImportDirectory = bulkImportDirectory;
        this.samplingOnly = samplingOnly;
        this.outputKeysOnly = outputKeysOnly;
        this.skipSampling = skipSampling;
        this.indexName = indexName;
        init();
    }

    @Override
    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION",justification = "Intentional")
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        try{
            super.init(context);
            source.init(context);
            writeInfo.initialize(context);
            heapConglom=writeInfo.getConglomerateId();
            pkCols=writeInfo.getPkColumnMap();
            autoIncrementRowLocationArray=writeInfo. getConstantAction()!=null &&
                    ((InsertConstantOperation)writeInfo.getConstantAction()).getAutoincRowLocation()!=null?
                    ((InsertConstantOperation)writeInfo.getConstantAction()).getAutoincRowLocation():new RowLocation[0];
            defaultAutoIncrementValues=WriteReadUtils.getStartAndIncrementFromSystemTables(autoIncrementRowLocationArray,
                    activation.getLanguageConnectionContext().getDataDictionary(),
                    heapConglom);
            spliceSequences=new SpliceSequence[autoIncrementRowLocationArray.length];
            int length=autoIncrementRowLocationArray.length;
            for(int i=0;i<length;i++){
                HBaseRowLocation rl=(HBaseRowLocation)autoIncrementRowLocationArray[i];
                if(rl==null){
                    spliceSequences[i]=null;
                }else{
                    byte[] rlBytes=rl.getBytes();
                    SConfiguration config=context.getSystemConfiguration();
                    SequenceKey key=new SequenceKey(
                            rlBytes,
                            isSingleRowResultSet()?1l:config.getSequenceBlockSize(),
                            defaultAutoIncrementValues[i].getFirst(),
                            defaultAutoIncrementValues[i].getSecond(),
                            SIDriver.driver().getTableFactory(),
                            SIDriver.driver().getOperationFactory());
                    spliceSequences[i]=EngineDriver.driver().sequencePool().get(key);
                }
            }
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }

    }

    public boolean isAboveFailThreshold(long numberOfErrors) {
        return this.failBadRecordCount >= 0 && numberOfErrors > this.failBadRecordCount;
    }

    @Override
    public String toString(){
        return "Insert{destTable="+heapConglom+",source="+source+"}";
    }

    @Override
    public String prettyPrint(int indentLevel){
        return "Insert"+super.prettyPrint(indentLevel);
    }

    /**
     *
     * Called from BaseActivation.getSetAutoIncrementValue
     *
     * @param columnPosition	position of the column in the table (1-based)
     * @param increment				the amount to increment by
     *
     * @return
     * @throws StandardException
     *
     * @see BaseActivation#getSetAutoincrementValue(int, long)
     */
    @Override
    public DataValueDescriptor increment(int columnPosition,long increment) throws StandardException{
        assert activation!=null && spliceSequences!=null:"activation or sequences are null";
        nextIncrement=((BaseActivation)activation).ignoreSequence()?-1:spliceSequences[columnPosition-1].getNext();
        this.getActivation().getLanguageConnectionContext().setIdentityValue(nextIncrement);
        if(rowTemplate==null)
            rowTemplate=getExecRowDefinition();
        DataValueDescriptor dvd=rowTemplate.cloneColumn(columnPosition);
        dvd.setValue(nextIncrement);
        return dvd;
    }

    @Override
    public void close() throws StandardException{
        super.close();
        if(nextIncrement!=-1) // Do we do this twice?
            this.getActivation().getLanguageConnectionContext().setIdentityValue(nextIncrement);
    }

    private boolean isSingleRowResultSet(){
        boolean isRow=false;
        if(source instanceof RowOperation)
            isRow=true;
        else if(source instanceof NormalizeOperation)
            isRow=(((NormalizeOperation)source).source instanceof RowOperation);
        return isRow;
    }

    @Override
    public void setActivation(Activation activation) throws StandardException{
        super.setActivation(activation);
        SpliceOperationContext context=SpliceOperationContext.newContext(activation);
        try{
            init(context);
        }catch(IOException ioe){
            throw StandardException.plainWrapException(ioe);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        autoIncrementRowLocationArray=new RowLocation[in.readInt()];
        for(int i=0;i<autoIncrementRowLocationArray.length;i++){
            autoIncrementRowLocationArray[i]=(HBaseRowLocation)in.readObject();
        }
        insertMode=InsertNode.InsertMode.valueOf(in.readUTF());
        if(in.readBoolean())
            statusDirectory=in.readUTF();
        failBadRecordCount=in.readInt();
        skipConflictDetection=in.readBoolean();
        skipWAL=in.readBoolean();
        bulkImportDirectory = in.readBoolean()?in.readUTF():null;
        samplingOnly = in.readBoolean();
        outputKeysOnly = in.readBoolean();
        skipSampling = in.readBoolean();
        if (in.readBoolean())
            indexName = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        int length=autoIncrementRowLocationArray.length;
        out.writeInt(length);
        for(int i=0;i<length;i++){
            out.writeObject(autoIncrementRowLocationArray[i]);
        }
        out.writeUTF(insertMode.toString());
        out.writeBoolean(statusDirectory!=null);
        if(statusDirectory!=null)
            out.writeUTF(statusDirectory);
        out.writeInt(failBadRecordCount);
        out.writeBoolean(skipConflictDetection);
        out.writeBoolean(skipWAL);
        out.writeBoolean(bulkImportDirectory!=null);
        if (bulkImportDirectory!=null)
            out.writeUTF(bulkImportDirectory);
        out.writeBoolean(samplingOnly);
        out.writeBoolean(outputKeysOnly);
        out.writeBoolean(skipSampling);
        out.writeBoolean(indexName != null);
        if (indexName != null)
            out.writeUTF(indexName);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        if(statusDirectory != null) {
            // if we have a status directory, we're an import and so permissive
            dsp.setPermissive(statusDirectory, getVTIFileName(), failBadRecordCount);
        }
        if (outputKeysOnly) {
            // For split key calculation, allow nonnullable column to be null
            if (source instanceof NormalizeOperation) {
                ((NormalizeOperation) source).setRequireNotNull(false);
            }
        }
        DataSet set=source.getDataSet(dsp);
        OperationContext operationContext=dsp.createOperationContext(this);
        ExecRow execRow=getExecRowDefinition();
        int[] execRowTypeFormatIds=WriteReadUtils.getExecRowTypeFormatIds(execRow);
        if(insertMode.equals(InsertNode.InsertMode.UPSERT) && pkCols==null)
            throw ErrorState.UPSERT_NO_PRIMARY_KEYS.newException(""+heapConglom+"");
        TxnView txn=getCurrentTransaction();

        ClusterHealth.ClusterHealthWatcher healthWatcher = null;
        if (skipWAL) {
            healthWatcher = SIDriver.driver().clusterHealth().registerWatcher();
        }

        operationContext.pushScope();
        try{
            if(statusDirectory!=null)
                dsp.setSchedulerPool("import");

            if (bulkImportDirectory!=null && bulkImportDirectory.compareToIgnoreCase("NULL") !=0) {
                HBaseBulkImporter importer = set.bulkImportData(operationContext)
                        .heapConglom(heapConglom)
                        .tableVersion(tableVersion)
                        .operationContext(operationContext)
                        .autoIncrementRowLocationArray(autoIncrementRowLocationArray)
                        .sequences(spliceSequences)
                        .pkCols(pkCols)
                        .execRow(getExecRowDefinition())
                        .txn(txn)
                        .bulkImportDirectory(bulkImportDirectory)
                        .samplingOnly(samplingOnly)
                        .outputKeysOnly(outputKeysOnly)
                        .skipSampling(skipSampling)
                        .indexName(indexName)
                        .build();
                return importer.write();
            }
            else {
                /*

                int[] pkCols,
                              String tableVersion,
                              ExecRow execRowDefinition,
                              RowLocation[] autoIncrementRowLocationArray,
                              SpliceSequence[] spliceSequences,
                              long heapConglom,
                              TxnView txn,
                              OperationContext operationContext,
                              boolean isUpsert
                 */
                PairDataSet dataSet = set.index(new InsertPairFunction(operationContext), true);
                DataSetWriter writer = dataSet.insertData(operationContext)
                        .autoIncrementRowLocationArray(autoIncrementRowLocationArray)
                        .execRowDefinition(getExecRowDefinition())
                        .execRowTypeFormatIds(execRowTypeFormatIds)
                        .sequences(spliceSequences)
                        .isUpsert(insertMode.equals(InsertNode.InsertMode.UPSERT))
                        .pkCols(pkCols)
                        .tableVersion(tableVersion)
                        .destConglomerate(heapConglom)
                        .operationContext(operationContext)
                        .txn(txn)
                        .build();
                return writer.write();
            }
        }finally{
            if (skipWAL) {
                flushAndCheckErrors(healthWatcher);
            }
            operationContext.popScope();
        }
    }

    private void flushAndCheckErrors(ClusterHealth.ClusterHealthWatcher healthWatcher) throws StandardException {
        PartitionFactory tableFactory = SIDriver.driver().getTableFactory();

        try {
            for (long cid : writeInfo.getIndexConglomerateIds()) {
                Partition table = tableFactory.getTable(Long.toString(cid));
                table.flush();
            }
            Partition table = tableFactory.getTable(Long.toString(writeInfo.getConglomerateId()));
            table.flush();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        try {
            if (healthWatcher.failedServers() > 0) {
                throw StandardException.newException(SQLState.REGION_SERVER_FAILURE_WITH_NO_WAL_ERROR, healthWatcher.failedServers());
            }
        } finally {
            healthWatcher.close();
        }
    }
    @Override
    public String getVTIFileName(){
        return getSubOperations().get(0).getVTIFileName();
    }


    public boolean skipConflictDetection() {
        return skipConflictDetection;
    }

    public boolean skipWAL() {
        return skipWAL;
    }
}
