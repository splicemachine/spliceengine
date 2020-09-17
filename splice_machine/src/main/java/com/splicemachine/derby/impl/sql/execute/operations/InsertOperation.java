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

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.HasIncrement;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.compile.InsertNode;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.sql.execute.TriggerRowHolderImpl;
import com.splicemachine.derby.impl.sql.execute.actions.InsertConstantOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SequenceKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.stream.output.insert.InsertPipelineWriter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.server.ClusterHealth;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.system.CsvOptions;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Pair;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;


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
    protected String delimited;
    protected String escaped;
    protected String lines;
    protected String storedAs;
    protected String location;
    protected String compression;
    protected int partitionByRefItem;
    protected int[] partitionBy;
    private boolean skipConflictDetection;
    private boolean skipWAL;
    protected String bulkImportDirectory;
    protected boolean samplingOnly;
    protected boolean outputKeysOnly;
    protected boolean skipSampling;
    protected String indexName;
    protected double sampleFraction;

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
                           String delimited,
                           String escaped,
                           String lines,
                           String storedAs,
                           String location,
                           String compression,
                           int partitionByRefItem,
                           String bulkImportDirectory,
                           boolean samplingOnly,
                           boolean outputKeysOnly,
                           boolean skipSampling,
                           double sampleFraction,
                           String indexName) throws StandardException{
        super(source,generationClauses,checkGM,source.getActivation(),optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
        this.insertMode=InsertNode.InsertMode.valueOf(insertMode);
        this.statusDirectory=statusDirectory;
        this.skipConflictDetection=skipConflictDetection;
        this.skipWAL=skipWAL;
        this.failBadRecordCount = (failBadRecordCount >= 0 ? failBadRecordCount : -1);
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
        this.storedAs = storedAs;
        this.location = location;
        this.compression = compression;
        this.partitionByRefItem = partitionByRefItem;
        this.bulkImportDirectory = bulkImportDirectory;
        this.samplingOnly = samplingOnly;
        this.outputKeysOnly = outputKeysOnly;
        this.skipSampling = skipSampling;
        this.sampleFraction = sampleFraction;
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

            GenericStorablePreparedStatement statement = context.getPreparedStatement();
            if (this.storedAs == null || partitionByRefItem ==-1)
                this.partitionBy = TableDescriptor.EMPTY_PARTITON_ARRAY;
            else
                this.partitionBy = ((ReferencedColumnsDescriptorImpl) statement.getSavedObject(partitionByRefItem)).getReferencedColumnPositions();
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
        long nextIdentityColumnValue;
        assert activation!=null && spliceSequences!=null:"activation or sequences are null";
        nextIdentityColumnValue=((BaseActivation)activation).ignoreSequence()?-1:spliceSequences[columnPosition-1].getNext();
        if(rowTemplate==null)
            rowTemplate=getExecRowDefinition();
        DataValueDescriptor dvd=rowTemplate.cloneColumn(columnPosition);
        dvd.setValue(nextIdentityColumnValue);
        synchronized (this) {
            this.getActivation().getLanguageConnectionContext().setIdentityValue(nextIncrement);
            if (increment > 0) {
                if (nextIdentityColumnValue > nextIncrement)
                    nextIncrement = nextIdentityColumnValue;
            }
            else {
                if (nextIdentityColumnValue < nextIncrement)
                    nextIncrement = nextIdentityColumnValue;
            }
        }
        return dvd;
    }

    @Override
    public void close() throws StandardException{
        super.close();
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
        delimited = in.readBoolean()?in.readUTF():null;
        escaped = in.readBoolean()?in.readUTF():null;
        lines = in.readBoolean()?in.readUTF():null;
        storedAs = in.readBoolean()?in.readUTF():null;
        location = in.readBoolean()?in.readUTF():null;
        compression = in.readBoolean()?in.readUTF():null;
        bulkImportDirectory = in.readBoolean()?in.readUTF():null;
        skipConflictDetection=in.readBoolean();
        skipWAL=in.readBoolean();
        samplingOnly = in.readBoolean();
        outputKeysOnly = in.readBoolean();
        skipSampling = in.readBoolean();
        if (in.readBoolean())
            indexName = in.readUTF();
        partitionByRefItem = in.readInt();
        sampleFraction = in.readDouble();
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
        out.writeBoolean(delimited!=null);
        if (delimited!=null)
            out.writeUTF(delimited);
        out.writeBoolean(escaped!=null);
        if (escaped!=null)
            out.writeUTF(escaped);
        out.writeBoolean(lines!=null);
        if (lines!=null)
            out.writeUTF(lines);
        out.writeBoolean(storedAs!=null);
        if (storedAs!=null)
            out.writeUTF(storedAs);
        out.writeBoolean(location!=null);
        if (location!=null)
            out.writeUTF(location);
        out.writeBoolean(compression!=null);
        if (compression!=null)
            out.writeUTF(compression);
        out.writeBoolean(bulkImportDirectory!=null);
        if (bulkImportDirectory!=null)
            out.writeUTF(bulkImportDirectory);
        out.writeBoolean(skipConflictDetection);
        out.writeBoolean(skipWAL);
        out.writeBoolean(samplingOnly);
        out.writeBoolean(outputKeysOnly);
        out.writeBoolean(skipSampling);
        out.writeBoolean(indexName != null);
        if (indexName != null)
            out.writeUTF(indexName);
        out.writeInt(partitionByRefItem);
        out.writeDouble(sampleFraction);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

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
        Pair<DataSet, int[]> pair = getBatchedDataset(dsp);
        DataSet set = pair.getFirst();
        int[] expectedUpdateCounts = pair.getSecond();
        
        ExecRow execRow=getExecRowDefinition();
        int[] execRowTypeFormatIds=WriteReadUtils.getExecRowTypeFormatIds(execRow);

        operationContext.pushScope();
        if(insertMode.equals(InsertNode.InsertMode.UPSERT) && pkCols==null)
            throw ErrorState.UPSERT_NO_PRIMARY_KEYS.newException(""+heapConglom+"");

        if (dsp.isSparkExplain()) {
            dsp.prependSpliceExplainString(this.explainPlan);
            return set;
        }
        TxnView txn=getTransactionForWrite(dsp);

        ClusterHealth.ClusterHealthWatcher healthWatcher = null;
        if (skipWAL) {
            healthWatcher = SIDriver.driver().clusterHealth().registerWatcher();
        }

        try{
            // initTriggerRowHolders can't be called in the TriggerHandler constructor
            // because it has to be called after getCurrentTransaction() elevates the
            // transaction to writable.
            if (triggerHandler != null)
                triggerHandler.initTriggerRowHolders(isOlapServer(), txn, SpliceClient.token, 0);
            if(statusDirectory!=null)
                dsp.setSchedulerPool("import");
            if (storedAs!=null) {
                ImportUtils.validateWritable(location,false);

                if (storedAs.toLowerCase().equals("p"))
                    return set.writeParquetFile(dsp, partitionBy, location, compression, operationContext);
                else if (storedAs.toLowerCase().equals("a"))
                    return set.writeAvroFile(dsp, partitionBy, location, compression, operationContext);
                else if (storedAs.toLowerCase().equals("o"))
                    return set.writeORCFile(IntArrays.count(execRowTypeFormatIds.length),partitionBy,location, compression, operationContext);
                else if (storedAs.toLowerCase().equals("t"))
                    return set.writeTextFile(location, new CsvOptions(delimited, escaped, lines), operationContext);
                else
                    throw new RuntimeException("storedAs type not supported -> " + storedAs);
            }
            InsertDataSetWriterBuilder writerBuilder = null;
            if (bulkImportDirectory!=null && bulkImportDirectory.compareToIgnoreCase("NULL") !=0) {
                // bulk import
                writerBuilder = set.bulkInsertData(operationContext)
                        .bulkImportDirectory(bulkImportDirectory)
                        .samplingOnly(samplingOnly)
                        .outputKeysOnly(outputKeysOnly)
                        .skipSampling(skipSampling)
                        .indexName(indexName);
            }
            else {
                // regular import
                writerBuilder = set.insertData(operationContext);
            }

            DataSetWriter writer = writerBuilder
                    .autoIncrementRowLocationArray(autoIncrementRowLocationArray)
                    .execRowTypeFormatIds(execRowTypeFormatIds)
                    .sequences(spliceSequences)
                    .isUpsert(insertMode.equals(InsertNode.InsertMode.UPSERT))
                    .sampleFraction(sampleFraction)
                    .pkCols(pkCols)
                    .tableVersion(tableVersion)
                    .updateCounts(expectedUpdateCounts)
                    .destConglomerate(heapConglom)
                    .tempConglomerateID(getTriggerConglomID())
                    .operationContext(operationContext)
                    .txn(txn)
                    .token(SpliceClient.token)
                    .execRowDefinition(getExecRowDefinition())
                    .build();
            return writer.write();

        }
        catch (Exception e) {
            exceptionHit = true;
            throw Exceptions.parseException(e);
        }
        finally{
            if (skipWAL) {
                flushAndCheckErrors(healthWatcher);
            }
            finalizeNestedTransaction();
            operationContext.popScope();
        }
    }

    private void flushAndCheckErrors(ClusterHealth.ClusterHealthWatcher healthWatcher) throws StandardException {
        PartitionFactory tableFactory = SIDriver.driver().getTableFactory();

        try {
            for (long cid : writeInfo.getIndexConglomerateIds()) {
                try (Partition table = tableFactory.getTable(Long.toString(cid))) {
                    table.flush();
                }
            }
            try (Partition table = tableFactory.getTable(Long.toString(writeInfo.getConglomerateId()))) {
                table.flush();
            }
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

    public String getTableVersion() { return tableVersion; }
}
