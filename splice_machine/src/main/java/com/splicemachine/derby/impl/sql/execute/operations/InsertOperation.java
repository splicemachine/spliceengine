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

package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.utils.IntArrays;
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
                           double optimizerEstimatedRowCount,
                           double optimizerEstimatedCost,
                           String tableVersion,
                           String delimited,
                           String escaped,
                           String lines,
                           String storedAs,
                           String location,
                           String compression,
                           int partitionByRefItem) throws StandardException{
        super(source,generationClauses,checkGM,source.getActivation(),optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
        this.insertMode=InsertNode.InsertMode.valueOf(insertMode);
        this.statusDirectory=statusDirectory;
        this.failBadRecordCount = (failBadRecordCount >= 0 ? failBadRecordCount : -1);
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
        this.storedAs = storedAs;
        this.location = location;
        this.compression = compression;
        this.partitionByRefItem = partitionByRefItem;
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
        delimited = in.readBoolean()?in.readUTF():null;
        escaped = in.readBoolean()?in.readUTF():null;
        lines = in.readBoolean()?in.readUTF():null;
        storedAs = in.readBoolean()?in.readUTF():null;
        location = in.readBoolean()?in.readUTF():null;
        compression = in.readBoolean()?in.readUTF():null;
        this.partitionByRefItem = in.readInt();
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
        out.writeInt(partitionByRefItem);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        if(statusDirectory != null) {
            // if we have a status directory, we're an import and so permissive
            dsp.setPermissive(statusDirectory, getVTIFileName(), failBadRecordCount);
        }
        DataSet set=source.getDataSet(dsp);
        OperationContext operationContext=dsp.createOperationContext(this);
        ExecRow execRow=getExecRowDefinition();
        int[] execRowTypeFormatIds=WriteReadUtils.getExecRowTypeFormatIds(execRow);
        if(insertMode.equals(InsertNode.InsertMode.UPSERT) && pkCols==null)
            throw ErrorState.UPSERT_NO_PRIMARY_KEYS.newException(""+heapConglom+"");
        TxnView txn=getCurrentTransaction();

        operationContext.pushScope();
        try{
            if(statusDirectory!=null)
                dsp.setSchedulerPool("import");
            if (storedAs!=null) {
                ImportUtils.validateWritable(location,false);

                if (storedAs.toLowerCase().equals("p"))
                    return set.writeParquetFile(IntArrays.count(execRowTypeFormatIds.length),partitionBy,location, compression, operationContext);
                if (storedAs.toLowerCase().equals("o"))
                    return set.writeORCFile(IntArrays.count(execRowTypeFormatIds.length),partitionBy,location, compression, operationContext);
                if (storedAs.toLowerCase().equals("t"))
                    return set.writeTextFile(this,location,delimited,lines,IntArrays.count(execRowTypeFormatIds.length), operationContext);
                new RuntimeException("storedAs type not supported -> " + storedAs);
            }




            PairDataSet dataSet=set.index(new InsertPairFunction(operationContext),true);
            DataSetWriter writer=dataSet.insertData(operationContext)
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
        }finally{
            operationContext.popScope();
        }

    }

    @Override
    public String getVTIFileName(){
        return getSubOperations().get(0).getVTIFileName();
    }


    @Override
    public void openCore() throws StandardException{

        DataSetProcessor dsp = EngineDriver.driver().processorFactory().chooseProcessor(activation,this);
        if (statusDirectory != null || dsp.getType() == DataSetProcessor.Type.SPARK) {
            remoteQueryClient = EngineDriver.driver().processorFactory().getRemoteQueryClient(this);
            remoteQueryClient.submit();
            locatedRowIterator = remoteQueryClient.getIterator();
        } else {
            openCore(dsp);
        }
    }

}
