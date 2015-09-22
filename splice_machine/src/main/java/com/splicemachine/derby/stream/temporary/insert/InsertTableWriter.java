package com.splicemachine.derby.stream.temporary.insert;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.temporary.AbstractTableWriter;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.IntArrays;
import java.util.Iterator;

/**
 * Created by jleach on 5/5/15.
 */
public class InsertTableWriter extends AbstractTableWriter {
    protected int[] pkCols;
    protected String tableVersion;
    protected ExecRow execRowDefinition;
    protected RowLocation[] autoIncrementRowLocationArray;
    protected static final KVPair.Type dataType = KVPair.Type.INSERT;
    protected WriteCoordinator writeCoordinator;
    protected SpliceSequence[] spliceSequences;
    protected RecordingCallBuffer<KVPair> writeBuffer;
    protected PairEncoder encoder;
    public int rowsWritten;
    protected InsertOperation insertOperation;

    public InsertTableWriter(int[] pkCols, String tableVersion, ExecRow execRowDefinition,
                             RowLocation[] autoIncrementRowLocationArray,SpliceSequence[] spliceSequences,
                             long heapConglom, TxnView txn, InsertOperation insertOperation) {
        super(txn,heapConglom);
        this.pkCols = pkCols;
        this.tableVersion = tableVersion;
        this.execRowDefinition = execRowDefinition;
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        this.spliceSequences = spliceSequences;
        this.insertOperation = insertOperation;
        this.destinationTable = Long.toString(heapConglom).getBytes();
    }

    public void open() throws StandardException {
          open(null,null);
    }

    public void open(TriggerHandler triggerHandler, SpliceOperation operation) throws StandardException {
        super.open(triggerHandler, operation);
        try {
            writeBuffer = writeCoordinator.writeBuffer(destinationTable,
                    txn, Metrics.noOpMetricFactory());
            encoder = new PairEncoder(getKeyEncoder(), getRowHash(), dataType);
            if (insertOperation != null)
                insertOperation.tableWriter = this;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
    public void close() throws StandardException {
        try {
            writeBuffer.flushBuffer();
            writeBuffer.close();
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    };

    public void insert(ExecRow execRow) throws StandardException {
        try {
            beforeRow(execRow);
            KVPair encode = encoder.encode(execRow);
            writeBuffer.add(encode);
            rowsWritten++;
            TriggerHandler.fireAfterRowTriggers(triggerHandler, execRow, flushCallback);
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    public void insert(Iterator<ExecRow> execRows) throws StandardException {
        while (execRows.hasNext())
            insert(execRows.next());
    }

    public void write(ExecRow execRow) throws StandardException {
        insert(execRow);
    }

    public void write(Iterator<ExecRow> execRows) throws StandardException {
        insert(execRows);
    }


    public KeyEncoder getKeyEncoder() throws StandardException {
        HashPrefix prefix;
        DataHash dataHash;
        KeyPostfix postfix = NoOpPostfix.INSTANCE;
        if(pkCols==null){
            prefix = new SaltedPrefix(SpliceDriver.driver().getUUIDGenerator().newGenerator(100));
            dataHash = NoOpDataHash.INSTANCE;
        }else{
            int[] keyColumns = new int[pkCols.length];
            for(int i=0;i<keyColumns.length;i++){
                keyColumns[i] = pkCols[i] -1;
            }
            prefix = NoOpPrefix.INSTANCE;
            DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(execRowDefinition);
            dataHash = BareKeyHash.encoder(keyColumns,null, SpliceDriver.driver().getKryoPool(),serializers);
        }
        return new KeyEncoder(prefix,dataHash,postfix);
    }
/* Hive Auto Increment Writing
    public DataValueDescriptor increment(int columnPosition, long increment) throws StandardException {
        long nextIncrement = spliceSequences[columnPosition-1].getNext();
        DataValueDescriptor dvd = execRowDefinition.cloneColumn(columnPosition);
        dvd.setValue(nextIncrement);
        return dvd;
    }
*/
    public DataHash getRowHash() throws StandardException {
        //get all columns that are being set
        int[] columns = getEncodingColumns(execRowDefinition.nColumns(),pkCols);
        DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(execRowDefinition);
        return new EntryDataHash(columns,null,serializers);
    }

    public static int[] getEncodingColumns(int n, int[] pkCols) {
        int[] columns = IntArrays.count(n);
        // Skip primary key columns to save space
        if (pkCols != null) {
            for(int pkCol:pkCols) {
                columns[pkCol-1] = -1;
            }
        }
        return columns;
    }

}