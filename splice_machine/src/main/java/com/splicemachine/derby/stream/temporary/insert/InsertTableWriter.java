package com.splicemachine.derby.stream.temporary.insert;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceIdentityColumnKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.client.HTableInterface;
import java.util.Iterator;

/**
 * Created by jleach on 5/5/15.
 */
public class InsertTableWriter implements AutoCloseable {
    protected int[] pkCols;
    protected String tableVersion;
    protected HTableInterface sysColumnTable;
    protected ExecRow execRowDefinition;
    protected RowLocation[] autoIncrementRowLocationArray;
    protected Pair<Long,Long>[] defaultAutoIncrementValues;
    protected long heapConglom;
    int[] execRowTypeFormatIds;
    protected static final KVPair.Type dataType = KVPair.Type.INSERT;
    protected TxnView txn;
    protected WriteCoordinator writeCoordinator;
    protected SpliceSequence[] spliceSequences;
    protected int incrementBatch;
    protected RecordingCallBuffer<KVPair> writeBuffer;
    protected byte[] destinationTable;
    protected PairEncoder encoder;
    public int rowsWritten;

    public InsertTableWriter(int[] pkCols, String tableVersion, ExecRow execRowDefinition,
                             RowLocation[] autoIncrementRowLocationArray,Pair<Long,Long>[] defaultAutoIncrementValues,
                             long heapConglom, TxnView txn) {
        this.pkCols = pkCols;
        this.tableVersion = tableVersion;
        this.execRowDefinition = execRowDefinition;
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        this.defaultAutoIncrementValues = defaultAutoIncrementValues;
        this.heapConglom = heapConglom;
        this.txn = txn;
    }


    public void open() throws StandardException {
        destinationTable = Long.toString(heapConglom).getBytes();
        try {
            HTableInterface sysColumnTable = null;
            spliceSequences = new SpliceSequence[autoIncrementRowLocationArray.length];
            for (int i = 0; i < autoIncrementRowLocationArray.length; i++) {
                int columnPosition = i + 1;
                if (i == 0)
                    sysColumnTable = SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES);
                HBaseRowLocation rl = (HBaseRowLocation) autoIncrementRowLocationArray[i];
                byte[] rlBytes = rl.getBytes();
                spliceSequences[0] = SpliceDriver.driver().getSequencePool().get(new SpliceIdentityColumnKey(
                        sysColumnTable, rlBytes,
                        heapConglom, columnPosition, incrementBatch, defaultAutoIncrementValues[i].getFirst(),
                        defaultAutoIncrementValues[i].getSecond()));
            }
            writeCoordinator = SpliceDriver.driver().getTableWriter();
            writeBuffer = writeCoordinator.writeBuffer(destinationTable,
                    txn, Metrics.noOpMetricFactory());
            encoder = new PairEncoder(getKeyEncoder(), getRowHash(), dataType);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
    public void close() throws StandardException {
        try {
            writeBuffer.flushBuffer();
            writeBuffer.close();
            if (sysColumnTable != null)
                sysColumnTable.close();
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    };

    public void write(ExecRow execRow) throws StandardException {
        try {
            KVPair encode = encoder.encode(execRow);
            writeBuffer.add(encode);
            rowsWritten++;
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    public void write(Iterator<ExecRow> execRows) throws StandardException {
        while (execRows.hasNext())
            write(execRows.next());
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

    public DataValueDescriptor increment(int columnPosition, long increment) throws StandardException {
        long nextIncrement = spliceSequences[columnPosition-1].getNext();
        DataValueDescriptor dvd = execRowDefinition.cloneColumn(columnPosition);
        dvd.setValue(nextIncrement);
        return dvd;
    }

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