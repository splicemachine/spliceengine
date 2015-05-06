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
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.IntArrays;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Created by jleach on 5/5/15.
 */
public class InsertTableWriter {
    protected int[] pkCols;
    protected String tableVersion;
    protected HTableInterface sysColumnTable;
    protected boolean singleRow;
    protected ExecRow execRowDefinition;
    protected RowLocation[] autoIncrementRowLocationArray;
    protected long heapConglom;
    protected static final KVPair.Type dataType = KVPair.Type.INSERT;
    protected TxnView txn;
    WriteCoordinator writeCoordinator;
    public InsertTableWriter() {
        writeCoordinator =  SpliceDriver.driver().getTableWriter();
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
        int index = columnPosition-1;

        HBaseRowLocation rl = (HBaseRowLocation) autoIncrementRowLocationArray[index];

        byte[] rlBytes = rl.getBytes();

        if(sysColumnTable==null){
            sysColumnTable = SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES);
        }

        SpliceSequence sequence;
        long nextIncrement;
        try {
            if (singleRow) { // Single Sequence Move
                sequence = SpliceDriver.driver().getSequencePool().get(new SpliceIdentityColumnKey(sysColumnTable,rlBytes,
                        heapConglom,columnPosition,activation.getLanguageConnectionContext().getDataDictionary(),1l));
                nextIncrement = sequence.getNext();
// Do we need to do this?
//                this.getActivation().getLanguageConnectionContext().setIdentityValue(nextIncrement);
            } else {
                sequence = SpliceDriver.driver().getSequencePool().get(new SpliceIdentityColumnKey(sysColumnTable,rlBytes,
                        heapConglom,columnPosition,activation.getLanguageConnectionContext().getDataDictionary(),SpliceConstants.sequenceBlockSize));
                nextIncrement = sequence.getNext();
            }
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
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
