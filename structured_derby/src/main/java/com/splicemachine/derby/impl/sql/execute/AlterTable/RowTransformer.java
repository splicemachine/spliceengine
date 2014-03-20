package com.splicemachine.derby.impl.sql.execute.AlterTable;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/7/14
 * Time: 11:12 AM
 * To change this template use File | Settings | File Templates.
 */
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.iapi.sql.execute.ExecRow;
import com.splicemachine.hbase.KVPair;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.derby.catalog.UUID;

import java.sql.SQLException;
import java.io.IOException;
public class RowTransformer {

    private UUID tableId;
    private String txnId;
    private boolean initialized = false;
    private ExecRow oldRow;
    private ExecRow newRow;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;
    private EntryDecoder rowDecoder;
    private PairEncoder entryEncoder;
    private int[] oldColumnOrdering;
    private int[] newColumnOrdering;
    private MultiFieldDecoder keyDecoder;
    private KeyMarshaller keyMarshaller;
    private int[] baseColumnMap;
    int[] formatIds;
    DataValueDescriptor[] kdvds;

    public RowTransformer(UUID tableId,
                          String txnId,
                          ColumnInfo[] columnInfos,
                          int droppedColumnPosition) {
        this.tableId = tableId;
        this.txnId = txnId;
        this.columnInfos = columnInfos;
        this.droppedColumnPosition = droppedColumnPosition;
    }

    private Snowflake.Generator getRandomGenerator(){
        return SpliceDriver.driver().getUUIDGenerator().newGenerator(100);
    }

    private void initExecRow() throws StandardException{
        oldRow = new ValueRow(columnInfos.length);
        newRow = new ValueRow(columnInfos.length - 1);

        int i = 1;
        int j =1;
        for (ColumnInfo col:columnInfos){
            DataValueDescriptor dataValue = col.dataType.getNull();
            oldRow.setColumn(i, dataValue);
            if (i++ != droppedColumnPosition){
                newRow.setColumn(j++, dataValue);
            }
        }
    }

    private void initEncoder() {

        // Initialize decoder
        rowDecoder = new EntryDecoder(SpliceDriver.getKryoPool());
        keyDecoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
        keyMarshaller = new KeyMarshaller();

        // initialize encoder
        oldColumnOrdering = DataDictionaryUtils.getColumnOrdering(txnId, tableId);
        newColumnOrdering = DataDictionaryUtils.getColumnOrderingAfterDropColumn(oldColumnOrdering, droppedColumnPosition);

        KeyEncoder encoder;
        if(newColumnOrdering!=null&& newColumnOrdering.length>0){
            encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(newColumnOrdering, null), NoOpPostfix.INSTANCE);
        }else {
            encoder = new KeyEncoder(new SaltedPrefix(getRandomGenerator()),NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
        }
        int[] columns = IntArrays.count(newRow.nColumns());

        if (newColumnOrdering != null && newColumnOrdering.length > 0) {
            for (int col:newColumnOrdering) {
                columns[col] = -1;
            }
        }
        DataHash rowHash = new EntryDataHash(columns, null);

        entryEncoder = new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);
    }
    private void initialize() throws StandardException, SQLException{
        initExecRow();
        initEncoder();
        baseColumnMap = IntArrays.count(oldRow.nColumns());

        if (oldColumnOrdering != null && oldColumnOrdering.length > 0) {
            formatIds = DataDictionaryUtils.getFormatIds(txnId, tableId);
            kdvds = new DataValueDescriptor[oldColumnOrdering.length];
            for (int i = 0; i < oldColumnOrdering.length; ++i) {
                kdvds[i] = LazyDataValueFactory.getLazyNull(formatIds[oldColumnOrdering[i]]);
            }
        }
        initialized = true;
    }

    public KVPair transform(KeyValue kv) throws StandardException, SQLException, IOException{
        if (!initialized) {
            initialize();
        }

        // Decode a row
        oldRow.resetRowArray();
        DataValueDescriptor[] oldFields = oldRow.getRowArray();
        if (oldFields.length != 0) {
            if(oldColumnOrdering != null && oldColumnOrdering.length > 0) {
                // decode the old primary key columns
                keyMarshaller.decode(kv, oldFields, baseColumnMap, keyDecoder, oldColumnOrdering, kdvds);
            }
            RowMarshaller.sparsePacked().decode(kv, oldFields, baseColumnMap, rowDecoder);
        }

        // encode the result
        KVPair newPair = entryEncoder.encode(newRow);

        // preserve the old row key
        newPair.setKey(kv.getRow());
        return newPair;
    }

}
