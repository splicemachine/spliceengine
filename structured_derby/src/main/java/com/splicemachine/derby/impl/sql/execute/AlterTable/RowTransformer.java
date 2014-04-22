package com.splicemachine.derby.impl.sql.execute.AlterTable;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/7/14
 * Time: 11:12 AM
 * To change this template use File | Settings | File Templates.
 */

import com.google.common.io.Closeables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
public class RowTransformer implements Closeable {

    private UUID tableId;
    private String txnId;
    private boolean initialized = false;
    private ExecRow oldRow;
    private ExecRow newRow;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;
    private EntryDataDecoder rowDecoder;
		private KeyHashDecoder keyDecoder;
    private PairEncoder entryEncoder;
    private int[] oldColumnOrdering;
    private int[] newColumnOrdering;
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

    private void initEncoder() throws StandardException {
				String tableVersion = DataDictionaryUtils.getTableVersion(txnId,tableId);
				DescriptorSerializer[] oldSerializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(oldRow);

        // Initialize decoder
				rowDecoder = new EntryDataDecoder(baseColumnMap,null,oldSerializers);
				if(oldColumnOrdering!=null && oldColumnOrdering.length>0){
						DescriptorSerializer[] oldDenseSerializers = VersionedSerializers.forVersion(tableVersion,false).getSerializers(oldRow);
						keyDecoder = BareKeyHash.decoder(oldColumnOrdering,null,oldDenseSerializers);
				}else{
						keyDecoder = NoOpDataHash.instance().getDecoder();
				}
//        keyMarshaller = new KeyMarshaller();

        // initialize encoder
        oldColumnOrdering = DataDictionaryUtils.getColumnOrdering(txnId, tableId);
        newColumnOrdering = DataDictionaryUtils.getColumnOrderingAfterDropColumn(oldColumnOrdering, droppedColumnPosition);

        KeyEncoder encoder;
				DescriptorSerializer[] newSerializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(newRow);
        if(newColumnOrdering!=null&& newColumnOrdering.length>0){
						//must use dense encodings in the key
						DescriptorSerializer[] denseSerializers = VersionedSerializers.forVersion(tableVersion, false).getSerializers(newRow);
						encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(newColumnOrdering, null, denseSerializers), NoOpPostfix.INSTANCE);
        }else {
            encoder = new KeyEncoder(new SaltedPrefix(getRandomGenerator()),NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
        }
        int[] columns = IntArrays.count(newRow.nColumns());

        if (newColumnOrdering != null && newColumnOrdering.length > 0) {
            for (int col:newColumnOrdering) {
                columns[col] = -1;
            }
        }
        DataHash rowHash = new EntryDataHash(columns, null,newSerializers);

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

		public KVPair transform(KVPair kvPair) throws StandardException,SQLException,IOException{
				if (!initialized) {
						initialize();
				}

				// Decode a row
				oldRow.resetRowArray();
				DataValueDescriptor[] oldFields = oldRow.getRowArray();
				if (oldFields.length != 0) {
						keyDecoder.set(kvPair.getRow(), 0, kvPair.getRow().length);
						keyDecoder.decode(oldRow);

						rowDecoder.set(kvPair.getValue(),0,kvPair.getValue().length);
						rowDecoder.decode(oldRow);
				}

				// encode the result
				KVPair newPair = entryEncoder.encode(newRow);

				// preserve the old row key
				newPair.setKey(kvPair.getRow());
				return newPair;
		}

    public KVPair transform(Cell kv) throws StandardException, SQLException, IOException{
        if (!initialized) {
            initialize();
        }

        // Decode a row
        oldRow.resetRowArray();
        DataValueDescriptor[] oldFields = oldRow.getRowArray();
        if (oldFields.length != 0) {
						keyDecoder.set(kv.getRowArray(),kv.getRowOffset(),kv.getRowLength());
						keyDecoder.decode(oldRow);

						rowDecoder.set(kv.getValueArray(),kv.getValueOffset(),kv.getValueLength());
						rowDecoder.decode(oldRow);
        }

        // encode the result
        KVPair newPair = entryEncoder.encode(newRow);

        // preserve the old row key
        newPair.setKey(kv.getRowArray());
        return newPair;
    }

		public void close() {
				Closeables.closeQuietly(keyDecoder);
				Closeables.closeQuietly(rowDecoder);
				Closeables.closeQuietly(entryEncoder);
		}

}
