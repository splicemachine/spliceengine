package com.splicemachine.derby.ddl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.altertable.AlterTableRowTransformer;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.NoOpDataHash;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.derby.utils.marshall.SaltedPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.ddl.TransformingDDLDescriptor;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.writehandler.altertable.AlterTableInterceptWriteHandler;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.UUIDGenerator;

/**
 * Add column descriptor
 */
public class TentativeAddColumnDesc implements TransformingDDLDescriptor, Externalizable{
    private String tableVersion;
    private long newConglomId;
    private long oldConglomId;
    private int[] columnOrdering;
    private ColumnInfo[] columnInfos;

    public TentativeAddColumnDesc() {}

    public TentativeAddColumnDesc(String tableVersion,
                                  long newConglomId,
                                  long oldConglomId,
                                  int[] columnOrdering,
                                  ColumnInfo[] columnInfos) {
        this.tableVersion = tableVersion;
        this.newConglomId = newConglomId;
        this.oldConglomId = oldConglomId;
        this.columnOrdering = columnOrdering;
        this.columnInfos = columnInfos;
    }

    @Override
    public long getBaseConglomerateNumber() {
        return oldConglomId;
    }

    @Override
    public long getConglomerateNumber() {
        return newConglomId;
    }

    @Override
    public RowTransformer createRowTransformer() throws IOException {
        return create(tableVersion, columnOrdering, columnInfos);
    }

    @Override
    public WriteHandler createWriteHandler(RowTransformer transformer) throws IOException {
        return new AlterTableInterceptWriteHandler(transformer, Bytes.toBytes(String.valueOf(newConglomId)));

    }

    public ColumnInfo[] getColumnInfos() {
        return columnInfos;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tableVersion);
        out.writeLong(newConglomId);
        out.writeLong(oldConglomId);
        ArrayUtil.writeIntArray(out, columnOrdering);
        out.writeInt(columnInfos.length);
        for (ColumnInfo col:columnInfos) {
            out.writeObject(col);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tableVersion = (String) in.readObject();
        newConglomId = in.readLong();
        oldConglomId = in.readLong();
        columnOrdering = ArrayUtil.readIntArray(in);
        int size = in.readInt();
        columnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            columnInfos[i] = (ColumnInfo)in.readObject();
        }
    }

    private static RowTransformer create(String tableVersion, int[] columnOrdering, ColumnInfo[] columnInfos)
        throws IOException {
        // template rows
        ExecRow oldRow = new ValueRow(columnInfos.length-1);
        ExecRow newRow = new ValueRow(columnInfos.length);

        try {
            for (int i=0; i<columnInfos.length-1; i++) {
                DataValueDescriptor dvd = columnInfos[i].dataType.getNull();
                oldRow.setColumn(i+1,dvd);
                newRow.setColumn(i+1,dvd);
            }
            // set default value if given
            DataValueDescriptor newColDefaultValue = columnInfos[columnInfos.length-1].defaultValue;
            newRow.setColumn(columnInfos.length,
                             (newColDefaultValue != null ?
                                 newColDefaultValue :
                                 columnInfos[columnInfos.length-1].dataType.getNull()));
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }

        // key decoder
        KeyHashDecoder keyDecoder;
        if(columnOrdering!=null && columnOrdering.length>0){
            DescriptorSerializer[] oldDenseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(oldRow);
            keyDecoder = BareKeyHash.decoder(columnOrdering, null, oldDenseSerializers);
        }else{
            keyDecoder = NoOpDataHash.instance().getDecoder();
        }

        // row decoder
        DescriptorSerializer[] oldSerializers =
            VersionedSerializers.forVersion(tableVersion, true).getSerializers(oldRow);
        EntryDataDecoder rowDecoder = new EntryDataDecoder(IntArrays.count(oldRow.nColumns()),null,oldSerializers);

        // Row encoder
        KeyEncoder encoder;
        DescriptorSerializer[] newSerializers =
            VersionedSerializers.forVersion(tableVersion, true).getSerializers(newRow);
        if(columnOrdering !=null&& columnOrdering.length>0){
            //must use dense encodings in the key
            DescriptorSerializer[] denseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(newRow);
            encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(columnOrdering, null,
                                                                              denseSerializers), NoOpPostfix.INSTANCE);
        } else {
            UUIDGenerator uuidGenerator = SpliceDriver.driver().getUUIDGenerator().newGenerator(100);
            encoder = new KeyEncoder(new SaltedPrefix(uuidGenerator), NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
        }
        int[] columns = IntArrays.count(newRow.nColumns());

        if (columnOrdering != null && columnOrdering.length > 0) {
            for (int col: columnOrdering) {
                columns[col] = -1;
            }
        }
        DataHash rowHash = new EntryDataHash(columns, null,newSerializers);
        PairEncoder rowEncoder = new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);

        return new AlterTableRowTransformer(oldRow, newRow, keyDecoder, rowDecoder, rowEncoder);
    }

}
