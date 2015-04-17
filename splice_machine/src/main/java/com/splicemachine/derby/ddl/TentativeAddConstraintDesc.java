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
 * Add constraint descriptor
 */
public class TentativeAddConstraintDesc implements TransformingDDLDescriptor, Externalizable{
    private String tableVersion;
    private long newConglomId;
    private long oldConglomId;
    private int[] srcColumnOrdering;
    private int[] targetColumnOrdering;
    private ColumnInfo[] columnInfos;

    public TentativeAddConstraintDesc() {}

    public TentativeAddConstraintDesc(String tableVersion,
                                      long newConglomId,
                                      long oldConglomId,
                                      int[] srcColumnOrdering,
                                      int[] targetColumnOrdering,
                                      ColumnInfo[] columnInfos) {
        this.tableVersion = tableVersion;
        this.newConglomId = newConglomId;
        this.oldConglomId = oldConglomId;
        this.srcColumnOrdering = srcColumnOrdering;
        this.targetColumnOrdering = targetColumnOrdering;
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
        return create(tableVersion, srcColumnOrdering, targetColumnOrdering, columnInfos);
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
        ArrayUtil.writeIntArray(out, srcColumnOrdering);
        ArrayUtil.writeIntArray(out, targetColumnOrdering);
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
        srcColumnOrdering = ArrayUtil.readIntArray(in);
        targetColumnOrdering = ArrayUtil.readIntArray(in);
        int size = in.readInt();
        columnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            columnInfos[i] = (ColumnInfo)in.readObject();
        }
    }

    private static RowTransformer create(String tableVersion,
                                         int[] sourceKeyOrdering,
                                         int[] targetKeyOrdering,
                                         ColumnInfo[] columnInfos)
        throws IOException {
        // template rows : 1-1 when not dropping/adding columns
        ExecRow srcRow = new ValueRow(columnInfos.length);
        ExecRow targetRow = new ValueRow(columnInfos.length);

        // Set the types on each column
        try {
            for (int i=0; i<columnInfos.length; i++) {
                DataValueDescriptor dvd = columnInfos[i].dataType.getNull();
                srcRow.setColumn(i + 1, dvd);
                targetRow.setColumn(i + 1, dvd);
            }
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }

        // Key decoder
        KeyHashDecoder keyDecoder;
        if(sourceKeyOrdering!=null && sourceKeyOrdering.length>0){
            // We'll need src table key column order when we have keys (PK, unique) on src table
            // Must use dense encodings in the key serializer (sparse = false)
            DescriptorSerializer[] denseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(srcRow);
            keyDecoder = BareKeyHash.decoder(sourceKeyOrdering, null, denseSerializers);
        }else{
            // Just use the no-op key decoder for old rows when no key in src table
            keyDecoder = NoOpDataHash.instance().getDecoder();
        }

        // Row decoder
        DescriptorSerializer[] oldSerializers =
            VersionedSerializers.forVersion(tableVersion, true).getSerializers(srcRow);
        EntryDataDecoder rowDecoder = new EntryDataDecoder(IntArrays.count(srcRow.nColumns()),null,oldSerializers);

        // Row encoder
        KeyEncoder encoder;
        DescriptorSerializer[] newSerializers =
            VersionedSerializers.forVersion(tableVersion, true).getSerializers(targetRow);
        if(targetKeyOrdering !=null&& targetKeyOrdering.length>0){
            // We'll need target table key column order when we have keys (PK, unique) on target table
            // Must use dense encodings in the key serializer (sparse = false)
            DescriptorSerializer[] denseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(targetRow);
            encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(targetKeyOrdering, null,
                                                                              denseSerializers), NoOpPostfix.INSTANCE);
        } else {
            // Just use the no-op key decoder for new rows when no key in target table
            UUIDGenerator uuidGenerator = SpliceDriver.driver().getUUIDGenerator().newGenerator(100);
            encoder = new KeyEncoder(new SaltedPrefix(uuidGenerator), NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
        }

        // Column ordering mask for all columns. Keys will be masked from the value row.
        int[] columnOrdering = IntArrays.count(targetRow.nColumns());
        if (targetKeyOrdering != null && targetKeyOrdering.length > 0) {
            for (int col: targetKeyOrdering) {
                columnOrdering[col] = -1;
            }
        }
        DataHash rowHash = new EntryDataHash(columnOrdering, null,newSerializers);
        PairEncoder rowEncoder = new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);

        // Create and return the row transformer
        return new AlterTableRowTransformer(srcRow, targetRow, keyDecoder, rowDecoder, rowEncoder);
    }

}
