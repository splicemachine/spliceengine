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
import com.splicemachine.pipeline.exception.SpliceDoNotRetryIOException;
import com.splicemachine.pipeline.writehandler.altertable.AlterTableInterceptWriteHandler;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.UUIDGenerator;

/**
 * Drop column.
 */
public class TentativeDropColumnDesc implements TransformingDDLDescriptor, Externalizable{
    private long conglomerateNumber;
    private long baseConglomerateNumber;
    private String tableVersion;
    private int[] srcColumnOrdering;
    private int[] targetColumnOrdering;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;

    public TentativeDropColumnDesc() {}

    public TentativeDropColumnDesc(long baseConglomerateNumber,
                                   long conglomerateNumber,
                                   String tableVersion,
                                   int[] srcColumnOrdering,
                                   int[] targetColumnOrdering,
                                   ColumnInfo[] columnInfos,
                                   int droppedColumnPosition) {
        this.conglomerateNumber = conglomerateNumber;
        this.baseConglomerateNumber = baseConglomerateNumber;
        this.tableVersion = tableVersion;
        this.srcColumnOrdering = srcColumnOrdering;
        this.targetColumnOrdering = targetColumnOrdering;
        this.columnInfos = columnInfos;
        this.droppedColumnPosition = droppedColumnPosition;

    }

    @Override
    public long getBaseConglomerateNumber() {
        return baseConglomerateNumber;
    }

    @Override
    public long getConglomerateNumber() {
        return conglomerateNumber;
    }

    @Override
    public RowTransformer createRowTransformer() throws IOException {
        return create(tableVersion, srcColumnOrdering, targetColumnOrdering, columnInfos, droppedColumnPosition);
    }

    @Override
    public WriteHandler createWriteHandler(RowTransformer transformer) throws IOException {
        return new AlterTableInterceptWriteHandler(transformer, Bytes.toBytes(String.valueOf(conglomerateNumber)));
    }

    @Override
    public int[] getKeyColumnPositions() {
        return srcColumnOrdering;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomerateNumber);
        out.writeLong(baseConglomerateNumber);
        out.writeInt(droppedColumnPosition);
        out.writeObject(tableVersion);
        ArrayUtil.writeIntArray(out, srcColumnOrdering);
        ArrayUtil.writeIntArray(out, targetColumnOrdering);
        int size = columnInfos.length;
        out.writeInt(size);
        for (ColumnInfo col:columnInfos) {
            out.writeObject(col);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        conglomerateNumber = in.readLong();
        baseConglomerateNumber = in.readLong();
        droppedColumnPosition = in.readInt();
        tableVersion = (String) in.readObject();
        srcColumnOrdering = ArrayUtil.readIntArray(in);
        targetColumnOrdering = ArrayUtil.readIntArray(in);
        int size = in.readInt();
        columnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            columnInfos[i] = (ColumnInfo)in.readObject();
        }
    }

    private static RowTransformer create(String tableVersion,
                                        int[] oldColumnOrdering,
                                        int[] newColumnOrdering,
                                        ColumnInfo[] columnInfos,
                                        int droppedColumnPosition) throws SpliceDoNotRetryIOException {
        // template rows
        ExecRow oldRow = new ValueRow(columnInfos.length);
        ExecRow newRow = new ValueRow(columnInfos.length-1);

        try {
            int i = 1;
            int j =1;
            for (ColumnInfo col:columnInfos){
                DataValueDescriptor dataValue = col.dataType.getNull();
                oldRow.setColumn(i, dataValue);
                if (i++ != droppedColumnPosition){
                    newRow.setColumn(j++, dataValue);
                }
            }
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }

        // Key decoder
        KeyHashDecoder keyDecoder;
        if(oldColumnOrdering!=null && oldColumnOrdering.length>0){
            //must use dense encodings in the key if there's a column ordering
            DescriptorSerializer[] oldDenseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(oldRow);
            keyDecoder = BareKeyHash.decoder(oldColumnOrdering, null, oldDenseSerializers);
        }else{
            keyDecoder = NoOpDataHash.instance().getDecoder();
        }

        // Value decoder
        DescriptorSerializer[] oldSerializers =
            VersionedSerializers.forVersion(tableVersion, true).getSerializers(oldRow);
        EntryDataDecoder valueDecoder = new EntryDataDecoder(IntArrays.count(oldRow.nColumns()),null,oldSerializers);

        // Key encoder
        KeyEncoder keyEncoder;
        if(newColumnOrdering !=null && newColumnOrdering.length>0){
            //must use dense encodings in the key if there's a column ordering
            DescriptorSerializer[] denseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(newRow);
            keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(newColumnOrdering, null,
                                                                              denseSerializers), NoOpPostfix.INSTANCE);
        } else {
            UUIDGenerator uuidGenerator = SpliceDriver.driver().getUUIDGenerator().newGenerator(100);
            keyEncoder = new KeyEncoder(new SaltedPrefix(uuidGenerator), NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
        }

        // Row Encoder
        int[] columns = IntArrays.count(newRow.nColumns());
        if (newColumnOrdering != null && newColumnOrdering.length > 0) {
            for (int col: newColumnOrdering) {
                columns[col] = -1;
            }
        }
        DescriptorSerializer[] newSerializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(newRow);
        DataHash rowEncoder = new EntryDataHash(columns, null,newSerializers);
        PairEncoder pairEncoder = new PairEncoder(keyEncoder,rowEncoder, KVPair.Type.INSERT);

        return new AlterTableRowTransformer(oldRow, newRow, keyDecoder, valueDecoder, pairEncoder);
    }
}
