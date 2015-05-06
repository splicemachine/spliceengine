package com.splicemachine.derby.stream.temporary.update;

import com.google.common.io.Closeables;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.NoOpKeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

import java.io.IOException;

/**
 * Created by jleach on 5/5/15.
 */
public class PkDataHash implements DataHash<ExecRow> {
    private final String tableVersion;
    private ExecRow currentRow;
    private byte[] rowKey;
    private int[] keyColumns;
    private MultiFieldEncoder encoder;
    private MultiFieldDecoder decoder;
    private DataValueDescriptor[] kdvds;
    private DescriptorSerializer[] serializers;

    public PkDataHash(int[] keyColumns, DataValueDescriptor[] kdvds,String tableVersion) {
        this.keyColumns = keyColumns;
        this.kdvds = kdvds;
        this.tableVersion = tableVersion;
    }

    @Override
    public void setRow(ExecRow rowToEncode) {
        this.currentRow = rowToEncode;
    }

    @Override
    public byte[] encode() throws StandardException, IOException {
        rowKey = ((RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
        if (encoder == null)
            encoder = MultiFieldEncoder.create(keyColumns.length);
        if (decoder == null)
            decoder = MultiFieldDecoder.create();
        pack();
        return encoder.build();
    }

    private void pack() throws StandardException {
        encoder.reset();
        decoder.set(rowKey);
        if(serializers==null)
            serializers = VersionedSerializers.forVersion(tableVersion, false).getSerializers(currentRow);
        int i = 0;
        for (int col:keyColumns) {
            if (col > 0) {
                DataValueDescriptor dvd = currentRow.getRowArray()[col];
                DescriptorSerializer serializer = serializers[col];
                serializer.encode(encoder,dvd,false);
                DerbyBytesUtil.skip(decoder, kdvds[i++]);
            } else {
                int offset = decoder.offset();
                DerbyBytesUtil.skip(decoder,kdvds[i++]);
                int limit = decoder.offset()-1-offset;
                encoder.setRawBytes(decoder.array(),offset,limit);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if(serializers!=null){
            for(DescriptorSerializer serializer:serializers){
                Closeables.closeQuietly(serializer);
            }
        }
    }

    @Override
    public KeyHashDecoder getDecoder() {
        return NoOpKeyHashDecoder.INSTANCE;
    }
}