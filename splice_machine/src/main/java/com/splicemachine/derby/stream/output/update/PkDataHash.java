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

package com.splicemachine.derby.stream.output.update;

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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
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
                try{serializer.close();}catch(IOException ignored){}
            }
        }
    }

    @Override
    public KeyHashDecoder getDecoder() {
        return NoOpKeyHashDecoder.INSTANCE;
    }
}