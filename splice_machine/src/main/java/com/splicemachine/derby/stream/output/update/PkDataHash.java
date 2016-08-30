/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.output.update;

import org.spark_project.guava.io.Closeables;
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