/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.altertable;

import java.io.IOException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.RowTransformer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Used by alter table write interceptors to map rows written in src table to new
 * target table.
 * <p/>
 * This class is driven by its exec row definitions and its encoder/decoder.<br/>
 * These are created for a specific alter table action in TransformingDDLDescriptors
 * specializations.
 */
public class AlterTableRowTransformer implements RowTransformer{
    private final ExecRow srcRow;
    private final ExecRow templateRow;
    private final EntryDataDecoder rowDecoder;
    private final KeyHashDecoder keyDecoder;
    private final PairEncoder entryEncoder;
    private final int[] columnMapping;
    private final int copyLen;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public AlterTableRowTransformer(ExecRow srcRow,
                                    int[] columnMapping,
                                    ExecRow templateRow,
                                    KeyHashDecoder keyDecoder,
                                    EntryDataDecoder rowDecoder,
                                    PairEncoder entryEncoder) {
        this.srcRow = srcRow;
        this.columnMapping = columnMapping;
        this.templateRow = templateRow;
        this.rowDecoder = rowDecoder;
        this.keyDecoder = keyDecoder;
        this.entryEncoder = entryEncoder;
        // array copy must use the smaller of the two lengths -
        // for drop column, templateRow will be shorter.
        // for add column, srcRow will be shorter.
        this.copyLen = Math.min(srcRow.nColumns(), templateRow.nColumns());
    }

    @Override
    public KVPair transform(ExecRow row) throws StandardException, IOException {
        ExecRow mergedRow = templateRow.getClone();

        for (int i = 0; i < columnMapping.length; i++) {
            int targetIndex = columnMapping[i];
            if (targetIndex != 0) {
                mergedRow.setColumn(targetIndex, row.cloneColumn(i+1));
            }
        }
        // encode and return the result
        return entryEncoder.encode(mergedRow);
    }

    public KVPair transform(KVPair kvPair) throws StandardException, IOException {
        // Decode a row
        ExecRow mergedRow = templateRow.getClone();
        srcRow.resetRowArray();
        decodeRow(kvPair, srcRow, keyDecoder, rowDecoder);

        DataValueDescriptor[] srcArray = srcRow.getRowArray();
        DataValueDescriptor[] mergedArray = mergedRow.getRowArray();
        System.arraycopy(srcArray, 0, mergedArray, 0, copyLen);

        // encode and return the result
        return entryEncoder.encode(mergedRow);
    }

    private static void decodeRow(KVPair kvPair, ExecRow srcRow, KeyHashDecoder keyDecoder, EntryDataDecoder
        rowDecoder) throws StandardException {
        if (srcRow.nColumns() > 0) {
            keyDecoder.set(kvPair.getRowKey(), 0, kvPair.getRowKey().length);
            keyDecoder.decode(srcRow);

            rowDecoder.set(kvPair.getValue(), 0, kvPair.getValue().length);
            rowDecoder.decode(srcRow);
        }
    }

    @Override
    public void close() throws IOException{
        keyDecoder.close();
        rowDecoder.close();
        entryEncoder.close();
    }
}
