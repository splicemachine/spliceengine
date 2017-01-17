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

package com.splicemachine.derby.impl.sql.execute.altertable;

import java.io.IOException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.pipeline.RowTransformer;
import com.splicemachine.storage.Record;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.ws.rs.NotSupportedException;

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
    private final int[] columnMapping;
    private final int copyLen;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public AlterTableRowTransformer(ExecRow srcRow,
                                    int[] columnMapping,
                                    ExecRow templateRow
) {
        this.srcRow = srcRow;
        this.columnMapping = columnMapping;
        this.templateRow = templateRow;
        // array copy must use the smaller of the two lengths -
        // for drop column, templateRow will be shorter.
        // for add column, srcRow will be shorter.
        this.copyLen = Math.min(srcRow.nColumns(), templateRow.nColumns());
    }

    @Override
    public Record transform(ExecRow row) throws StandardException, IOException {
        throw new UnsupportedOperationException("Not Implemented yet");

        /*
        ExecRow mergedRow = templateRow.getClone();

        for (int i = 0; i < columnMapping.length; i++) {
            int targetIndex = columnMapping[i];
            if (targetIndex != 0) {
                mergedRow.setColumn(targetIndex, row.cloneColumn(i+1));
            }
        }
        // encode and return the result
        return entryEncoder.encode(mergedRow);
        */
    }

    public Record transform(Record record) throws StandardException, IOException {
        throw new NotSupportedException("Not Implemented Yet");
        /*
        // Decode a row
        ExecRow mergedRow = templateRow.getClone();
        srcRow.resetRowArray();
        decodeRow(kvPair, srcRow, keyDecoder, rowDecoder);

        DataValueDescriptor[] srcArray = srcRow.getRowArray();
        DataValueDescriptor[] mergedArray = mergedRow.getRowArray();
        System.arraycopy(srcArray, 0, mergedArray, 0, copyLen);

        // encode and return the result
        return entryEncoder.encode(mergedRow);
        */
    }

    @Override
    public void close() throws IOException{

    }
}
