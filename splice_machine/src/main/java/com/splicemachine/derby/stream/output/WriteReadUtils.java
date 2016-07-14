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

package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.utils.Pair;

/**
 * Created by jleach on 5/6/15.
 */
public class WriteReadUtils {

    public static int[] getExecRowTypeFormatIds(ExecRow currentTemplate) throws StandardException {
       int[] execRowTypeFormatIds = new int[currentTemplate.nColumns()];
        for(int i = 1;i<=currentTemplate.nColumns();i++) {
            execRowTypeFormatIds[i-1] = currentTemplate.getColumn(i).getTypeFormatId();
        }
        return execRowTypeFormatIds;
    }

    public static ExecRow getExecRowFromTypeFormatIds(int[] typeFormatIds) {
        try {
            ExecRow row = new ValueRow(typeFormatIds.length);
            DataValueDescriptor dvds[] = new DataValueDescriptor[typeFormatIds.length];
            for (int pos = 0; pos < typeFormatIds.length; pos++) {
                dvds[pos] = LazyDataValueFactory.getLazyNull(typeFormatIds[pos]);
            }
            row.setRowArray(dvds);
            return row;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
   }

	public static Pair<Long,Long>[] getStartAndIncrementFromSystemTables(RowLocation[] autoIncrementRowLocationArray,DataDictionary dataDictionary, long seqConglomId) throws StandardException {
        if (autoIncrementRowLocationArray.length ==0)
            return new Pair[0];
        Pair<Long,Long>[] defaultAutoIncrementValues = new Pair[autoIncrementRowLocationArray.length];
        ConglomerateDescriptor conglomerateDescriptor = dataDictionary.getConglomerateDescriptor(seqConglomId);
        assert conglomerateDescriptor != null : "Can't find conglomerate descriptor for seqConglomId: "+seqConglomId;
        TableDescriptor tableDescriptor = dataDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());
        ColumnDescriptorList columnDescriptorList = tableDescriptor.getColumnDescriptorList();
        for(int i = 0; i< autoIncrementRowLocationArray.length; i++){
            ColumnDescriptor cd = columnDescriptorList.get(i);
            defaultAutoIncrementValues[i] = new Pair<>(cd.getAutoincStart(),cd.getAutoincInc());
        }
        return defaultAutoIncrementValues;
    }

}