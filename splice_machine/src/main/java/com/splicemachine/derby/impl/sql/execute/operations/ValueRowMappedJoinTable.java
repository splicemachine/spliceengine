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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.JoinTable;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
class ValueRowMappedJoinTable implements JoinTable{
    private final Map<ValueRow, List<ExecRow>> table;
    private final int[] outerHashKeys;
    private final int numKeys;
    private final DataValueDescriptor[] keys;
    private final ValueRow keyRow;

    public ValueRowMappedJoinTable(Map<ValueRow, List<ExecRow>> table, int[] outerHashkeys){
        this.table=table;
        this.outerHashKeys = outerHashkeys;
        this.numKeys = outerHashKeys.length;
        this.keys = new DataValueDescriptor[numKeys];
        this.keyRow = new ValueRow(keys);
    }

    @Override
    public Iterator<ExecRow> fetchInner(ExecRow outer) throws IOException, StandardException{

        for (int i = 0; i < numKeys; i++) {
            keyRow.setColumn(i+1, outer.getColumn(outerHashKeys[i] + 1));
        }
        
        List<ExecRow> rows = table.get(keyRow);
        if(rows==null)
            return Collections.emptyIterator();
        else
            return rows.iterator();
    }

    //nothing to close
    @Override public void close(){}

    static class Factory implements JoinTable.Factory{
        private final Map<ValueRow,List<ExecRow>> table;
        private final int[] outerHashKeys;

        public Factory(Map<ValueRow, List<ExecRow>> table,int[] outerHashKeys){
            this.table=table;
            this.outerHashKeys=outerHashKeys;
        }

        @Override
        public JoinTable newTable(){
            return new ValueRowMappedJoinTable(table, outerHashKeys);
        }
    }
}
