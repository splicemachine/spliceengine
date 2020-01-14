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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
@ThreadSafe
class ValueRowMapTableLoader implements BroadcastJoinCache.JoinTableLoader{
    public static BroadcastJoinCache.JoinTableLoader INSTANCE = new ValueRowMapTableLoader();

    private ValueRowMapTableLoader(){} //singleton class

    @Override
    public JoinTable.Factory load(Callable<Stream<ExecRow>> streamLoader,int[] innerHashKeys,int[] outerHashKeys, ExecRow outerTemplateRow) throws Exception {
        Map<ValueRow, List<ExecRow>> table=new HashMap<>();

        int numKeys = innerHashKeys.length;
        DataValueDescriptor[] keys = new DataValueDescriptor[numKeys];
        ValueRow keyRow = new ValueRow(keys);
        try(Stream<ExecRow> innerRows=streamLoader.call()){
            ExecRow right;
            while((right=innerRows.next())!=null){

                for (int i = 0; i < numKeys; i++) {
                    keyRow.setColumn(i+1, right.getColumn(innerHashKeys[i] + 1));
                }
                List<ExecRow> rows=table.get(keyRow);
                if(rows==null){
                    rows=new ArrayList<>(1);
                    table.put((ValueRow)keyRow.getClone(), rows);
                }
                rows.add(right.getClone());
            }
        }catch(Exception e){
            throw getException(e);
        }

        return new ValueRowMappedJoinTable.Factory(table,outerHashKeys);
    }

    private Exception getException(Throwable parent) {
        if (parent.getCause() instanceof Exception)
            return (Exception)parent.getCause();
        if (parent instanceof Exception)
            return (Exception)parent;
        return new ExecutionException(parent);
    }
}
