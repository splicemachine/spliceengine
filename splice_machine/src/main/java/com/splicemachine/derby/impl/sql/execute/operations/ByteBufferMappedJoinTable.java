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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
class ByteBufferMappedJoinTable implements JoinTable{
    private final Map<ByteBuffer, List<ExecRow>> table;
    private int[] outerHashKeys;
    private ExecRow outerTemplateRow;

    public ByteBufferMappedJoinTable(Map<ByteBuffer, List<ExecRow>> table,int[] outerHashkeys, ExecRow outerTemplateRow){
        this.table=table;
        this.outerHashKeys = outerHashkeys;
        this.outerTemplateRow = outerTemplateRow;
    }

    @Override
    public Iterator<ExecRow> fetchInner(ExecRow outer) throws IOException, StandardException{
        byte[] outerKey=outer.generateRowKey(outerHashKeys);
        assert outerKey!=null: "Programmer error: outer row does not have row key";
        List<ExecRow> rows = table.get(ByteBuffer.wrap(outerKey));
        if(rows==null)
            return Collections.emptyIterator();
        else
            return rows.iterator();
    }

    //nothing to close
    @Override public void close(){}

    static class Factory implements JoinTable.Factory{
        private final Map<ByteBuffer,List<ExecRow>> table;
        private final int[] outerHashKeys;
        private final ExecRow outerTemplateRow;

        public Factory(Map<ByteBuffer, List<ExecRow>> table,int[] outerHashKeys,ExecRow outerTemplateRow){
            this.table=table;
            this.outerHashKeys=outerHashKeys;
            this.outerTemplateRow=outerTemplateRow;
        }

        @Override
        public JoinTable newTable(){
            return new ByteBufferMappedJoinTable(table,outerHashKeys,outerTemplateRow);
        }
    }
}
