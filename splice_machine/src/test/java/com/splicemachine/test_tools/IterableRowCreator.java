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

package com.splicemachine.test_tools;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 8/28/15
 */
public class IterableRowCreator implements RowCreator{
    private final int batchSize;
    private final Iterable<Iterable<Object>> rows;

    private Iterator<Iterable<Object>> currentIter;

    public IterableRowCreator(Iterable<Iterable<Object>> rows){
       this(rows,1);
    }

    public IterableRowCreator(Iterable<Iterable<Object>> rows,int batchSize){
        this.rows=rows;
        this.batchSize = batchSize;
        this.currentIter = rows.iterator();
    }

    @Override
    public boolean advanceRow(){
        return currentIter!=null && currentIter.hasNext();
    }

    @Override public int batchSize(){ return batchSize; }

    @Override
    public void setRow(PreparedStatement ps) throws SQLException{
        Iterable<Object> nextRow = currentIter.next();
        int i=1;
        for(Object v:nextRow){
            ps.setObject(i++,v);
        }
    }

    @Override
    public void reset(){
       currentIter = rows.iterator();
    }
}
