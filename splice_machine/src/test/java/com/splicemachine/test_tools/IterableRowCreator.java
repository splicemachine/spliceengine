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
