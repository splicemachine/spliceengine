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
    private Iterator<Iterable<Object>> rows;

    public IterableRowCreator(Iterable<Iterable<Object>> rows){
       this(rows,1);
    }

    public IterableRowCreator(Iterable<Iterable<Object>> rows,int batchSize){
        this.rows=rows.iterator();
        this.batchSize = batchSize;
    }

    @Override
    public boolean advanceRow(){
        return rows.hasNext();
    }

    @Override public int batchSize(){ return batchSize; }

    @Override
    public void setRow(PreparedStatement ps) throws SQLException{
        Iterable<Object> nextRow = rows.next();
        int i=1;
        for(Object v:nextRow){
            ps.setObject(i++,v);
        }
    }
}
