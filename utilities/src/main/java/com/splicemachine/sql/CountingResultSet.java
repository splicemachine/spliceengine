package com.splicemachine.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 7/30/15
 */
public class CountingResultSet extends ForwardingResultSet{
    private long rowCount = 0l;
    public CountingResultSet(ResultSet rs){
        super(rs);
    }

    @Override
    public boolean next() throws SQLException{
        boolean next=super.next();
        if(next)
            rowCount++;
        return next;
    }

    public long getRowCount(){
        return rowCount;
    }
}
