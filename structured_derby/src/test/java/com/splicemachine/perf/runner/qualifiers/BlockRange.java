package com.splicemachine.perf.runner.qualifiers;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Created on: 3/28/13
 */
public class BlockRange implements Qualifier{
    private final int start;
    private final int resultPosition;
    private final AtomicInteger nextBound;
    private final int range;

    public BlockRange(int start, int resultPosition, int range) {
        this.start = start;
        this.resultPosition = resultPosition;
        this.range = range;
        this.nextBound=new AtomicInteger(start);
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException {
        ps.setInt(position,nextBound());
    }

    private int nextBound(){
        return nextBound.getAndAdd(range);
    }

    @Override
    public void validate(ResultSet rs) throws SQLException {
//        Preconditions.checkArgument(rs.getInt(resultPosition)<start,"Too large value returned!");
    }

    public static Qualifier create(int resultPosition,Map<String,Object> qualifierConfig){
        int start = 0;
        boolean isMax=true;
        int range=10;
        if(qualifierConfig.containsKey("start"))
            start = ((Double)qualifierConfig.get("start")).intValue();
        if(qualifierConfig.containsKey("range"))
            range = ((Double)qualifierConfig.get("range")).intValue();
        return new BlockRange(start, resultPosition, range);
    }
}
