package com.splicemachine.perf.runner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/17/13
 */
public class Index {
    private final String name;
    private final String table;
    private final boolean unique;
    private final List<String> columns;

    public Index(String name, String table, boolean unique, List<String> columns) {
        this.name = name;
        this.table = table;
        this.unique = unique;
        this.columns = columns;
    }

    public void create(Connection conn) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(getCreateIndexString());
        ps.execute();
    }

    private String getCreateIndexString(){
        StringBuilder sb = new StringBuilder("create ");
        if(unique)
            sb = sb.append("unique ");
        sb = sb.append("index ").append(name).append(" on ").append(table).append("(");
        boolean isStart=true;
        for(String column:columns){
            if(!isStart)sb = sb.append(",");
            else isStart=false;

            sb = sb.append(column);
        }

        sb.append(")");

        return sb.toString();
    }
}
