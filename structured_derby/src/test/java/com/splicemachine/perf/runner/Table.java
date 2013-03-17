package com.splicemachine.perf.runner;

import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Scott Fines
 *Created on: 3/15/13
 */
public class Table {
    private final String name;
    private final List<Column> columns;
    private final int numRows;

    public Table(String name, List<Column> columns, int numRows) {
        this.name = name;
        this.columns = columns;
        this.numRows = numRows;
    }

    public PreparedStatement getCreateTableStatement(Connection conn) throws SQLException{
        return conn.prepareStatement(getCreateTableString());
    }

    public void loadData(Connection conn) throws SQLException{
        PreparedStatement ps = conn.prepareStatement(getInsertDataString());
    }

    public String getInsertDataString(){
        StringBuilder ib = new StringBuilder("insert into ").append(name).append("(");
        boolean isStart=true;
        for(Column column:columns){
            if(!isStart)ib = ib.append(",");
            else isStart=false;
            ib = ib.append(column.getName());
        }
        ib = ib.append(") values (");
        isStart=true;
        for(int i=0;i<columns.size();i++){
            if(!isStart)ib = ib.append(",");
            else isStart=false;
            ib = ib.append("?");
        }
        ib = ib.append(")");
        return ib.toString();
    }

    public String getCreateTableString(){
        StringBuilder cb = new StringBuilder("create table ").append(name).append("(");
        boolean isStart = true;
        List<Column> pkCols = Lists.newArrayList();
        for(Column column:columns){
            if(!isStart)cb = cb.append(",");
            else isStart = false;
            cb.append(column.getSqlRep());

            if(column.isPrimaryKey())pkCols.add(column);
        }

        if(pkCols.size()>0){
            cb = cb.append(", PRIMARY KEY(");
            isStart=true;
            for(Column pkCol:pkCols){
                if(!isStart)cb.append(",");
                else isStart=false;
                cb = cb.append(pkCol.getName());
            }
            cb = cb.append(")");
        }
        cb.append(")");
        return cb.toString();
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", columns=" + columns +
                ", numRows=" + numRows +
                '}';
    }

    public String getName() {
        return name;
    }

    public PreparedStatement getInsertDataStatement(Connection conn) throws SQLException {
        return conn.prepareStatement(getInsertDataString());
    }

    public int getNumRows() {
        return numRows;
    }

    public void fillStatement(PreparedStatement ps) throws SQLException {
        int pos=1;
        for(Column column:columns){
            column.setData(ps, pos);
            pos++;
        }
    }
}
