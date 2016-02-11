package com.splicemachine.derby.impl.stats;


import com.google.common.base.Strings;
import com.splicemachine.derby.test.framework.TestConnection;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 4/1/15
 */
public class TestDataBuilder implements AutoCloseable{
    private List<PreparedStatement> insertStatements;
    private int rowCount = 0;
    private int rowPosition = 1;

    private final TestConnection connection;
    private final String insertFormat;
    private final int batchSize;

    public TestDataBuilder(String schema,String tableSchema,TestConnection connection,int batchSize){
        this.connection=connection;
        this.insertStatements = new ArrayList<>();
        this.batchSize = batchSize;
        this.insertFormat = buildInsertFormat(schema,tableSchema);
    }

    TestDataBuilder newTable(String tableName) throws SQLException{
        insertStatements.add(connection.prepareStatement(String.format(insertFormat,tableName)));
        return this;
    }

    public TestConnection getConnection(){
        return connection;
    }

    public TestDataBuilder booleanField(boolean val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setBoolean(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder shortField(short val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setShort(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder intField(int val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setInt(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder bigintField(long val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setLong(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder realField(float val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setFloat(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder doubleField(double val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setDouble(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder numericField(BigDecimal val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setBigDecimal(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder charField(String val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setString(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder varcharField(String val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setString(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder dateField(Date val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setDate(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder timeField(Time val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setTime(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public TestDataBuilder timestampField(Timestamp val) throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.setTimestamp(rowPosition,val);
        }
        rowPosition++;
        return this;
    }

    public void rowEnd() throws SQLException{
        for(PreparedStatement ps:insertStatements){
            ps.addBatch();
        }
        rowCount++;
        if(rowCount % batchSize==0){
            flush();
            rowCount=0;
        }
        rowPosition = 1;
    }

    public void flush() throws SQLException{
        if(rowCount<=0) return; //nothing to do
        for(PreparedStatement ps : insertStatements){
            ps.executeBatch();
        }
    }

    @Override
    public void close() throws SQLException{
        SQLException e = null;
        for(PreparedStatement ps:insertStatements){
            try{
                ps.close();
            }catch(SQLException se){
                if(e==null)
                    e = se;
                else{
                    e.setNextException(se);
                    e = se;
                }
            }
        }
        if(e!=null)
            throw e;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private String buildInsertFormat(String schema,String tableFormat){
        String insertFormat = "insert into "+schema+".%s";

        insertFormat+="("+tableFormat+") values (";
        String[] elems = tableFormat.split(",");
        if(elems.length==1)
            insertFormat+="?";
        else{
            String prepPoints=Strings.repeat("?,",elems.length-1);
            insertFormat+=prepPoints+"?)";
        }
        return insertFormat;
    }
}
