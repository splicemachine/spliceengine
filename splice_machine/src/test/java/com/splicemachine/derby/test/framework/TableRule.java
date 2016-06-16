package com.splicemachine.derby.test.framework;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

/**
 * Rule for configuring a table, given a connection.
 * @author Scott Fines
 *         Date: 6/21/16
 */
public class TableRule implements TestRule{
    private final String tableName;
    private final String tableSchema;
    private final Connection connection;

    public TableRule(Connection connection,
                     String tableName,
                     String tableSchema){
        this.connection = connection;
        this.tableName=tableName;
        this.tableSchema = tableSchema;
    }

    @Override
    public org.junit.runners.model.Statement apply(final org.junit.runners.model.Statement base,
                                                   Description description){

        return new org.junit.runners.model.Statement(){
            @Override
            public void evaluate() throws Throwable{
                try{
                    setup();
                }catch(SQLException e){
                    throw new SetupFailureException(e);
                }
                List<Throwable> errors = new LinkedList<>();
                try{
                    base.evaluate();
                }catch(Throwable t){
                    errors.add(t);
                }

                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

    private void setup() throws SQLException{
        try(Statement s = connection.createStatement()){
            s.execute("drop table if exists "+tableName);

            s.execute("create table "+tableName+tableSchema);
        }
    }

    @Override
    public String toString(){
        return tableName;
    }
}
