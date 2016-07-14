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
    private final List<TableRule> dependentTables = new LinkedList<>();

    public TableRule(Connection connection,
                     String tableName,
                     String tableSchema){
        this.connection = connection;
        this.tableName=tableName;
        this.tableSchema = tableSchema;
    }

    public TableRule childTable(TableRule childTable){
        dependentTables.add(childTable);
        return this;
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



    @Override
    public String toString(){
        return tableName;
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private void setup() throws SQLException{
        try(Statement s = connection.createStatement()){
            for(TableRule dependentTable: dependentTables){
                dependentTable.drop(s);
            }

            drop(s);


            create(s);

            for(TableRule dependentTable:dependentTables){
                dependentTable.create(s);
            }
        }
    }

    private void create(Statement s) throws SQLException{
        s.execute("create table "+tableName+tableSchema);
    }

    private void drop(Statement s) throws SQLException{
        s.execute("drop table if exists "+tableName);
    }
}
