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
import org.junit.runners.model.Statement;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 6/27/16
 */
public class SchemaRule implements TestRule{
    private final String schemaName;
    private final Connection connection;

    public SchemaRule(Connection connection,String schemaName){
        this.schemaName=processSchemaName(schemaName);
        this.connection=connection;
    }

    @Override
    public Statement apply(final Statement base,Description description){
        return new Statement(){
            @Override
            public void evaluate() throws Throwable{
                String oldSchema=setupSchema();

                List<Throwable> errors = new LinkedList<>();
                try{
                    base.evaluate();
                }catch(Throwable t){
                    errors.add(t);
                }

                try{
                    connection.setSchema(oldSchema);
                }catch(SQLException se){
                    errors.add(se);
                }

                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

    public String setupSchema() throws SetupFailureException{
        String oldSchema;
        try{
            oldSchema = connection.getSchema();
        }catch(SQLException se){
            oldSchema="SPLICE";
        }
        try{
            connection.setSchema("SPLICE");
            createSchema();
        }catch(SQLException se){
            //X0Y68 is the "SCHEMA already exists" error, so just ignore those
            if(!"X0Y68".equals(se.getSQLState()))
                throw new SetupFailureException(se);
        }

        try{
            connection.setSchema(schemaName);
        }catch(SQLException se){
            throw new SetupFailureException(se);
        }
        return oldSchema;
    }

    private void createSchema() throws SQLException{
        try(java.sql.Statement s =connection.createStatement()){
            s.execute("create schema "+ schemaName);
        }
    }

    private String processSchemaName(String schema){
        if(schema==null) throw new IllegalArgumentException("No Schema name specified");
        if(schema.contains("\"")) return schema;
        else return schema.toUpperCase();
    }
}
