/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
                try{
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
