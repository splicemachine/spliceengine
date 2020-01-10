/*
 * Copyright 2012 - 2020 Splice Machine, Inc.
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

package org.splicetest.sqlj;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedResultSetMetaData;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Properties;

/**
 * The purpose of this de
 * 
 * @author erindriggers
 *
 */
public class PropertiesFileVTI  implements DatasetProvider, VTICosting{

    private String fileName;
    
    protected OperationContext operationContext;
    
    public PropertiesFileVTI (String pfileName) {
        this.fileName = pfileName;
    }
       
    public static DatasetProvider getPropertiesFileVTI(String fileName) {
        return new PropertiesFileVTI(fileName);
    }

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
        operationContext = dsp.createOperationContext(op);
        
        //Create an arraylist to store the key / value pairs
        ArrayList<ExecRow> items = new ArrayList<ExecRow>();
        
        try {
            Properties properties = new Properties();
            
            //Load the properties file
            properties.load( getClass().getClassLoader().getResourceAsStream(fileName) );
            
            //Loop through the properties and create an array
            for(String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
                ValueRow valueRow = new ValueRow(2);
                valueRow.setColumn(1,new SQLVarchar(key));
                valueRow.setColumn(2,new SQLVarchar(value));
                items.add(valueRow);
              }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            operationContext.popScope();
        }
        return dsp.createDataSet(items.iterator());
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment arg0)
            throws SQLException {
        return 0;
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment arg0) throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment arg0)
            throws SQLException {
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }

    @Override
    public OperationContext getOperationContext() {
        return this.operationContext;
    }
    
    /*
     * Metadata
     */
    private static final ResultColumnDescriptor[] columnInfo = {
        EmbedResultSetMetaData.getResultColumnDescriptor("KEY1", Types.VARCHAR, false, 200),
        EmbedResultSetMetaData.getResultColumnDescriptor("VALUE", Types.VARCHAR, false, 200),
    };

    private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);
    
}
