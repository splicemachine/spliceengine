/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package com.splicemachine.db.client.am;

import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import com.splicemachine.db.shared.common.reference.SQLState;

/**
 * A metadata object to be used with logical connections when connection
 * pooling is being used.
 *
 * @see LogicalDatabaseMetaData
 */
public class LogicalDatabaseMetaData40
    extends LogicalDatabaseMetaData {

    /**
     * Creates a new logical database metadata object.
     *
     * @param logicalCon the associated logical connection
     * @param logWriter destination for log/error messages
     * @throws SQLException if obtaining the JDBC driver versions fail
     */
    LogicalDatabaseMetaData40(LogicalConnection logicalCon,
                              LogWriter logWriter)
            throws SQLException {
        super(logicalCon, logWriter);
    }

    public boolean autoCommitFailureClosesAllResultSets()
            throws SQLException {
        return getRealMetaDataObject().autoCommitFailureClosesAllResultSets();
    }

    public ResultSet getClientInfoProperties()
            throws SQLException {
        return getRealMetaDataObject().getClientInfoProperties();
    }

    public ResultSet getFunctions(String catalog, String schemaPattern,
                                  String functionNamePattern)
            throws SQLException {
        return getRealMetaDataObject().getFunctions(
                catalog, schemaPattern, functionNamePattern);
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern,
                                        String columnNamePattern)
            throws SQLException {
        return getRealMetaDataObject().getFunctionColumns(
                catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    public RowIdLifetime getRowIdLifetime()
            throws SQLException {
        return getRealMetaDataObject().getRowIdLifetime();
    }

    public ResultSet getSchemas(String catalog, String schemaPattern)
            throws SQLException {
        return getRealMetaDataObject().getSchemas(catalog, schemaPattern);
    }

    public boolean isWrapperFor(Class<?> interfaces)
            throws SQLException {
        getRealMetaDataObject(); // Check for open connection.
        return interfaces.isInstance(this);
    }

    public boolean supportsStoredFunctionsUsingCallSyntax()
            throws SQLException {
        return getRealMetaDataObject().supportsStoredFunctionsUsingCallSyntax();
    }

    public <T> T unwrap(Class<T> interfaces)
            throws SQLException {
        getRealMetaDataObject(); // Check for open connection.
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(
                                super.logWriter,
                                new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                                interfaces
                            ).getSQLException();
        }
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  JDBC 4.1 - New public methods
    //
    /////////////////////////////////////////////////////////////////////////

    /** See DatabaseMetaData javadoc */
    public  boolean generatedKeyAlwaysReturned() throws SQLException
    {
        return ((ClientDatabaseMetaData) getRealMetaDataObject()).generatedKeyAlwaysReturned();
    }

	/**
    * See DatabaseMetaData javadoc. Empty ResultSet because Derby does
    * not support pseudo columns.
	*/
	public ResultSet getPseudoColumns
        ( String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern )
        throws SQLException
	{
		return ((ClientDatabaseMetaData) getRealMetaDataObject()).getPseudoColumns
            ( catalog, schemaPattern, tableNamePattern, columnNamePattern );
	}

}
