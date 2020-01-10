/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.db;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.error.PublicAPI;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;

import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.Properties;
import java.sql.SQLException;

/**
  *	PropertyInfo is a class with static methods that retrieve the properties
  * associated with a table or index and set and retrieve properties associated
  * with a database.
  * 
  * <P>
  This class can only be used within an SQL-J statement, a Java procedure or a server side Java method.
  <p>This class can be accessed using the class alias <code> PROPERTYINFO </code> in SQL-J statements.
  */
public final class PropertyInfo
{

    /**
     * Get the Properties associated with a given table.
     *
	 * @param schemaName    The name of the schema that the table is in.
	 * @param tableName     The name of the table.
	 * 
	 * @return Properties   The Properties associated with the specified table.
     *                      (An empty Properties is returned if the table does not exist.)
     * @exception SQLException on error
     */
    public static Properties getTableProperties(String schemaName, String tableName)
        throws SQLException
	{
		return	PropertyInfo.getConglomerateProperties( schemaName, tableName, false );
	}

	/**
		Set or delete the value of a property of the database on the current connection.

		@param key the property key
		@param value the new value, if null the property is deleted.

		@exception SQLException on error
	*/
	public static void setDatabaseProperty(String key, String value) throws SQLException
	{
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

		try {
		Authorizer a = lcc.getAuthorizer();
		a.authorize((Activation) null, Authorizer.PROPERTY_WRITE_OP);

        // Get the current transaction controller
        TransactionController tc = lcc.getTransactionExecute();

		tc.setProperty(key, value, false);
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	/**
	  Internal use only.
	  */
    private	PropertyInfo() {}


	//////////////////////////////////////////////////////////////////////////////
	//
	//	PRIVATE METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

    /**
     * Get the Properties associated with a given conglomerate
     *
	 * @param schemaName    	The name of the schema that the conglomerate is in.
	 * @param conglomerateName  The name of the conglomerate.
	 * 
	 * @return Properties   The Properties associated with the specified conglomerate.
     *                      (An empty Properties is returned if the conglomerate does not exist.)
     * @exception SQLException on error
     */
	private static Properties	getConglomerateProperties( String schemaName, String conglomerateName, boolean isIndex )
        throws SQLException
	{
		long					  conglomerateNumber;

        // find the language context.
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

        // Get the current transaction controller
        TransactionController tc = lcc.getTransactionExecute();

		try {

		// find the DataDictionary
		DataDictionary dd = lcc.getDataDictionary();


		// get the SchemaDescriptor
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
		if ( !isIndex)
		{
			// get the TableDescriptor for the table
			TableDescriptor td = dd.getTableDescriptor(conglomerateName, sd, tc);

			// Return an empty Properties if table does not exist or if it is for a view.
			if ((td == null) || td.getTableType() == TableDescriptor.VIEW_TYPE) { return new Properties(); }

			conglomerateNumber = td.getHeapConglomerateId();
		}
		else
		{
			// get the ConglomerateDescriptor for the index
			ConglomerateDescriptor cd = dd.getConglomerateDescriptor(conglomerateName, sd, false);

			// Return an empty Properties if index does not exist
			if (cd == null) { return new Properties(); }

			conglomerateNumber = cd.getConglomerateNumber();
		}

		ConglomerateController cc = tc.openConglomerate(
                conglomerateNumber,
                false,
                0, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		Properties properties = tc.getUserCreateConglomPropList();
		cc.getTableProperties( properties );

		cc.close();
        return properties;

		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}

	}
}
