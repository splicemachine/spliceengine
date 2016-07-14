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
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.dbTesting.functionTests.util;

import java.util.Properties;

/**
 * This class extends PropertyInfo to provide support for viewing ALL
 * table/index properties, not just the user-visible ones.
 */
public class TestPropertyInfo
{

    /**
     * Get ALL the Properties associated with a given table, not just the
	 * customer-visible ones.
     *
	 * @param schemaName    The name of the schema that the table is in.
	 * @param tableName     The name of the table.
	 * 
	 * @return Properties   The Properties associated with the specified table.
     *                      (An empty Properties is returned if the table does not exist.)
     * @exception java.sql.SQLException thrown on error
     */
    public static String getAllTableProperties(String schemaName, String tableName)
        throws java.sql.SQLException
	{
		Properties p =	TestPropertyInfo.getConglomerateProperties( schemaName, tableName, false );
		if (p == null)
			return null;

        throw new UnsupportedOperationException("splice");
	}

/**
     * Get a specific property  associated with a given table, not just the
	 * customer-visible ones.
     *
	 * @param schemaName    The name of the schema that the table is in.
	 * @param tableName     The name of the table.
	 * 
	 * @param key           The table property  to retrieve
	 * @return               Property value 
     * @exception java.sql.SQLException thrown on error
     */
	public static String getTableProperty(String schemaName, String tableName,
										  String key) throws java.sql.SQLException
	{
		return TestPropertyInfo.getConglomerateProperties( schemaName, tableName, false ).getProperty(key);
	}

    /**
     * Get ALL the Properties associated with a given index, not just the customer-visible ones.
     *
	 * @param schemaName    The name of the schema that the index is in.
	 * @param indexName     The name of the index.
	 * 
	 * @return Properties   The Properties associated with the specified index.
     *                      (An empty Properties is returned if the index does not exist.)
     * @exception java.sql.SQLException thrown on error
     */
    public static String getAllIndexProperties(String schemaName, String indexName)
        throws java.sql.SQLException
	{
		Properties p = TestPropertyInfo.getConglomerateProperties( schemaName, indexName, true );

		if (p == null)
			return null;

		throw new UnsupportedOperationException("splice");
	}

	/**
	  Return the passed in Properties object with a property filtered out.
	  This is useful for filtering system depenent properties to make
	  test canons stable.
	  */
	public static Properties filter(Properties p, String filterMe)
	{
		p.remove(filterMe);
		return p;
	}

	private static Properties	getConglomerateProperties( String schemaName, String conglomerateName, boolean isIndex )
        throws java.sql.SQLException
	{
        throw new UnsupportedOperationException("splice");
	}
}
