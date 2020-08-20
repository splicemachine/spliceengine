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

package com.splicemachine.db.iapi.sql.conn;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.StatementPermission;

/**
  The Authorizer verifies a connected user has the authorization 
  to perform a requested database operation using the current
  connection.

  <P>
  Today no object based authorization is supported.
  */
public interface Authorizer
{
	/** SQL write (insert,update,delete) operation */
	int SQL_WRITE_OP = 0;
	/** SQL SELECT  operation */
	int	SQL_SELECT_OP = 1;
	/** Any other SQL operation	*/
	int	SQL_ARBITARY_OP = 2;
	/** SQL CALL/VALUE  operation */
	int	SQL_CALL_OP = 3;
	/** SQL DDL operation */
	int SQL_DDL_OP   = 4;
	/** database property write operation */
	int PROPERTY_WRITE_OP = 5;
	/**  database jar write operation */
	int JAR_WRITE_OP = 6;
	
	/* Privilege types for SQL standard (grant/revoke) permissions checking. */
	int NULL_PRIV = -1;
	int SELECT_PRIV = 0;
	int UPDATE_PRIV = 1;
	int REFERENCES_PRIV = 2;
	int INSERT_PRIV = 3;
	int DELETE_PRIV = 4;
	int TRIGGER_PRIV = 5;
	int EXECUTE_PRIV = 6;
	int USAGE_PRIV = 7;
    /* 
     * DERBY-4191
     * Used to check if user has a table level select privilege/any column 
     * level select privilege to fulfill the requirements for following kind 
     * of queries
     * select count(*) from t1
     * select count(1) from t1
     * select 1 from t1
     * select t1.c1 from t1, t2
     * DERBY-4191 was added for Derby bug where for first 3 queries above,
     * we were not requiring any select privilege on t1. And for the 4th
     * query, we were not requiring any select privilege on t2 since no
     * column was selected from t2
     */
	int MIN_SELECT_PRIV = 8;
    int PRIV_TYPE_COUNT = 9;
    
	/* Used to check who can create schemas or who can modify objects in schema */
	int CREATE_SCHEMA_PRIV = 16;
	int MODIFY_SCHEMA_PRIV = 17;
	int DROP_SCHEMA_PRIV = 18;

    /* Check who can create and drop roles */
	int CREATE_ROLE_PRIV = 19;
	int DROP_ROLE_PRIV = 20;

	/* ACCESS schema privilege to control the visibility of a schemas to users */
	int ACCESS_PRIV = 21;

	/**
	 * The system authorization ID is defined by the SQL2003 spec as the grantor
	 * of privileges to object owners.
	 */
	String SYSTEM_AUTHORIZATION_ID = "_SYSTEM";

	/**
	 * The public authorization ID is defined by the SQL2003 spec as implying all users.
	 */
	String PUBLIC_AUTHORIZATION_ID = "PUBLIC";

	/**
	  Verify the connected user is authorized to perform the requested
	  operation.

	  This variation should only be used with operations that do not use tables
	  or routines. If the operation involves tables or routines then use the
	  variation of the authorize method that takes an Activation parameter. The
	  activation holds the table, column, and routine lists.

	  @param operation the enumeration code for the requsted operation.

	  @exception StandardException Thrown if the operation is not allowed
	 */
	void authorize(int operation) throws StandardException;

    boolean canSeeSchema(Activation activation, String schemaName, String authorizationId);

    /**
	  Verify the connected user is authorized to perform the requested
	  operation.

	  @param activation holds the list of tables, columns, and routines used.
	  @param operation the enumeration code for the requested operation.

	  @exception StandardException Thrown if the operation is not allowed.
	*/
	void authorize(Activation activation, int operation)
				throws StandardException;

   /**
	 Get the readOnly status for this authorizer's connection.
	 */
   boolean isReadOnlyConnection();

   /**
	 Set the readOnly status for this authorizer's connection.
	 @param on true means set the connection to read only mode,
	           false means set the connection to read wrte mode.
	 @param authorize true means to verify the caller has authority
	        to set the connection and false means do not check. 
	 @exception StandardException Oops not allowed.
	 */
   void setReadOnlyConnection(boolean on, boolean authorize)
		 throws StandardException;

   /**
	 Refresh this authorizer to reflect a change in the database
	 permissions.
	 
	 @exception AuthorizerSessionException Connect permission gone.
	 @exception StandardException Oops.
	 */
   void refresh() throws StandardException;
}
