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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.HashSet;

/**
 * Generates System-level Stored Procedures
 *
 * @author Scott Fines
 * Date: 2/18/13
 */
public interface SystemProcedureGenerator {

    String MODULE = SystemProcedureGenerator.class.getCanonicalName();

    void createProcedures(TransactionController tc, HashSet newlyCreatedRoutines) throws StandardException;

    /**
	 * Create or update a system stored procedure.  If the system stored procedure already exists in the data dictionary,
	 * the stored procedure will be dropped and then created again.
	 * 
	 * @param schemaName           the schema where the procedure does and/or will reside
	 * @param procName             the procedure to create or update
	 * @param tc                   the xact
	 * @param newlyCreatedRoutines set of newly created procedures
	 * @throws StandardException
	 */
    void createOrUpdateProcedure(
    		String schemaName,
    		String procName,
    		TransactionController tc,
    		HashSet newlyCreatedRoutines) throws StandardException;

    /**
	 * Create or update all system stored procedures.  If the system stored procedure already exists in the data dictionary,
	 * the stored procedure will be dropped and then created again.
	 * 
	 * @param schemaName           the schema where the procedures do and/or will reside
	 * @param tc                   the xact
	 * @param newlyCreatedRoutines set of newly created procedures
	 * @throws StandardException
	 */
    void createOrUpdateAllProcedures(
    		String schemaName,
    		TransactionController tc,
    		HashSet newlyCreatedRoutines) throws StandardException;

    /**
	 * Create a system stored procedure.
	 * PLEASE NOTE:
	 * This method is currently not used, but will be used when Splice Machine has a SYS_DEBUG schema available
	 * with tools to debug and repair databases and data dictionaries.
	 *
	 * @param schemaName	name of the system schema
	 * @param procName		name of the system stored procedure
     * @param tc			TransactionController to use
     * @param newlyCreatedRoutines	set of newly created routines
     * @throws StandardException
     */
	void createProcedure(String schemaName, String procName,
		TransactionController tc, HashSet newlyCreatedRoutines) throws StandardException;

    /**
	 * Drop a system stored procedure.
	 * PLEASE NOTE:
	 * This method is currently not used, but will be used when Splice Machine has a SYS_DEBUG schema available
	 * with tools to debug and repair databases and data dictionaries.
	 *
	 * @param schemaName	name of the system schema
	 * @param procName		name of the system stored procedure
     * @param tc			TransactionController to use
     * @param newlyCreatedRoutines	set of newly created routines
     * @throws StandardException
     */
	void dropProcedure(String schemaName, String procName,
		TransactionController tc, HashSet newlyCreatedRoutines) throws StandardException;
}
