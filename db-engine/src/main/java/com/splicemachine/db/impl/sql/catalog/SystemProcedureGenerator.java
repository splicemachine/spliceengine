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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
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
