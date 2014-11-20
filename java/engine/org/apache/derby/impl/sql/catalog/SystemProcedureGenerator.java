package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.TransactionController;

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
