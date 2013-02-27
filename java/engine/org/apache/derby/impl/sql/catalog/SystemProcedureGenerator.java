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

    String MODULE = "org.apache.derby.impl.sql.catalog.SystemProcedureGenerator";

    void createProcedures(TransactionController tc, HashSet newlyCreatedRoutines) throws StandardException;




}
