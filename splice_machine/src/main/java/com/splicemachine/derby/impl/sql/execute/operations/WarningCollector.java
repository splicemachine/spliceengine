package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;

import java.sql.SQLWarning;

/**
 * @author Scott Fines
 *         Created on: 11/1/13
 */
public interface WarningCollector {

    void addWarning(String warningState) throws StandardException;
}
