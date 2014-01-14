package com.splicemachine.derby.iapi.sql.execute;

/**
 * Used to indicate an Operation which has to interact directly with Derby code.
 *
 * @author Scott Fines
 * Created on: 9/23/13
 */
public interface ConvertedResultSet {

    SpliceOperation getOperation();
}
