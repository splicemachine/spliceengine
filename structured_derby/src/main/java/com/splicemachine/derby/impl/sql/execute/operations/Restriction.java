package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * Abstract representation of a Restriction.
 *
 * @author Scott Fines
 * Created on: 10/29/13
 */
public interface Restriction {
    /**
     * Apply a restriction to the merged row.
     *
     * @param row the row to restrict
     * @return true if the row is to be emitted, false otherwise
     * @throws org.apache.derby.iapi.error.StandardException if something goes wrong during the restriction
     */
    boolean apply(ExecRow row) throws StandardException;

    static final Restriction noOpRestriction = new Restriction() {
        @Override
        public boolean apply(ExecRow row) throws StandardException {
            return true;
        }
    };
}
