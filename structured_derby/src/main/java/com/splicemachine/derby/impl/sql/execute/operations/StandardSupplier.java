package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;

/**
 * Supplier interface which allows the throwing of a StandardException
 *
 * @author Scott Fines
 * Created on: 10/29/13
 */
public interface StandardSupplier<T> {

    T get() throws StandardException;
}
