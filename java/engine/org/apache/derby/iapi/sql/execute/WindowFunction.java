package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * A pure function interface -- apply function over one or more arguments
 * and return a value.
 *
 * @author Jeff Cunningham
 *         Date: 8/4/14
 */
public interface WindowFunction extends ExecAggregator {

    /**
     * Apply the function to the given dvds in infix order and return the result.<br/>
     *
     * @param leftDvd dvd to which to apply to the LHS the function.
     * @param rightDvd dvd to which to apply to the LHS the function, if needed.
     * @param previousValue the running value of previous calls to this method, if needed.
     * @return the accumulation of this execution and previousValue
     */
//    DataValueDescriptor apply(DataValueDescriptor leftDvd, DataValueDescriptor rightDvd, DataValueDescriptor previousValue) throws StandardException;

    WindowFunction newWindowFunction();
}
