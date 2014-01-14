package com.splicemachine.derby.utils;

import org.apache.derby.iapi.error.StandardException;

/**
 * @author Scott Fines
 *         Created on: 10/31/13
 */
public class StandardSuppliers {

    private StandardSuppliers(){}

    private static final StandardSupplier EMPTY_SUPPLIER = new StandardSupplier() {
        @Override
        public Object get() throws StandardException {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T>StandardSupplier<T> emptySupplier(){
        return (StandardSupplier<T>) EMPTY_SUPPLIER;
    }
}
