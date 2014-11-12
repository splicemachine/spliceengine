package com.splicemachine.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.NumberDataType;

/**
 * @author Scott Fines
 *         Created on: 4/19/13
 */
abstract class LazyNumber extends NumberDataType {

    protected boolean dirtyValue;
    protected boolean dirtyBytes;

    protected byte[] serializedValue;

    @Override
    public void setValue(byte[] theValue) throws StandardException {
        this.serializedValue = theValue;
        this.dirtyValue = true;
    }

}
