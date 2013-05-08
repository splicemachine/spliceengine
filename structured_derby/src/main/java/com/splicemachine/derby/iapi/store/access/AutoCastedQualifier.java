package com.splicemachine.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;

public class AutoCastedQualifier implements Qualifier {

    private Qualifier originalQualifier;
    private DataValueDescriptor castedDvd;

    public AutoCastedQualifier(Qualifier originalQualifier, DataValueDescriptor castedDvd){
        this.originalQualifier = originalQualifier;
        this.castedDvd = castedDvd;
    }

    @Override
    public int getColumnId() {
        return originalQualifier.getColumnId();
    }

    @Override
    public DataValueDescriptor getOrderable() throws StandardException {
        return castedDvd;
    }

    @Override
    public int getOperator() {
        return originalQualifier.getOperator();
    }

    @Override
    public boolean negateCompareResult() {
        return originalQualifier.negateCompareResult();
    }

    @Override
    public boolean getOrderedNulls() {
        return originalQualifier.getOrderedNulls();
    }

    @Override
    public boolean getUnknownRV() {
        return originalQualifier.getUnknownRV();
    }

    @Override
    public void clearOrderableCache() {
        originalQualifier.clearOrderableCache();
    }

    @Override
    public void reinitialize() {
        originalQualifier.reinitialize();
    }
}
