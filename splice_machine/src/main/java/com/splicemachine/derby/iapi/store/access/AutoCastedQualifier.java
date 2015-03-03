package com.splicemachine.derby.iapi.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

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

    @Override
    public String getText() {
        return originalQualifier.getText();
    }
}
