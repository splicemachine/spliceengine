/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
    public int getStoragePosition() {
        return originalQualifier.getStoragePosition();
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

    @Override
    public int getVariantType() {
        return originalQualifier.getVariantType();
    }
}
