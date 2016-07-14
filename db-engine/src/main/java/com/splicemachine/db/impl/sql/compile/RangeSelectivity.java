/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 *
 * Range Selectivity Holder, allows one to modify the start or stop based on steps in the optimizer.
 *
 */
public class RangeSelectivity extends AbstractSelectivityHolder {
    public DataValueDescriptor start;
    public boolean includeStart;
    public DataValueDescriptor stop;
    public boolean includeStop;
    public StoreCostController storeCost;

    public RangeSelectivity(StoreCostController storeCost,DataValueDescriptor start, DataValueDescriptor stop,boolean includeStart, boolean includeStop,
                            int colNum, QualifierPhase phase){
        super(colNum,phase);
        this.start = start;
        this.stop = stop;
        this.includeStart = includeStart;
        this.includeStop = includeStop;
        this.storeCost = storeCost;
    }

    public double getSelectivity()  throws StandardException {
        if (selectivity==-1.0d)
            selectivity = storeCost.getSelectivity(colNum,start,includeStart,stop,includeStop);
        return selectivity;
    }
    public boolean isRangeSelectivity() {
        return true;
    }
}

