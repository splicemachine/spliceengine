/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
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
    private double selectivityFactor;
    private boolean useExtrapolation;

    public RangeSelectivity(StoreCostController storeCost,DataValueDescriptor start, DataValueDescriptor stop,boolean includeStart, boolean includeStop,
                            int colNum, QualifierPhase phase, double selectivityFactor, boolean useExtrapolation){
        super(colNum,phase);
        this.start = start;
        this.stop = stop;
        this.includeStart = includeStart;
        this.includeStop = includeStop;
        this.storeCost = storeCost;
        this.selectivityFactor = selectivityFactor;
        this.useExtrapolation = useExtrapolation;
    }

    public double getSelectivity()  throws StandardException {
        if (selectivity==-1.0d) {
            selectivity = storeCost.getSelectivity(colNum, start, includeStart, stop, includeStop, useExtrapolation);
            if (selectivityFactor > 0)
                selectivity *= selectivityFactor;
        }
        return selectivity;
    }
    public boolean isRangeSelectivity() {
        return true;
    }
}

