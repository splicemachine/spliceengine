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

import com.splicemachine.db.iapi.sql.compile.CostEstimate;

public class Level2CostEstimateImpl extends CostEstimateImpl {
    public Level2CostEstimateImpl() { }

    public Level2CostEstimateImpl(double theCost,
                                  double theRowCount,
                                  double theSingleScanRowCount) {
        super(theCost, theRowCount, theSingleScanRowCount);
    }

    /** @see CostEstimate#cloneMe */
    public CostEstimate cloneMe() {
        return new Level2CostEstimateImpl(cost, rowCount, singleScanRowCount);
    }

    public String toString() {
        return "Level2CostEstimateImpl: at " + hashCode() + ", cost == " + cost +
                ", rowCount == " + rowCount +
                ", singleScanRowCount == " + singleScanRowCount;
    }

    public CostEstimateImpl setState(double theCost, double theRowCount, CostEstimateImpl retval) {
        if (retval == null) {
            retval = new Level2CostEstimateImpl();
        }

        return super.setState(theCost, theRowCount, retval);
    }
}
