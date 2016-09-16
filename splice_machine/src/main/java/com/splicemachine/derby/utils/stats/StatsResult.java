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

package com.splicemachine.derby.utils.stats;

import com.splicemachine.derby.iapi.sql.olap.AbstractOlapResult;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

import java.util.List;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class StatsResult extends AbstractOlapResult {
    List<LocatedRow> rowList;

    public StatsResult(List<LocatedRow> rowList) {
        this.rowList = rowList;
    }

    public List<LocatedRow> getRowList() {
        return rowList;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }
}
