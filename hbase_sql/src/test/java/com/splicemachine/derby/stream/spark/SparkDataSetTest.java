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

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.AbstractDataSetTest;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.spark.SparkDataSet;

/**
 * Created by jleach on 4/15/15.
 */
public class SparkDataSetTest extends AbstractDataSetTest{

    public SparkDataSetTest() {
        super();
    }

    @Override
    protected DataSet<ExecRow> getTenRowsTwoDuplicateRecordsDataSet() {
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(tenRowsTwoDuplicateRecords));
    }

}