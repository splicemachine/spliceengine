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

package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ColumnStatisticsMerge;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.catalog.Aggregate;
import com.splicemachine.db.impl.sql.catalog.DefaultSystemAggregateGenerator;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevPop;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevSamp;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
public class SpliceSystemAggregatorGenerator extends DefaultSystemAggregateGenerator {
    public SpliceSystemAggregatorGenerator(DataDictionary dictionary) {
        super(dictionary);
    }

    @Override
    public void createAggregates(TransactionController tc) throws StandardException {
        super.createAggregates(tc);
        //now create Splice-specific aggregates
        UUID sysFunUUID = dictionary.getSysFunSchemaDescriptor().getUUID();

        Aggregate aggregate = new Aggregate(
                "STDDEV_POP",
                TypeDescriptor.DOUBLE,
                TypeDescriptor.DOUBLE,
                SpliceStddevPop.class.getCanonicalName());

        aggregate.createSystemAggregate(dictionary, tc, sysFunUUID);

        aggregate = new Aggregate(
                "STDDEV_SAMP",
                TypeDescriptor.DOUBLE,
                TypeDescriptor.DOUBLE,
                SpliceStddevSamp.class.getCanonicalName());

        aggregate.createSystemAggregate(dictionary, tc, sysFunUUID);

        TypeId mergeTypeId = TypeId.getUserDefinedTypeId(ColumnStatisticsImpl.class.getCanonicalName(), false);
        DataTypeDescriptor dtd = new DataTypeDescriptor(mergeTypeId,true);
        TypeDescriptor mergeTypeDescriptor = dtd.getCatalogType();
        aggregate = new Aggregate("STATS_MERGE", mergeTypeDescriptor,mergeTypeDescriptor,
                ColumnStatisticsMerge.class.getCanonicalName());
        aggregate.createSystemAggregate(dictionary,tc,sysFunUUID);

    }
}
