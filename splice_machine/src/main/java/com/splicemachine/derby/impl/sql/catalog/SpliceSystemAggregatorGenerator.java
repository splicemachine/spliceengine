/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
