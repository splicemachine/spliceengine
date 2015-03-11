package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.catalog.Aggregate;
import com.splicemachine.db.impl.sql.catalog.DefaultSystemAggregateGenerator;
import com.splicemachine.derby.impl.sql.execute.operations.ColumnStatisticsMerge;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevPop;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevSamp;
import com.splicemachine.stats.ColumnStatistics;

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

        TypeId mergeTypeId = TypeId.getUserDefinedTypeId(ColumnStatistics.class.getCanonicalName(), false);
        DataTypeDescriptor dtd = new DataTypeDescriptor(mergeTypeId,true);
        TypeDescriptor mergeTypeDescriptor = dtd.getCatalogType();
        aggregate = new Aggregate("STATS_MERGE", mergeTypeDescriptor,mergeTypeDescriptor,
                ColumnStatisticsMerge.class.getCanonicalName());
        aggregate.createSystemAggregate(dictionary,tc,sysFunUUID);
    }
}
