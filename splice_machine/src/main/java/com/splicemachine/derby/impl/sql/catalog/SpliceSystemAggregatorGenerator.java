package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.impl.sql.execute.operations.ColumnStatisticsMerge;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevPop;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevSamp;
import com.splicemachine.stats.ColumnStatistics;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.impl.sql.catalog.Aggregate;
import org.apache.derby.impl.sql.catalog.DefaultSystemAggregateGenerator;

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
