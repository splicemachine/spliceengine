package com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.AggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.Externalizable;

/**
 * Provides information about a GroupedAggregate operation
 *
 * @author Scott Fines
 * Created on: 11/4/13
 */
public interface GroupedAggregateContext extends WarningCollector,Externalizable{

    void init(SpliceOperationContext context,AggregateContext genericAggregateContext) throws StandardException;

    int[] getGroupingKeys();

    boolean[] getGroupingKeyOrder();

    int[] getNonGroupedUniqueColumns();

    int getNumDistinctAggregates();
}
