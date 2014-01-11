package com.splicemachine.derby.impl.sql.execute.operations.framework;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

public interface AggregateFinisher<K, T extends ExecRow> {
    T finishAggregation(T row) throws StandardException;
}
