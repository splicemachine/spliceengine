package com.splicemachine.derby.impl.sql.execute.operations.framework;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

public interface AggregateFinisher<K, T extends ExecRow> {
    T finishAggregation(T row) throws StandardException;
}
