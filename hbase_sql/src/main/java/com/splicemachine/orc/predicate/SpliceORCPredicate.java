package com.splicemachine.orc.predicate;

import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.orc.OrcPredicate;
import com.splicemachine.orc.metadata.ColumnStatistics;

import java.util.Map;

/**
 * Created by jleach on 3/20/17.
 */
public class SpliceORCPredicate implements OrcPredicate {
    protected Qualifier[][] qualifiers;
    public SpliceORCPredicate(Qualifier[][] qualifiers) {
        this.qualifiers = qualifiers;
    }

    @Override
    public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex) {
        return true;
    }
}
