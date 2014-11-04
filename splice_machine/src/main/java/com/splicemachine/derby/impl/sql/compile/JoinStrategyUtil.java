package com.splicemachine.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

public class JoinStrategyUtil {

    public static boolean isNonCoveringIndex(Optimizable innerTable) {
        try {
            AccessPath path = innerTable.getCurrentAccessPath();
            if (path != null) {
                ConglomerateDescriptor cd = path.getConglomerateDescriptor();
                if (cd != null && cd.isIndex() && !innerTable.isCoveringIndex(cd)) {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("could not determine if index is covering", e);
        }
        return false;
    }

}
