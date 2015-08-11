package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Optimizable;

/**
 * Created by jleach on 8/8/15.
 */
    public class PredicateSelectivity extends AbstractSelectivityHolder {
        private Predicate p;
        private Optimizable baseTable;
        public PredicateSelectivity(Predicate p,Optimizable baseTable, QualifierPhase phase){
            super(0,phase);
            this.p = p;
            this.baseTable = baseTable;
        }

        public double getSelectivity() throws StandardException {
            if (selectivity == -1.0d)
                selectivity = p.selectivity(baseTable);
            return selectivity;
        }

    }
