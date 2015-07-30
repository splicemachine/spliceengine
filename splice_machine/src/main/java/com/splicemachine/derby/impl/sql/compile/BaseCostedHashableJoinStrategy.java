package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.BitSet;

/**
 * Abstract class to provide convenience methods for costing joins.
 *
 * @author Scott Fines
 *         Date: 3/25/15
 */
public abstract class BaseCostedHashableJoinStrategy extends HashableJoinStrategy{

    protected double getTotalHeapSize(double outerHeapSize,
                                        double innerHeapSize,
                                        double innerRowCount,
                                        double outerRowCount,
                                        double totalOutputRows){
        double perRowHeapSize = outerHeapSize/outerRowCount;
        perRowHeapSize+=innerHeapSize/innerRowCount;
        return perRowHeapSize*totalOutputRows;
    }

    protected double getTotalRemoteCost(double outerRemoteCost,
                                        double innerRemoteCost,
                                        double outerRowCount,
                                        double innerRowCount,
                                        double totalOutputRows){
        double perRowRemoteCost = outerRemoteCost/outerRowCount;
        perRowRemoteCost+=innerRemoteCost/innerRowCount;
        return totalOutputRows*perRowRemoteCost;
    }

}
