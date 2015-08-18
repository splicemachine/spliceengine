package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.StoreCostController;

/**
 * Created by jleach on 8/8/15.
 */
public class InListSelectivity extends AbstractSelectivityHolder {
    private Predicate p;
    private StoreCostController storeCost;

    public InListSelectivity(StoreCostController storeCost, Predicate p,QualifierPhase phase){
        super( ( (ColumnReference) p.getSourceInList().getLeftOperand()).getColumnNumber(),phase);
        this.p = p;
        this.storeCost = storeCost;
    }

    public double getSelectivity() throws StandardException {
        if (selectivity==-1.0d) {
            InListOperatorNode sourceInList=p.getSourceInList();
            ValueNodeList rightOperandList=sourceInList.getRightOperandList();
            for(Object o: rightOperandList){
                ConstantNode cn = (ConstantNode)o;
                if (selectivity==-1.0d)
                    selectivity = storeCost.getSelectivity(colNum,cn.getValue(),true,cn.getValue(),true);
                else
                    selectivity+=storeCost.getSelectivity(colNum,cn.getValue(),true,cn.getValue(),true);
            }
        }
        return selectivity>1.0d?0.9d:selectivity;
    }
}
