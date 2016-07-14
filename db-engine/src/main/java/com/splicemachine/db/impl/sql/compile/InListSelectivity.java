/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.StoreCostController;

 /**
 *
 * Selectivity for an in clause list.  The selectivity is additive but if it ends up being over 1 it snaps back to 0.9.
  *
 *
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
