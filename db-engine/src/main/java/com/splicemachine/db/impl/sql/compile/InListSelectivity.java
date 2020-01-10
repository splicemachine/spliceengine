/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.store.access.StoreCostController;

import static com.splicemachine.db.impl.sql.compile.SelectivityUtil.DEFAULT_INLIST_SELECTIVITY;

/**
 *
 * Selectivity for an in clause list.  The selectivity is additive but if it ends up being over 1 it snaps back to 0.9.
  *
 *
 */
public class InListSelectivity extends AbstractSelectivityHolder {
    private Predicate p;
    private StoreCostController storeCost;
    private double selectivityFactor;
    private int colNo [];
    private final double DEFAULT_SINGLE_VALUE_SELECTIVITY = 0.5d;
    private boolean useExtrapolation = false;

    public InListSelectivity(StoreCostController storeCost, Predicate p,QualifierPhase phase, double selectivityFactor)
        throws StandardException {
        super(((ColumnReference) p.getSourceInList().leftOperandList.elementAt(0)).getColumnNumber(), phase);
        
        colNo = new int[p.getSourceInList().leftOperandList.size()];
        int i = 0;
        for (Object v : p.getSourceInList().leftOperandList) {
            colNo[i] = ((ColumnReference) v).getColumnNumber();
            i++;
        }
        this.p = p;
        this.storeCost = storeCost;
        this.selectivityFactor = selectivityFactor;
        this.useExtrapolation = isExtrapolationEnabled();
    }
    
    private double multiplySelectivity(ValueNode vn, int columnNumber, double localSelectivity, boolean useExtrapolation) {
        if (vn instanceof ConstantNode) {
            ConstantNode cn = (ConstantNode)vn;
            if (localSelectivity == -1.0d)
                localSelectivity = storeCost.getSelectivity(columnNumber, cn.getValue(), true, cn.getValue(), true, useExtrapolation);
            else
                localSelectivity *= storeCost.getSelectivity(columnNumber, cn.getValue(), true, cn.getValue(), true, useExtrapolation);
        }
        else
            localSelectivity *= DEFAULT_SINGLE_VALUE_SELECTIVITY;
        
        return localSelectivity;
    }
    private double addSelectivity(ValueNode vn, int columnNumber, double localSelectivity, boolean useExtrapolation) {
        double tempSel = -1.0d;
        if (vn instanceof ListValueNode) {
            ListValueNode lvn = (ListValueNode)vn;

            for (int i = 0; i < lvn.numValues(); i++) {
                ValueNode tempConst = lvn.getValue(i);
                tempSel = multiplySelectivity(tempConst, colNo[i], tempSel, useExtrapolation);
            }
        }
        else {
            if (vn instanceof ConstantNode) {
                ConstantNode cn = (ConstantNode) vn;
                tempSel = storeCost.getSelectivity(columnNumber, cn.getValue(), true,
                                                   cn.getValue(), true, useExtrapolation);
            }
            else {
                tempSel = DEFAULT_SINGLE_VALUE_SELECTIVITY;
            }
        }
        if (localSelectivity == -1.0d)
            localSelectivity = tempSel;
        else
            localSelectivity += tempSel;
    
        return localSelectivity;
    }

    public double getSelectivity() throws StandardException {
        if (selectivity==-1.0d) {
            InListOperatorNode sourceInList=p.getSourceInList();
            ValueNodeList rightOperandList=sourceInList.getRightOperandList();
            for(Object o: rightOperandList){
                ValueNode vn = (ValueNode)o;
                selectivity = addSelectivity(vn, getColNum(), selectivity, useExtrapolation);
            }
            if (selectivityFactor > 0)
                selectivity *= selectivityFactor;

            if (selectivity > DEFAULT_INLIST_SELECTIVITY)
                selectivity = DEFAULT_INLIST_SELECTIVITY;
        }
        return selectivity;
    }

    private boolean isExtrapolationEnabled() throws StandardException {
        InListOperatorNode sourceInList = p.getSourceInList();
        if(sourceInList==null)
            return false;
        ValueNode lo = sourceInList.getLeftOperand();
        if(!(lo instanceof ColumnReference))
            return false;
        ColumnReference cr = (ColumnReference)lo;
        if (cr == null)
            return false;
        ColumnDescriptor columnDescriptor = cr.getSource().getTableColumnDescriptor();
        if (columnDescriptor != null)
            return columnDescriptor.getUseExtrapolation()!=0;

        return false;

    }
}
