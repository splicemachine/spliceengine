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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.AccessPath;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

class AccessPathImpl implements AccessPath{
    ConglomerateDescriptor cd=null;
    boolean coveringIndexScan=false;
    boolean nonMatchingIndexScan=false;
    JoinStrategy joinStrategy=null;
    int lockMode;
    Optimizer optimizer;
    private CostEstimate costEstimate=null;
    private boolean isJoinStrategyHinted = false;

    AccessPathImpl(Optimizer optimizer){
        this.optimizer=optimizer;
    }

    @Override public ConglomerateDescriptor getConglomerateDescriptor(){ return cd; }

    @Override public void setConglomerateDescriptor(ConglomerateDescriptor cd){ this.cd=cd; }
    @Override public CostEstimate getCostEstimate(){ return costEstimate; }

    @Override
    public void setCostEstimate(CostEstimate costEstimate){
        /*
		** CostEstimates are mutable, so keep the best cost estimate in
		** a copy.
		*/
        if(this.costEstimate==null){
            if(costEstimate!=null){
                this.costEstimate=costEstimate.cloneMe();
            }
        }else{
            if(costEstimate==null)
                this.costEstimate=null;
            else
                this.costEstimate.setCost(costEstimate);
        }
    }

    @Override public boolean getCoveringIndexScan(){ return coveringIndexScan; }
    @Override public void setCoveringIndexScan(boolean coveringIndexScan){ this.coveringIndexScan=coveringIndexScan; }

    @Override
    public boolean getNonMatchingIndexScan(){ return nonMatchingIndexScan; }
    @Override
    public void setNonMatchingIndexScan(boolean nonMatchingIndexScan){ this.nonMatchingIndexScan=nonMatchingIndexScan; }

    @Override public JoinStrategy getJoinStrategy(){ return joinStrategy; }
    @Override public void setJoinStrategy(JoinStrategy joinStrategy){ this.joinStrategy=joinStrategy; }

    @Override public int getLockMode(){ return lockMode; }
    @Override public void setLockMode(int lockMode){ this.lockMode=lockMode; }

    @Override
    public void copy(AccessPath copyFrom){
        setConglomerateDescriptor(copyFrom.getConglomerateDescriptor());
        setCostEstimate(copyFrom.getCostEstimate());
        setCoveringIndexScan(copyFrom.getCoveringIndexScan());
        setNonMatchingIndexScan(copyFrom.getNonMatchingIndexScan());
        setJoinStrategy(copyFrom.getJoinStrategy());
        setHintedJoinStrategy(copyFrom.isHintedJoinStrategy());
        setLockMode(copyFrom.getLockMode());
    }

    @Override public Optimizer getOptimizer(){ return optimizer; }

    @Override
    public String toString(){
        if(SanityManager.DEBUG){
            return "cd == "+cd+
                    ", costEstimate == "+costEstimate+
                    ", coveringIndexScan == "+coveringIndexScan+
                    ", nonMatchingIndexScan == "+nonMatchingIndexScan+
                    ", joinStrategy == "+joinStrategy+
                    ", lockMode == "+lockMode+
                    ", optimizer level == "+optimizer.getLevel();
        }else{
            return "";
        }
    }

    @Override
    public void initializeAccessPathName(DataDictionary dd,TableDescriptor td) throws StandardException{
        if(cd!=null && cd.isConstraint()){
            ConstraintDescriptor constraintDesc= dd.getConstraintDescriptor(td,cd.getUUID());
            if(constraintDesc==null){
                throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND,"CONSTRAINT on TABLE",td.getName());
            }
        }
    }

    @Override
    public void setHintedJoinStrategy(boolean isHintedJoinStrategy){
        this.isJoinStrategyHinted = isHintedJoinStrategy;
    }

    @Override
    public boolean isHintedJoinStrategy(){
        return isJoinStrategyHinted;
    }
}
