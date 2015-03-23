/*

   Derby - Class org.apache.derby.impl.sql.compile.AccessPathImpl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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
}
