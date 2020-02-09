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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;

import java.util.ArrayList;
import java.util.List;

public class RowOrderingImpl implements RowOrdering{

    /* This vector contains ColumnOrderings */
    List<ColumnOrdering> ordering;

    ColumnOrdering currentColumnOrdering;

    /* This vector contains unordered Optimizables */
    List<Optimizable> unorderedOptimizables;

    public RowOrderingImpl(){
        //noinspection Convert2Diamond
        ordering=new ArrayList<ColumnOrdering>();
        //noinspection Convert2Diamond
        unorderedOptimizables=new ArrayList<Optimizable>();
    }

    @Override
    public Iterable<ColumnOrdering> orderedColumns(){
        return ordering;
    }

    @Override
    public ColumnOrdering ordering(int orderPosition,int tableNumber,int columnNumber) throws StandardException{
        return null;
    }

    @Override
    public int orderedOnColumn(int direction,int orderPosition,
                                   int tableNumber,int columnNumber) throws StandardException{

		/*
		** Return false if we're looking for an ordering position that isn't
		** in this ordering.
		*/
        while (orderPosition < ordering.size()) {

            ColumnOrdering co = ordering.get(orderPosition);

		    /*
		    ** Is the column in question ordered with the given direction at
		    ** this position?
		    */
            Boolean isOrdered = co.ordered(direction, tableNumber, columnNumber);
            if (isOrdered)
                return orderPosition;

            /* if this column has constant value, we can treat it as sorted and skip */
            if (!co.getBoundByConstant())
                return -1;

            orderPosition ++;
        }
        return -1;
    }

    @Override
    public boolean orderedOnColumn(int direction, int tableNumber, int columnNumber) throws StandardException{
        boolean ordered=false;

        for(ColumnOrdering co : ordering){
            /*
            ** Is the column in question ordered with the given direction at
			** this position?
			*/
            boolean thisOrdered=co.ordered(direction, tableNumber, columnNumber);

            if(thisOrdered){
                ordered=true;
                break;
            }
        }

        return ordered;
    }

    @Override
    public int orderedPositionForColumn(int direction, int tableNumber, int columnNumber) throws StandardException{
        boolean ordered=false;

        int p = 0;
        for(ColumnOrdering co : ordering){
            /*
            ** Is the column in question ordered with the given direction at
			** this position?
			*/
            boolean thisOrdered=co.ordered(direction, tableNumber, columnNumber);

            if(thisOrdered){
                return p;
            }
            p++;
        }

        return -1;
    }

    @Override
    public int addOrderedColumn(int direction, int tableNumber, int columnNumber){
        if(!unorderedOptimizables.isEmpty())
            return -1;

        ColumnOrdering currentColumnOrdering;

        if(ordering.isEmpty()){
            currentColumnOrdering=new ColumnOrdering(direction);
            ordering.add(currentColumnOrdering);
        }else{
            currentColumnOrdering= ordering.get(ordering.size()-1);
        }

        if(SanityManager.DEBUG){
            if(currentColumnOrdering.direction()!=direction){
                SanityManager.THROWASSERT("direction == "+direction+
                        ", currentColumnOrdering.direction() == "+
                        currentColumnOrdering.direction());
            }
        }

        currentColumnOrdering.addColumn(tableNumber,columnNumber);
        return ordering.size() - 1;
    }

    @Override
    public void nextOrderPosition(int direction){
        if(!unorderedOptimizables.isEmpty())
            return;

        currentColumnOrdering=new ColumnOrdering(direction);
        ordering.add(currentColumnOrdering);
    }

    @Override
    public void removeOptimizable(int tableNumber){
		/*
		** Walk the list backwards, so we can remove elements
		** by position.
		*/
        for(int i=ordering.size()-1;i>=0;i--){
			/*
			** First, remove the table from all the ColumnOrderings
			*/
            ColumnOrdering ord=ordering.get(i);
            ord.removeColumns(tableNumber);
            if(ord.empty())
                ordering.remove(i);
        }

		/* Also remove from list of unordered optimizables */
        removeOptimizableFromVector(tableNumber,unorderedOptimizables);

    }

    @Override
    public void addUnorderedOptimizable(Optimizable optimizable){
        unorderedOptimizables.add(optimizable);
    }

    @Override
    public void copy(RowOrdering copyTo){
        assert copyTo instanceof RowOrderingImpl : "copyTo should be a RowOrderingImpl, is a "+ copyTo.getClass();

        RowOrderingImpl dest=(RowOrderingImpl)copyTo;

		/* Clear the ordering of what we're copying to */
        dest.ordering.clear();
        dest.currentColumnOrdering=null;

        for(int i=0;i<ordering.size();i++){
            ColumnOrdering co=ordering.get(i);

            dest.ordering.add(co.cloneMe());

            if(co==currentColumnOrdering)
                dest.rememberCurrentColumnOrdering(i);
        }
        dest.unorderedOptimizables.clear();
        dest.unorderedOptimizables.addAll(unorderedOptimizables);
    }

    @Override
    public RowOrdering getClone(){
        RowOrdering ordering = new RowOrderingImpl();
        copy(ordering);
        return ordering;
    }

    @Override
    public String toString(){
        StringBuilder retval= null;

        if(SanityManager.DEBUG){
            retval = new StringBuilder("Unordered optimizables: ");
            for(Optimizable opt : unorderedOptimizables){
                if(opt.getBaseTableName()!=null){
                    retval.append(opt.getBaseTableName());
                }else{
                    retval.append(opt.toString());
                }
                retval.append(" ");
            }
            retval.append("\n");

            for(int i=0;i<ordering.size();i++){
                retval.append(" ColumnOrdering ").append(i).append(": ").append(ordering.get(i));
            }
            return retval.toString();
        }

        return null;
    }

    /**
     * Return true if the given vector of Optimizables contains an Optimizable
     * with the given table number.
     */
    private boolean vectorContainsOptimizable(int tableNumber,List<Optimizable> vec){
        int i;

        for(i=vec.size()-1;i>=0;i--){
            Optimizable optTable=vec.get(i);

            if(optTable.hasTableNumber()){
                if(optTable.getTableNumber()==tableNumber){
                    return true;
                }
            }
        }
        return false;
    }

    private int optimizablePosition(int tableNumber,List<Optimizable> vec){
        int i;

        for(i=vec.size()-1;i>=0;i--){
            Optimizable optTable=vec.get(i);

            if(optTable.hasTableNumber()){
                if(optTable.getTableNumber()==tableNumber){
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Remove all optimizables with the given table number from the
     * given vector of optimizables.
     */
    private void removeOptimizableFromVector(int tableNumber,List<Optimizable> vec){
        int i;

        for(i=vec.size()-1;i>=0;i--){
            Optimizable optTable=vec.get(i);

            if(optTable.hasTableNumber()){
                if(optTable.getTableNumber()==tableNumber){
                    vec.remove(i);
                }
            }
        }
    }

    private void rememberCurrentColumnOrdering(int posn){
        currentColumnOrdering=ordering.get(posn);
    }

    /**
     * Returns true if there are unordered optimizables in the join order
     * other than the given one.
     */
    private boolean unorderedOptimizablesOtherThan(Optimizable optimizable){
        for(Optimizable thisOpt : unorderedOptimizables){
            if(thisOpt!=optimizable)
                return true;
        }

        return false;
    }

    public ColumnOrdering getOrderedColumn(int pos) {
        if (pos >= ordering.size())
            return null;

        return ordering.get(pos);
    }

    @Override
    public void mergeTo(RowOrdering target) {
        assert target instanceof RowOrderingImpl : "mergeTo should be a RowOrderingImpl, is a "+ target.getClass();

        RowOrderingImpl dest=(RowOrderingImpl)target;
        if (dest.unorderedOptimizables.size()>0)
            return;

        for(int i=0;i<ordering.size();i++){
            ColumnOrdering co=ordering.get(i);

            dest.ordering.add(co.cloneMe());

            if(co==currentColumnOrdering)
                dest.rememberCurrentColumnOrdering(i);
        }
        dest.unorderedOptimizables.addAll(unorderedOptimizables);
        return;
    }

    @Override
    public void removeAllOptimizables() {
        for(int i=ordering.size()-1;i>=0;i--){
            ordering.remove(i);
        }
        for(int i=unorderedOptimizables.size()-1;i>=0;i--){
            unorderedOptimizables.remove(i);
        }
    }

}
