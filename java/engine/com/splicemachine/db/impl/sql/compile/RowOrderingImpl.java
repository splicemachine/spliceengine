/*

   Derby - Class org.apache.derby.impl.sql.compile.RowOrderingImpl

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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RowOrderingImpl implements RowOrdering{

    /* This vector contains ColumnOrderings */
    List<ColumnOrdering> ordering;

    /*
    ** This ColumnOrdering represents the columns that can be considered
    ** ordered no matter what.  For example, columns that are compared to
    ** constants with = are always ordered.  Also, all columns in a one-row
    ** result set are ordered. Another instance of always ordered is when
    ** the column is involved in an equijoin with an optimizable which is
    ** always ordered on the column on which the equijoin is happening.
    */
    ColumnOrdering columnsAlwaysOrdered;

    /*
    ** This vector contains table numbers for tables that are always ordered.
    ** This happens for one-row tables.
    */
    List<Optimizable> alwaysOrderedOptimizables;

    ColumnOrdering currentColumnOrdering;

    /* This vector contains unordered Optimizables */
    List<Optimizable> unorderedOptimizables;

    public RowOrderingImpl(){
        //noinspection Convert2Diamond
        ordering=new ArrayList<ColumnOrdering>();
        //noinspection Convert2Diamond
        unorderedOptimizables=new ArrayList<Optimizable>();
        columnsAlwaysOrdered=new ColumnOrdering(RowOrdering.DONTCARE);
        //noinspection Convert2Diamond
        alwaysOrderedOptimizables=new ArrayList<Optimizable>();
    }

    /**
     * @see RowOrdering#isColumnAlwaysOrdered
     */
    public boolean isColumnAlwaysOrdered(int tableNumber,int columnNumber){
        return (columnsAlwaysOrdered.contains(tableNumber,columnNumber));
    }

    /**
     * @throws StandardException Thrown on error
     * @see RowOrdering#orderedOnColumn
     */
    public boolean orderedOnColumn(int direction,
                                   int orderPosition,
                                   int tableNumber,
                                   int columnNumber) throws StandardException{

		/*
        ** Return true if the table is always ordered.
		*/
        if(vectorContainsOptimizable(tableNumber,alwaysOrderedOptimizables)){
            return true;
        }

		/*
		** Return true if the column is always ordered.
		*/
        if(columnsAlwaysOrdered.contains(tableNumber,columnNumber)){
            return true;
        }

		/*
		** Return false if we're looking for an ordering position that isn't
		** in this ordering.
		*/
        if(orderPosition>=ordering.size())
            return false;

        ColumnOrdering co=ordering.get(orderPosition);

		/*
		** Is the column in question ordered with the given direction at
		** this position?
		*/
        return co.ordered(direction,tableNumber,columnNumber);
    }

    @Override
    public boolean orderedOnColumn(int direction,
                                   int tableNumber,
                                   int columnNumber) throws StandardException{
		/*
		** Return true if the table is always ordered.
		*/
        if(vectorContainsOptimizable(tableNumber,alwaysOrderedOptimizables)){
            return true;
        }

		/*
		** Return true if the column is always ordered.
		*/
        if(columnsAlwaysOrdered.contains(tableNumber,columnNumber)){
            return true;
        }

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
    public void addOrderedColumn(int direction, int tableNumber, int columnNumber){
        if(unorderedOptimizables.size()>0)
            return;

        ColumnOrdering currentColumnOrdering;

        if(ordering.size()==0){
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
    }

    @Override
    public void nextOrderPosition(int direction){
        if(unorderedOptimizables.size()>0)
            return;

        currentColumnOrdering=new ColumnOrdering(direction);
        ordering.add(currentColumnOrdering);
    }

    @Override
    public void optimizableAlwaysOrdered(Optimizable optimizable){
        // A table can't be ordered if there is an outer unordered table
        if(unorderedOptimizablesOtherThan(optimizable)){
            return;
        }

		/*
		** A table is not "always ordered" if any of the other ordered tables
		** in the join order are not also "always ordered".  In other words,
		** if any outer table is not a one-row table, this table is not
		** always ordered.
		**
		** The table that was passed in as a parameter may have already been
		** added as a table with ordered columns.  If it is the first table
		** in the list of ordered columns, then there should be no other
		** tables in this list, so we remove it from the list and add it
		** to the list of always-ordered tables.
		*/
        boolean hasTableNumber=optimizable.hasTableNumber();
        int tableNumber=(hasTableNumber?optimizable.getTableNumber():0);
        if(((ordering.size()==0) ||(hasTableNumber && ordering.get(0).hasTable(tableNumber)))
                && (hasTableNumber && !columnsAlwaysOrdered.hasAnyOtherTable(tableNumber))){
            if(optimizable.hasTableNumber())
                removeOptimizable(optimizable.getTableNumber());

            alwaysOrderedOptimizables.add(optimizable);
        }
    }

    /**
     * @see RowOrdering#columnAlwaysOrdered
     */
    public void columnAlwaysOrdered(Optimizable optimizable,int columnNumber){
        columnsAlwaysOrdered.addColumn(optimizable.getTableNumber(),
                columnNumber);
    }

    /**
     * @see RowOrdering#alwaysOrdered
     */
    public boolean alwaysOrdered(int tableNumber){
        return vectorContainsOptimizable(tableNumber, alwaysOrderedOptimizables );
    }

    /**
     * @see RowOrdering#removeOptimizable
     */
    public void removeOptimizable(int tableNumber){
        int i;

		/*
		** Walk the list backwards, so we can remove elements
		** by position.
		*/
        for(i=ordering.size()-1;i>=0;i--){
			/*
			** First, remove the table from all the ColumnOrderings
			*/
            ColumnOrdering ord=ordering.get(i);
            ord.removeColumns(tableNumber);
            if(ord.empty())
                ordering.remove(i);
        }

		/* Remove from list of always-ordered columns */
        columnsAlwaysOrdered.removeColumns(tableNumber);

		/* Also remove from list of unordered optimizables */
        removeOptimizableFromVector(tableNumber,unorderedOptimizables);

		/* Also remove from list of always ordered optimizables */
        removeOptimizableFromVector(tableNumber,alwaysOrderedOptimizables);
    }

    /**
     * @see RowOrdering#addUnorderedOptimizable
     */
    public void addUnorderedOptimizable(Optimizable optimizable){
        unorderedOptimizables.add(optimizable);
    }

    /**
     * @see RowOrdering#copy
     */
    public void copy(RowOrdering copyTo){
        assert copyTo instanceof RowOrderingImpl : "copyTo should be a RowOrderingImpl, is a "+ copyTo.getClass();

        RowOrderingImpl dest=(RowOrderingImpl)copyTo;

		/* Clear the ordering of what we're copying to */
        dest.ordering.clear();
        dest.currentColumnOrdering=null;

        dest.unorderedOptimizables.clear();
        Collections.copy(unorderedOptimizables,dest.unorderedOptimizables);
//        for(int i=0;i<unorderedOptimizables.size();i++){
//            dest.unorderedOptimizables.add(unorderedOptimizables.get(i));
//        }

        dest.alwaysOrderedOptimizables.clear();
        Collections.copy(alwaysOrderedOptimizables,dest.alwaysOrderedOptimizables);
//        for(int i=0;i<alwaysOrderedOptimizables.size();i++){
//            dest.alwaysOrderedOptimizables.add(alwaysOrderedOptimizables.get(i));
//        }

        for(int i=0;i<ordering.size();i++){
            ColumnOrdering co=ordering.get(i);

            dest.ordering.add(co.cloneMe());

            if(co==currentColumnOrdering)
                dest.rememberCurrentColumnOrdering(i);
        }

        dest.columnsAlwaysOrdered=null;
        if(columnsAlwaysOrdered!=null)
            dest.columnsAlwaysOrdered=columnsAlwaysOrdered.cloneMe();
    }

    @Override
    public String toString(){
        String retval=null;

        if(SanityManager.DEBUG){
            retval="Unordered optimizables: ";
            for(Optimizable opt : unorderedOptimizables){
                if(opt.getBaseTableName()!=null){
                    retval+=opt.getBaseTableName();
                }else{
                    retval+=opt.toString();
                }
                retval+=" ";
            }
            retval+="\n";

            retval+="\nAlways ordered optimizables: ";

            for(Optimizable opt : alwaysOrderedOptimizables){
                if(opt.getBaseTableName()!=null){
                    retval+=opt.getBaseTableName();
                }else{
                    retval+=opt.toString();
                }
                retval+=" ";
            }
            retval+="\n";

            for(int i=0;i<ordering.size();i++){
                retval+=" ColumnOrdering "+i+": "+ordering.get(i);
            }
        }

        return retval;
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
}
