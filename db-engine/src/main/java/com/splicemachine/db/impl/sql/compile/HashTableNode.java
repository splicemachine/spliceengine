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

import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.reference.ClassName;


import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;

import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;

/**
 * A HashTableNode represents a result set where a hash table is built.
 *
 */

public class HashTableNode extends SingleChildResultSetNode
{
	PredicateList	searchPredicateList;
	PredicateList	joinPredicateList;

	SubqueryList	pSubqueryList;
	SubqueryList	rSubqueryList;

	/**
	 * Initializer for a HashTableNode.
	 *
	 * @param childResult			The child result set
	 * @param tableProperties	Properties list associated with the table
	 * @param resultColumns			The RCL.
	 * @param searchPredicateList	Single table clauses
	 * @param joinPredicateList		Multi table clauses
	 * @param accessPath			The access path
	 * @param costEstimate			The cost estimate
	 * @param pSubqueryList			List of subqueries in RCL
	 * @param rSubqueryList			List of subqueries in Predicate lists
	 * @param hashKeyColumns		Hash key columns
	 */

	public void init(
						 Object childResult,
						 Object tableProperties,
						 Object resultColumns,
						 Object searchPredicateList,
						 Object joinPredicateList,
						 Object accessPath,
						 Object   costEstimate,
						 Object	pSubqueryList,
						 Object   rSubqueryList,
						 Object hashKeyColumns)
	{
		super.init(childResult, tableProperties);
		this.resultColumns = (ResultColumnList) resultColumns;
		this.searchPredicateList = (PredicateList) searchPredicateList;
		this.joinPredicateList = (PredicateList) joinPredicateList;
		this.trulyTheBestAccessPath = (AccessPathImpl) accessPath;
		this.costEstimate = (CostEstimate) costEstimate;
		this.pSubqueryList = (SubqueryList) pSubqueryList;
		this.rSubqueryList = (SubqueryList) rSubqueryList;
		setHashKeyColumns((int[]) hashKeyColumns);
	}

	/*
	 *  Optimizable interface
	 */

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Optimizable modifyAccessPath(JBitSet outerTables, Optimizer optimizer) 
		throws StandardException
	{
		return this;
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (searchPredicateList != null)
			{
				printLabel(depth, "searchPredicateList: ");
				searchPredicateList.treePrint(depth + 1);
			}

			if (joinPredicateList != null)
			{
				printLabel(depth, "joinPredicateList: ");
				joinPredicateList.treePrint(depth + 1);
			}
		}
	}

    /**
     * For joins, the tree will be (nodes are left out if the clauses
     * are empty):
     *
     *      ProjectRestrictResultSet -- for the having and the select list
     *      SortResultSet -- for the group by list
     *      ProjectRestrictResultSet -- for the where and the select list (if no group or having)
     *      the result set for the fromList
     *
	 *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		if (SanityManager.DEBUG)
        SanityManager.ASSERT(resultColumns != null, "Tree structure bad");

        //
        // If we are projecting and restricting the stream from a table
        // function, then give the table function all of the information that
        // it needs in order to push the projection and qualifiers into
        // the table function. See DERBY-4357.
        //
        if ( childResult instanceof FromVTI )
        {
            ((FromVTI) childResult).computeProjectionAndRestriction( searchPredicateList );
        }

		generateMinion( acb, mb, false);
	}

	/**
	 * General logic shared by Core compilation and by the Replication Filter
	 * compiler. A couple ResultSets (the ones used by PREPARE SELECT FILTER)
	 * implement this method.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb the method  the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateResultSet(ExpressionClassBuilder acb,
										   MethodBuilder mb)
									throws StandardException
	{
		generateMinion( acb, mb, true);
	}

	/**
	 * Logic shared by generate() and generateResultSet().
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb the method  the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	private void generateMinion(ExpressionClassBuilder acb,
									 MethodBuilder mb, boolean genChildResultSet)
									throws StandardException
	{
		MethodBuilder	userExprFun;
		ValueNode	searchClause = null;
		ValueNode	equijoinClause = null;


		/* The tableProperties, if non-null, must be correct to get this far.
		 * We simply call verifyProperties to set initialCapacity and
		 * loadFactor.
		 */
		verifyProperties(getDataDictionary());

		// build up the tree.

		/* Put the predicates back into the tree */
		if (searchPredicateList != null)
		{
			// Remove any redundant predicates before restoring
			searchPredicateList.removeRedundantPredicates();
			searchClause = searchPredicateList.restorePredicates();
			/* Allow the searchPredicateList to get garbage collected now
			 * that we're done with it.
			 */
			searchPredicateList = null;
		}

		// for the single table predicates, we generate an exprFun
		// that evaluates the expression of the clause
		// against the current row of the child's result.
		// if the restriction is empty, simply pass null
		// to optimize for run time performance.

   		// generate the function and initializer:
   		// Note: Boolean lets us return nulls (boolean would not)
   		// private Boolean exprN()
   		// {
   		//   return <<searchClause.generate(ps)>>;
   		// }
   		// static Method exprN = method pointer to exprN;





		// Map the result columns to the source columns
        ResultColumnList.ColumnMapping  mappingArrays =
            resultColumns.mapSourceColumns();

        int[] mapArray = mappingArrays.mapArray;

		int mapArrayItem = acb.addItem(new ReferencedColumnsDescriptorImpl(mapArray));

		// Save the hash key columns 

		FormatableIntHolder[] fihArray = 
				FormatableIntHolder.getFormatableIntHolders(hashKeyColumns()); 
		FormatableArrayHolder hashKeyHolder = new FormatableArrayHolder(fihArray);
		int hashKeyItem = acb.addItem(hashKeyHolder);

		/* Generate the HashTableResultSet:
		 *	arg1: childExpress - Expression for childResultSet
		 *  arg2: searchExpress - Expression for single table predicates
		 *	arg3	: equijoinExpress - Qualifier[] for hash table look up
		 *  arg4: projectExpress - Expression for projection, if any
		 *  arg5: resultSetNumber
		 *  arg6: mapArrayItem - item # for mapping of source columns
		 *  arg7: reuseResult - whether or not the result row can be reused
		 *						(ie, will it always be the same)
		 *	arg8: hashKeyItem - item # for int[] of hash column #s
		 *	arg9: removeDuplicates - don't remove duplicates in hash table (for now)
		 *	arg10: maxInMemoryRowCount - max row size for in-memory hash table
		 *	arg11: initialCapacity - initialCapacity for java.util.Hashtable
		 *	arg12	: loadFactor - loadFactor for java.util.Hashtable
		 *  arg13: estimated row count
		 *  arg14: estimated cost
		 *  arg15: close method
		 */

		acb.pushGetResultSetFactoryExpression(mb);

		if (genChildResultSet)
			childResult.generateResultSet(acb, mb);
		else
			childResult.generate((ActivationClassBuilder) acb, mb);

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// Get the final cost estimate based on child's cost.
		costEstimate = childResult.getFinalCostEstimate();

		// if there is no searchClause, we just want to pass null.
		if (searchClause == null)
		{
		   	mb.pushNull(ClassName.GeneratedMethod);
		}
		else
		{
			// this sets up the method and the static field.
			// generates:
			// 	DataValueDescriptor userExprFun { }
			userExprFun = acb.newUserExprFun();

			// searchClause knows it is returning its value;

			/* generates:
			 *    return <searchClause.generate(acb)>;
			 * and adds it to userExprFun
			 * NOTE: The explicit cast to DataValueDescriptor is required
			 * since the searchClause may simply be a boolean column or subquery
			 * which returns a boolean.  For example:
			 *		where booleanColumn
			 */

			searchClause.generateExpression(acb, userExprFun);
			userExprFun.methodReturn();


			/* PUSHCOMPILER
			userSB.newReturnStatement(searchClause.generateExpression(acb, userSB));
			*/

			// we are done modifying userExprFun, complete it.
			userExprFun.complete();

	   		// searchClause is used in the final result set as an access of the new static
   			// field holding a reference to this new method.
			// generates:
			//	ActivationClass.userExprFun
			// which is the static field that "points" to the userExprFun
			// that evaluates the where clause.
   			acb.pushMethodReference(mb, userExprFun);
		}
		/* Generate the qualifiers for the look up into
		 * the hash table.
		 */
		joinPredicateList.generateQualifiers(acb, mb, (Optimizable) childResult,
														false);

		/* Determine whether or not reflection is needed for the projection.
		 * Reflection is not needed if all of the columns map directly to source
		 * columns.
		 */
		if (reflectionNeededForProjection())
		{
			// for the resultColumns, we generate a userExprFun
			// that creates a new row from expressions against
			// the current row of the child's result.
			// (Generate optimization: see if we can simply
			// return the current row -- we could, but don't, optimize
			// the function call out and have execution understand
			// that a null function pointer means take the current row
			// as-is, with the performance trade-off as discussed above.)

			/* Generate the Row function for the projection */
			resultColumns.generateCore(acb, mb, false);
		}
		else
		{
		   	mb.pushNull(ClassName.GeneratedMethod);
		}

		mb.push(resultSetNumber);
		mb.push(mapArrayItem);
		mb.push(resultColumns.reusableResult());
		mb.push(hashKeyItem);
		mb.push(false);
		mb.push(-1L);
		mb.push(costEstimate.singleScanRowCount());
		mb.push(costEstimate.getEstimatedCost());

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getHashTableResultSet",
                ClassName.NoPutResultSet, 14);
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 *
     * @param v the visitor
     */
    @Override
	public void acceptChildren(Visitor v) throws StandardException {
		super.acceptChildren(v);

		if (searchPredicateList != null)
		{
			searchPredicateList = (PredicateList)searchPredicateList.accept(v, this);
		}

		if (joinPredicateList != null)
		{
			joinPredicateList = (PredicateList)joinPredicateList.accept(v, this);
		}
	}

    public void assignResultSetNumber() throws StandardException {
        super.assignResultSetNumber();
        /* Set the point of attachment in all subqueries attached
         * to this node.
		 */
        if (pSubqueryList != null && pSubqueryList.size() > 0) {
            pSubqueryList.setPointOfAttachment(resultSetNumber);
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(pSubqueryList.size() == 0,
                        "pSubqueryList.size() expected to be 0");
            }
        }
        if (rSubqueryList != null && rSubqueryList.size() > 0) {
            rSubqueryList.setPointOfAttachment(resultSetNumber);
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(rSubqueryList.size() == 0,
                        "rSubqueryList.size() expected to be 0");
            }
        }
    }
}
