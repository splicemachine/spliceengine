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

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * A SubqueryList represents a list of subquerys within a specific clause 
 * (select, where or having) in a DML statement.  It extends QueryTreeNodeVector.
 *
 */

public class SubqueryList extends QueryTreeNodeVector<SubqueryNode>{
	/**
	 * Add a subquery to the list.
	 *
	 * @param subqueryNode	A SubqueryNode to add to the list
	 *
	 */

	public void addSubqueryNode(SubqueryNode subqueryNode) throws StandardException {
		addElement(subqueryNode);
	}

	/**
	 * Preprocess a SubqueryList.  For now, we just preprocess each SubqueryNode
	 * in the list.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void preprocess(int numTables,
                           FromList outerFromList,
                           SubqueryList outerSubqueryList,
                           PredicateList outerPredicateList)  throws StandardException {
		SubqueryNode subqueryNode;

		int size = size();
		for (int index = 0; index < size; index++) {
			subqueryNode = elementAt(index);
			subqueryNode.preprocess(numTables, outerFromList,
									outerSubqueryList,
									outerPredicateList);
		}
	}

	/**
	 * Optimize the subqueries in this list.  
	 *
	 * @param dataDictionary	The data dictionary to use for optimization
	 * @param outerRows			The optimizer's estimate of the number of
	 *							times this subquery will be executed.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void optimize(DataDictionary dataDictionary, double outerRows) throws StandardException {
		int size = size();
		for (int index = 0; index < size; index++) {
			SubqueryNode	subqueryNode;
			subqueryNode = elementAt(index);
			subqueryNode.optimize(dataDictionary, outerRows);
		}
	}

	/**
	 * Modify the access paths for all subqueries in this list.
	 *
	 * @see ResultSetNode#modifyAccessPaths
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void modifyAccessPaths() throws StandardException {
		int size = size();
		for (int index = 0; index < size; index++) {
			SubqueryNode	subqueryNode;
			subqueryNode = elementAt(index);
			subqueryNode.modifyAccessPaths();
		}
	}

	/**
	 * Search to see if a query references the specifed table name.
	 *
	 * @param name		Table name (String) to search for.
	 * @param baseTable	Whether or not name is for a base table
	 *
	 * @return	true if found, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesTarget(String name, boolean baseTable) throws StandardException {
		int size = size();
		for (int index = 0; index < size; index++) {
			SubqueryNode	subqueryNode;

			subqueryNode = elementAt(index);
			if (subqueryNode.isMaterializable()) {
				continue;
			}

			if (subqueryNode.getResultSet().referencesTarget(name, baseTable)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean referencesSessionSchema() throws StandardException {
		int size = size();
		for (int index = 0; index < size; index++) {
			SubqueryNode	subqueryNode;

			subqueryNode = elementAt(index);

			if (subqueryNode.getResultSet().referencesSessionSchema()) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Set the point of attachment in all subqueries in this list.
	 *
	 * @param pointOfAttachment		The point of attachment
	 *
	 * @exception StandardException			Thrown on error
	 */
	public void setPointOfAttachment(int pointOfAttachment) throws StandardException {
		int size = size();

		for (int index = 0; index < size; index++) {
			SubqueryNode	subqueryNode;

			subqueryNode = elementAt(index);
			subqueryNode.setPointOfAttachment(pointOfAttachment);
		}
	}

	/**
	 * Decrement (query block) level (0-based) for 
	 * all of the tables in this subquery list.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
	void decrementLevel(int decrement) {
		int size = size();

		for (int index = 0; index < size; index++) {
			elementAt(index).getResultSet().decrementLevel(decrement);
		}
	}

	/**
     * Mark all of the subqueries in this 
     * list as being part of a having clause,
     * so we can avoid flattenning later.
	 * 
	 */
	public void markHavingSubqueries() {
	    int size = size();
	    
	    for (int index = 0; index < size; index++) {
	        SubqueryNode    subqueryNode;

	        subqueryNode = elementAt(index);
	        subqueryNode.setHavingSubquery(true);
	    }
	}

	/**
	 * Mark all of the subqueries in this list as being part of a where clause
	 * so we can avoid flattening later if needed.
	 */
	public void markWhereSubqueries() {
		int size = size();
		for (int index = 0; index < size; index++) {
			SubqueryNode    subqueryNode;

			subqueryNode = elementAt(index);
			subqueryNode.setWhereSubquery(true);
		}
	}
}

