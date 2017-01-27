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

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

import com.splicemachine.db.iapi.error.StandardException;

/**
  <p>
  A structure which is used to "qualify" a column.  Specifies
  that the column value in a given column identified by column
  id is to be compared via a specific operator to a particular
  DataValueDescriptor value.
  <p>
  The implementation of this interface is provided by the client; 
  the methods of Qualifier are the methods the access code uses to use it.
  <p>
  Arrays of qualifiers are provided to restrict the rows 
  returned by scans.  A row is returned from a scan if all qualifications
  in the array return true.  
  <p>
  A qualification returns true if in the following pseudo-code compare_result
  is true.
  <p>
  <blockquote><pre>
  if (qualifier.negateCompareResult())
  {
      compare_result = 
      row[(qualifier.getColumnId())].compare(
        qualifier.getOperator(), 
        qualifier.getOrderable(),
        qualifier.getOrderedNulls(), 
        qualifier.getUnknownRV()) 
      if (qualifier.negateCompareResult())
      {
          compare_result = !(compare_result);
      }
  }
  </blockquote></pre>
  <p>
  Qualifiers are often passed through interfaces as a set of Qualifiers,
  rather than one at a time, for example see the qualifier argument in 
  TransactionController.openScan(). 
  <p>
  To make this consistent the following protocols are to be used when passing
  around sets of Qualifiers.
  <p>
  A single dimensional array is to be used to pass around a set of AND'd 
  qualifiers.  Thus qualifier[] argument is to be treated as:
  <blockquote><pre>
      qualifier[0] AND qualifer[1] ... AND qualifier[qualifer.length - 1]
  </blockquote></pre>
  <p>
  A two dimensional array is to be used to pass around a AND's and OR's in
  conjunctive normal form.  The top slot of the 2 dimensional array is optimized
  for the more frequent where no OR's are present.  The first array slot is 
  always a list of AND's to be treated as described above for single dimensional
  AND qualifier arrays.  The subsequent slots are to be treated as AND'd arrays
  of OR's.  Thus the 2 dimensional array qual[][] argument is to be treated as 
  the following, note if qual.length = 1 then only the first array is valid and
  it is and an array of AND clauses:
  <blockquote><pre>
  (qual[0][0] AND qual[0][0] ... AND qual[0][qual[0].length - 1])
  AND
  (qual[1][0] OR  qual[1][1] ... OR  qual[1][qual[1].length - 1])
  AND
  (qual[2][0] OR  qual[2][1] ... OR  qual[2][qual[2].length - 1])
  ...
  AND (qual[qual.length - 1][0] OR  qual[1][1] ... OR  qual[1][2])
  </blockquote></pre>
  <p>
  If any of the array's qual[0].length ... qual[qual.length -1] are 0 length
  they will be evaluated as TRUE; but they must be not NULL.  See Example 4 for
  encoding of (a or b) that takes advantage of this.
  <p>
  Note that any of the arrays qual[0].length ... qual[qual.length -1] may also
  be of length 1, thus no guarantee is made the presence of OR
  predicates if qual.length > 1. See example 1a.
  <p>
  The following give pseudo-code examples of building Qualifier arrays:
  <p>
  Example 1: "a AND b AND c"
  <blockquote><pre>
    qualifier = new Qualifier[1][3]; // 3 AND clauses

    qualifier[0][0] = a
    qualifier[0][1] = b
    qualifier[0][2] = c
  </blockquote></pre>
  <p>
  Example 1a "a AND b AND c" - less efficient than example 1 but legal
  <blockquote><pre>
    qualifier = new Qualifier[3]; // 3 AND clauses
    qualifier[0] = new Qualifier[1];
    qualifier[1] = new Qualifier[1];
    qualifier[2] = new Qualifier[1];
	
    qualifier[0][0] = a
    qualifier[1][0] = b
    qualifier[2][0] = c
  </blockquote></pre>
  <p>
  Example 2: "(f) AND (a OR b) AND (c OR d OR e)"
    Would be represented by an array that looks like the following:
  <blockquote><pre>
    qualifier = new Qualifier[3]; // 3 and clauses
    qualifier[0] = new Qualifier[1]; // to be intitialized to f
    qualifier[1] = new Qualifier[2]; // to be initialized to (a OR b)
    qualifier[2] = new Qualifier[3]; // to be initialized to (c OR d OR e)

    qualifier[0][0] = f
    qualifier[1][0] = a
    qualifier[1][1] = b
    qualifier[2][0] = c
    qualifier[2][1] = d
    qualifier[2][2] = e
  </blockquote></pre>
  <p>
  Example 3: "(a OR b) AND (c OR d) AND (e OR f)" 
  <blockquote><pre>
    qualifier = new Qualifier[3]; // 3 and clauses
    qualifier = new Qualifier[4]; // 4 and clauses
    qualifier[0] = new Qualifier[1]; // to be intitialized to TRUE
    qualifier[1] = new Qualifier[2]; // to be initialized to (a OR b)
    qualifier[2] = new Qualifier[2]; // to be initialized to (c OR d)
    qualifier[3] = new Qualifier[2]; // to be initialized to (e OR f)

    qualifier[0][0] = TRUE
    qualifier[1][0] = a
    qualifier[1][1] = b
    qualifier[2][0] = c
    qualifier[2][1] = d
    qualifier[3][0] = e
    qualifier[3][1] = f
  </blockquote></pre>
  <p>
  Example 4: "(a OR b)" 
  <blockquote><pre>
    qualifier = new Qualifier[2]; // 2 and clauses
    qualifier[0] = new Qualifier[0]; // 0 length array is TRUE
    qualifier[1] = new Qualifier[2]; // to be initialized to (a OR b)

    qualifier[1][0] = a
    qualifier[1][1] = b
  </blockquote></pre>

  @see ScanController
  @see TransactionController#openScan 
  @see DataValueDescriptor#compare
**/


public interface Qualifier
{

	/**	
	 * The DataValueDescriptor can be 1 of 4 types:<ul>
	 *		<li> VARIANT		- cannot be cached as its value can vary 
	 *							  within a scan</li>
	 *		<li> SCAN_INVARIANT - can be cached within a scan as its value 
	 *							  will not change within a scan </li>
	 *		<li> QUERY_INVARIANT- can be cached across the life of the query
	 *								as its value will never change </li>
	 *		<li> CONSTANT 		- can be cached across executions. </li></ul>
	 * <p>
	 * <b>NOTE</b>: the following is guaranteed: <i> 
	 *		VARIANT < SCAN_INVARIANT < QUERY_INVARIANT < CONSTANT
	 */
	public static final int VARIANT = 0;
	public static final int SCAN_INVARIANT = 1;
	public static final int QUERY_INVARIANT = 2;
	public static final int CONSTANT = 3;

	/** 
     * Get the (zero based) id of the column to be qualified.
     * <p>
     * This id is the column number of the column in the table, no matter 
     * whether a partial column set is being retrieved by the actual fetch.
     * Note that the column being specified in the qualifier must appear in
     * the column list being fetched.
     **/
	int getColumnId();

    int getStoragePosition();

	/**
	 * Get the value that the column is to be compared to.
	 *
	 * @exception StandardException		Thrown on error
	 */
	DataValueDescriptor getOrderable() throws StandardException;

	/** Get the operator to use in the comparison. 
     *
     *  @see DataValueDescriptor#compare
     **/
	int getOperator();

	/** Determine if the result from the compare operation should be negated.  
     *  If true then only rows which fail the compare operation will qualify.
     *
     *  @see DataValueDescriptor#compare
     **/
	boolean negateCompareResult();

	/** Get the getOrderedNulls argument to use in the comparison. 
     *  
     *  @see DataValueDescriptor#compare
     **/
    boolean getOrderedNulls();

	/** Get the getOrderedNulls argument to use in the comparison.
     *  
     *  @see DataValueDescriptor#compare
     **/
    boolean getUnknownRV();

	/** Clear the DataValueDescriptor cache, if one exists.
	 *  (The DataValueDescriptor can be 1 of 3 types:
	 *		o  VARIANT		  - cannot be cached as its value can 
	 *							vary within a scan
	 *		o  SCAN_INVARIANT - can be cached within a scan as its
	 *							value will not change within a scan
	 *		o  QUERY_INVARIANT- can be cached across the life of the query
	 *							as its value will never change
	 */
	void clearOrderableCache();


	/** 
	 * This method reinitializes all the state of
	 * the Qualifier.  It is used to distinguish between
	 * resetting something that is query invariant
	 * and something that is constant over every
	 * execution of a query.  Basically, clearOrderableCache()
	 * will only clear out its cache if it is a VARIANT
	 * or SCAN_INVARIANT value.  However, each time a
	 * query is executed, the QUERY_INVARIANT qualifiers need
	 * to be reset.
	 */
	void reinitialize();

    String getText();

	int getVariantType();
}
