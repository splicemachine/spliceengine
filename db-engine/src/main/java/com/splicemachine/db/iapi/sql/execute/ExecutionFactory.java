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

package com.splicemachine.db.iapi.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.ListDataType;

/**
	This is the factory for creating a factories needed by
	execution per connection, and the context to hold them.
	<p>
	There is expected to be one of these configured per database.
	<p>
	If a factory is needed outside of execution (say,
	data dictionary or compilation), then it belongs in the
	LanguageConnectionContext.

	@see com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext
 */
public interface ExecutionFactory {

		/**
		Module name for the monitor's module locating system.
	 */
	String MODULE = "com.splicemachine.db.iapi.sql.execute.ExecutionFactory";

	/**
		Only one result set factory is needed for a database
		in the system.
		We require that an execution factory be configured for
		each database. Each execution factory then needs to
		know about the result set factory it is maintaining
		for its database, so that it can provide it through
		calls to this method.
		So, we reuse the result set factory by making it 
		available to each connection
		in that connection's execution context.

		@return the result set factory for this database.
	 */
	ResultSetFactory getResultSetFactory() throws StandardException;
	
  	/**
  	 * Get the ExecutionFactory from this ExecutionContext.
  	 *
		We want an execution context so that we can push it onto
		the stack.  We could instead require the implementation
		push it onto the stack for us, but this way we know
		which context object exactly was pushed onto the stack.

		@param cm  the context manager
	 */
	ExecutionContext newExecutionContext(ContextManager cm);

	/**
	 * Create an execution time ResultColumnDescriptor from a 
	 * compile time RCD.
	 *
	 * @param compileRCD	The compile time RCD.
	 *
	 * @return The execution time ResultColumnDescriptor
	 */
	ResultColumnDescriptor getResultColumnDescriptor(ResultColumnDescriptor compileRCD);

	/**
	 * Create a result description given parameters for it.
	 */
	ResultDescription getResultDescription(ResultColumnDescriptor[] columns,
		String statementType);

	/**
	 * Get an array of ScanQualifiers for a scan.  ScanQualifiers are used
	 * with the DataDictionary.
	 *
	 * @param numQualifiers	The number of ScanQualifiers to get.
	 */
	ScanQualifier[][] getScanQualifier(int numQualifiers);

	/**
	 * Release a ScanQualifier[] (back to the pool or free it).
	 */

	void releaseScanQualifier(ScanQualifier[][] scanQualifiers);

	/**
	 * Get a Qualifier to use with a scan of a conglomerate.
	 *
	 * @param columnId	The store id of the column to qualify
	 * @param operator	One of Orderable.ORDER_OP_EQUALS,
	 *					Orderable.ORDER_OP_LESSTHAN,
	 *					or Orderable.ORDER_OP_LESSOREQUALS
	 * @param orderableGetter	A generated method that returns the
	 *							Orderable to be compared with the column
	 * @param activation	The Activation that acts as the receiver for the
	 *						generated method
	 * @param orderedNulls	True means that null == null for the sake of
	 *						this Qualifier
	 * @param unknownRV	The value to return if the comparison between
	 *					the column and the Orderable value returns the
	 *					unknown truth value
	 * @param negateCompareResult	True means to negate the result of the comparison.
	 *					So, for example, to do a > comparison, you would
	 *					pass ORDER_OP_LESSOREQUALS and set negate to true.
	 * @param variantType	The variantType for the qualifier's orderable.
	 *						(Determines whether or not to cache the value.)
	 *
	 * @return	A new Qualifier
	 */
	Qualifier getQualifier(int columnId,
                           int storagePosition,
						   int operator,
						   GeneratedMethod orderableGetter,
						   Activation activation,
						   boolean orderedNulls,
						   boolean unknownRV,
						   boolean negateCompareResult,
						   int variantType,
                           String text);

    Qualifier getQualifier(int columnId,
                           int storagePosition,
                           int operator,
                           GeneratedMethod orderableGetter,
                           Activation activation,
                           boolean orderedNulls,
                           boolean unknownRV,
                           boolean negateCompareResult,
                           int variantType);

	/**
	  Create a new RowChanger for performing update and delete
	  operations based on full before and after rows.

	  @param heapConglom	Conglomerate # for the heap
	  @param heapSCOCI The SCOCI for the heap.
	  @param heapDCOCI The DCOCI for the heap.
	  @param irgs the IndexRowGenerators for the table's indexes. We use
	    positions in this array as local id's for indexes. To support updates,
	    only indexes that change need be included.
	  @param indexCIDS the conglomerateids for the table's idexes. 
	  	indexCIDS[ix] corresponds to the same index as irgs[ix].
	  @param indexSCOCIs the SCOCIs for the table's idexes. 
	  	indexSCOCIs[ix] corresponds to the same index as irgs[ix].
	  @param indexDCOCIs the DCOCIs for the table's idexes. 
	  	indexDCOCIs[ix] corresponds to the same index as irgs[ix].
	  @param numberOfColumns Number of columns in a full row.
	  @param tc the transaction controller
	  @param streamStorableHeapColIds Column ids of stream storable
	         columns. (0 based, Only needed for sync. null if none or
			 not needed).
	  @param activation	The Activation.
	  @exception StandardException		Thrown on error
	  */
	RowChanger
	getRowChanger(long heapConglom,
				  StaticCompiledOpenConglomInfo heapSCOCI,
				  DynamicCompiledOpenConglomInfo heapDCOCI,
				  IndexRowGenerator[] irgs,
				  long[] indexCIDS,
				  StaticCompiledOpenConglomInfo[] indexSCOCIs,
				  DynamicCompiledOpenConglomInfo[] indexDCOCIs,
				  int numberOfColumns,
				  TransactionController tc,
				  int[] changedColumnIds,
				  int[] streamStorableHeapColIds,
				  Activation activation) throws StandardException;

	/**
	  Create a new RowChanger for doing insert update and delete
	  operations based on partial before and after. 

	  @param heapConglom	Conglomerate # for the heap
	  @param heapSCOCI The SCOCI for the heap.
	  @param heapDCOCI The DCOCI for the heap.
	  @param irgs the IndexRowGenerators for the table's indexes. We use
	    positions in this array as local id's for indexes. To support updates,
	    only indexes that change need be included.
	  @param indexCIDS the conglomerateids for the table's idexes. 
	  	indexCIDS[ix] corresponds to the same index as irgs[ix].
	  @param indexSCOCIs the SCOCIs for the table's idexes. 
	  	indexSCOCIs[ix] corresponds to the same index as irgs[ix].
	  @param indexDCOCIs the DCOCIs for the table's idexes. 
	  	indexDCOCIs[ix] corresponds to the same index as irgs[ix].
	  @param numberOfColumns Number of columns in partial row.
	  @param tc the transaction controller 
	  @param changedColumnIds array of 1 based ints of columns
		to be updated.  Used by update only.
	  @param baseRowReadList the columns in the base row that were
		read (1 based)
	  @param baseRowReadMap baseRowReadMap[heapColId]->readRowColId
	         (0 based)
	  @param streamStorableColIds Column ids of stream storable
	         columns. (0 based, Only needed for sync. null if none or
			 not needed).
	  @param activation	The Activation.

	  @exception StandardException		Thrown on error
	  */
	RowChanger
	getRowChanger(long heapConglom,
				  StaticCompiledOpenConglomInfo heapSCOCI,
				  DynamicCompiledOpenConglomInfo heapDCOCI,
				  IndexRowGenerator[] irgs,
				  long[] indexCIDS,
				  StaticCompiledOpenConglomInfo[] indexSCOCIs,
				  DynamicCompiledOpenConglomInfo[] indexDCOCIs,
				  int numberOfColumns,
				  TransactionController tc,
				  int[] changedColumnIds,
				  FormatableBitSet baseRowReadList,
				  int[] baseRowReadMap,
				  int[] streamStorableColIds,
				  Activation activation) throws StandardException;


	// Methods from old RowFactory interface
	/**
		This returns a new row that is storable but not indexable 
	 */
	ExecRow getValueRow(int numColumns);

	/**
		This returns an indexable row
	 */
	ExecIndexRow	getIndexableRow(int numColumns);

	/**
		This returns the value row as an indexable row 
	 */
	ExecIndexRow	getIndexableRow(ExecRow valueRow);
	
	// Get a new ListDataType object that holds "numberOfValues" data values.
    ListDataType getListData(int numberOfValues);
}
