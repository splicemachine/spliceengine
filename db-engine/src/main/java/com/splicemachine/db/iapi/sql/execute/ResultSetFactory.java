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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.util.ArrayList;

/**
 * ResultSetFactory provides a wrapper around all of
 * the result sets needed in an execution implementation.
 * <p>
 * For the activations to avoid searching for this module
 * in their execute methods, the base activation supertype
 * should implement a method that does the lookup and salts
 * away this factory for the activation to use as it needs it.
 *
 */
public interface ResultSetFactory {
	/**
		Module name for the monitor's module locating system.
	 */
	String MODULE = "com.splicemachine.db.iapi.sql.execute.ResultSetFactory";

	//
	// DDL operations
	//

	/**
	    Generic DDL result set creation.

		@param activation 		the activation for this result set

		@return	ResultSet	A wrapper result set to run the Execution-time
		                        logic.
		@exception StandardException thrown when unable to create the
			result set
	 */
	ResultSet getDDLResultSet(Activation activation)
					throws StandardException;


	//
	// MISC operations
	//

	/**
	    Generic Misc result set creation.

		@param activation 		the activation for this result set

		@return	ResultSet	A wrapper result set to run the Execution-time
		                        logic.
		@exception StandardException thrown when unable to create the
			result set
	 */
	ResultSet getMiscResultSet(Activation activation)
					throws StandardException;

	//
	// Transaction operations
	//
	/**

		@param activation 		the activation for this result set

		@return	ResultSet	A wrapper result set to run the Execution-time
		                        logic.
		@exception StandardException thrown when unable to create the
			result set
	 */
	ResultSet getSetTransactionResultSet(Activation activation) 
		throws StandardException;

	//
	// DML statement operations
	//
	/**
		An insert result set simply reports that it completed, and
		the number of rows inserted.  It does not return rows.
		The insert has been completed once the
		insert result set is available.

		@param source the result set from which to take rows to
			be inserted into the target table.
		@param generationClauses	The code to compute column generation clauses if any
		@param checkGM	The code to enforce the check constraints, if any
		@return the insert operation as a result set.
		@exception StandardException thrown when unable to perform the insert
	 */
	ResultSet getInsertResultSet(NoPutResultSet source,
	                             GeneratedMethod generationClauses,
								 GeneratedMethod checkGM,
								 String insertMode,
								 String statusDirectory,
								 int failBadRecordCount,
								 boolean skipConflictDetection,
								 boolean skipWAL,
                                 double optimizerEstimatedRowCount,
                                 double optimizerEstimatedCost,
                                 String tableVersion,
                                 String explainPlan,
								 String delimited,
								 String escaped,
								 String lines,
								 String storedAs,
								 String location,
								 String compression,
								 int partitionBy,
								 String bulkImportDirectory,
								 boolean samplingOnly,
                                 boolean outputKeysOnly,
								 boolean skipSampling,
								 double sampleFraction,
								 String indexName)
        throws StandardException;

	/**
		An insert VTI result set simply reports that it completed, and
		the number of rows inserted.  It does not return rows.
		The insert has been completed once the
		insert result set is available.

		@param source the result set from which to take rows to
			be inserted into the target table.
		@param vtiRS	The code to instantiate the VTI, if necessary
		@return the insert VTI operation as a result set.
		@exception StandardException thrown when unable to perform the insert
	 */
	ResultSet getInsertVTIResultSet(NoPutResultSet source, 
								 NoPutResultSet vtiRS,
                                 double optimizerEstimatedRowCount,
                                 double optimizerEstimatedCost)
        throws StandardException;

	/**
		A delete VTI result set simply reports that it completed, and
		the number of rows deleted.  It does not return rows.
		The delete has been completed once the
		delete result set is available.

		@param source the result set from which to take rows to
			be inserted into the target table.
		@return the delete VTI operation as a result set.
		@exception StandardException thrown when unable to perform the insert
	 */
	ResultSet getDeleteVTIResultSet(NoPutResultSet source,double optimizerEstimatedRowCount,
                                    double optimizerEstimatedCost)
        throws StandardException;

	/**
		A delete result set simply reports that it completed, and
		the number of rows deleted.  It does not return rows.
		The delete has been completed once the
		delete result set is available.

		@param source the result set from which to take rows to
			be deleted from the target table. This result set must
			contain one column which provides RowLocations that are
			valid in the target table.
		@return the delete operation as a result set.
		@exception StandardException thrown when unable to perform the delete
	 */
	ResultSet getDeleteResultSet(NoPutResultSet source,double optimizerEstimatedRowCount,
                                 double optimizerEstimatedCost, String tableVersion,
                                 String explainPlan, String bulkDeleteDirectory, int colMapRefItem)
							throws StandardException;

	/**
		A delete Cascade result set simply reports that it completed, and
		the number of rows deleted.  It does not return rows.
		The delete has been completed once the
		delete result set is available.

		@param source the result set from which to take rows to
			be deleted from the target table.
		@param constantActionItem a constant action saved object reference
		@param dependentResultSets an array of DeleteCascade Resultsets
                                   for the current table referential action
								   dependents tables.
		@param resultSetId  an Id which is used to store the refence
                            to the temporary result set created of
                            the materilized rows.Dependent table resultsets
							uses the same id to access their parent temporary result sets.
		@return the delete operation as a delete cascade result set.
		@exception StandardException thrown when unable to perform the delete
	 */
	ResultSet getDeleteCascadeResultSet(NoPutResultSet source,
										int constantActionItem,
										ResultSet[] dependentResultSets, 
										String resultSetId)
							throws StandardException;

	/**
		An update result set simply reports that it completed, and
		the number of rows updated.  It does not return rows.
		The update has been completed once the
		update result set is available.

		@param source the result set from which to take rows to be 
			updated in the target table. This result set must contain 
			a column which provides RowLocations that are valid in the 
			target table, and new values to be placed in those rows.
		@param generationClauses	The code to compute column generation clauses if any
		@param checkGM	The code to enforce the check constraints, if any
		@return the update operation as a result set.
		@exception StandardException thrown when unable to perform the update
	 */
	ResultSet getUpdateResultSet(NoPutResultSet source, GeneratedMethod generationClauses,
								 GeneratedMethod checkGM,double optimizerEstimatedRowCount,
                                 double optimizerEstimatedCost, String tableVersion,
                                 String explainPlan)
        throws StandardException;

	/**
     * @param source the result set from which to take rows to be 
     *               updated in the target table.
     * @return the update operation as a result set.
     * @exception StandardException thrown on error
	 */
	ResultSet getUpdateVTIResultSet(NoPutResultSet source, double optimizerEstimatedRowCount,
									double optimizerEstimatedCost)
        throws StandardException;

	/**
		An update result set simply reports that it completed, and
		the number of rows updated.  It does not return rows.
		The update has been completed once the
		update result set is available.

		@param source the result set from which to take rows to be 
			updated in the target table. This result set must contain 
			a column which provides RowLocations that are valid in the 
			target table, and new values to be placed in those rows.
		@param generationClauses	The code to compute generated columns, if any
		@param checkGM	The code to enforce the check constraints, if any
		@param constantActionItem a constant action saved object reference
		@param rsdItem   result Description, saved object id. 				
		@return the update operation as a result set.
		@exception StandardException thrown when unable to perform the update
	 */
	ResultSet getDeleteCascadeUpdateResultSet(NoPutResultSet source, 
								 GeneratedMethod generationClauses,
								 GeneratedMethod checkGM,
								 int constantActionItem,
								 int rsdItem)
        throws StandardException;

	/**
		A call statement result set simply reports that it completed.  
		It does not return rows.

		@param methodCall a reference to a method in the activation
			  for the method call
		@param activation the activation for this result set

		@return the call statement operation as a result set.
		@exception StandardException thrown when unable to perform the call statement
	 */
	ResultSet getCallStatementResultSet(GeneratedMethod methodCall,
				                        Activation activation) 
        throws StandardException;

    ResultSet getCallStatementResultSet(GeneratedMethod methodCall,
                                        Activation activation,
                                        String origClassName,
                                        String origMethodName) 
        throws StandardException;
    
	//
	// Query expression operations
	//

	/**
		A project restrict result set iterates over its source,
		evaluating a restriction and when it is satisfied,
		constructing a row to return in its result set based on
		its projection.
		The rows can be constructed as they are requested from the
		result set.

		@param source the result set from which to take rows to be 
			filtered by this operation.
		@param restriction a reference to a method in the activation
			that is applied to the activation's "current row" field
			to determine whether the restriction is satisfied or not.
			The signature of this method is
			<verbatim>
				Boolean restriction() throws StandardException;
			</verbatim>
		@param projection a reference to a method in the activation
			that is applied to the activation's "current row" field
			to project out the expected result row.
			The signature of this method is
			<verbatim>
				ExecRow projection() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param constantRestriction a reference to a method in the activation
			that represents a constant expression (eg where 1 = 2).
			The signature of this method is
			<verbatim>
				Boolean restriction() throws StandardException;
			</verbatim>
		@param mapArrayItem	Item # for mapping of source to target columns
        @param cloneMapItem Item # for columns that need cloning
        @param reuseResult  Whether or not to reuse the result row.
		@param doesProjection	Whether or not this PRN does a projection
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@param filterPred  The filter predicate to apply on a source
		                   NativeSparkDataSet, if applicable.
		@param expressions The projected expressions to select from a source
		                   NativeSparkDataSet, if applicable.
	        @param hasGroupingFunction Is true if one of the projections is a
	                                   grouping function that needs special handling.
		@return the project restrict operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getProjectRestrictResultSet(NoPutResultSet source,
                                               GeneratedMethod restriction,
                                               GeneratedMethod projection, int resultSetNumber,
                                               GeneratedMethod constantRestriction,
                                               int mapArrayItem,
                                               int cloneMapItem,
                                               boolean reuseResult,
                                               boolean doesProjection,
                                               double optimizerEstimatedRowCount,
                                               double optimizerEstimatedCost,
                                               String explainPlan,
                                               String filterPred,
                                               String[] expressions,
	                                       boolean hasGroupingFunction) throws StandardException;

	// Provide old versions of getProjectRestrictResultSet so an upgrade from 2.7 to 2.8
	// can handle old versions of this method that were serialized to disk.
	NoPutResultSet getProjectRestrictResultSet(NoPutResultSet source,
                                               GeneratedMethod restriction,
                                               GeneratedMethod projection, int resultSetNumber,
                                               GeneratedMethod constantRestriction,
                                               int mapArrayItem,
                                               int cloneMapItem,
                                               boolean reuseResult,
                                               boolean doesProjection,
                                               double optimizerEstimatedRowCount,
                                               double optimizerEstimatedCost,
                                               String explainPlan,
                                               String filterPred,
                                               String[] expressions) throws StandardException;

	NoPutResultSet getProjectRestrictResultSet(NoPutResultSet source,
                                               GeneratedMethod restriction,
                                               GeneratedMethod projection, int resultSetNumber,
                                               GeneratedMethod constantRestriction,
                                               int mapArrayItem,
                                               int cloneMapItem,
                                               boolean reuseResult,
                                               boolean doesProjection,
                                               double optimizerEstimatedRowCount,
                                               double optimizerEstimatedCost,
                                               String explainPlan) throws StandardException;

	/**
		A hash table result set builds a hash table on its source,
		applying a list of predicates, if any, to the source,
		when building the hash table.  It then does a look up into
		the hash table on a probe.
		The rows can be constructed as they are requested from the
		result set.

		@param source the result set from which to take rows to be 
			filtered by this operation.
		@param singleTableRestriction restriction, if any, applied to
			input of hash table.
		@param equijoinQualifiers Qualifier[] for look up into hash table
		@param projection a reference to a method in the activation
			that is applied to the activation's "current row" field
			to project out the expected result row.
			The signature of this method is
			<verbatim>
				ExecRow projection() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param mapRefItem	Item # for mapping of source to target columns
		@param reuseResult	Whether or not to reuse the result row.
		@param keyColItem	Item for hash key column array
		@param removeDuplicates	Whether or not to remove duplicates when building the hash table
		@param maxInMemoryRowCount			Max size of in-memory hash table
		@param initialCapacity				initialCapacity for java.util.HashTable
		@param loadFactor					loadFactor for java.util.HashTable
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@return the project restrict operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getHashTableResultSet(NoPutResultSet source,
										 GeneratedMethod singleTableRestriction,
										 String equijoinQualifiersField,
										 GeneratedMethod projection, int resultSetNumber,
										 int mapRefItem,
										 boolean reuseResult,
										 int keyColItem,
										 boolean removeDuplicates,
										 long maxInMemoryRowCount,
										 int initialCapacity,
										 float loadFactor,
										 double optimizerEstimatedRowCount,
										 double optimizerEstimatedCost)
			 throws StandardException;

	/**
		A sort result set sorts its source and if requested removes
		duplicates.  It will generate the entire result when open, and
		then return it a row at a time.
		<p>
		If passed aggregates it will do scalar or vector aggregate
		processing.  A list of aggregator information is passed
		off of the PreparedStatement's savedObjects.  Aggregation
		and SELECT DISTINCT cannot be processed in the same sort.

		@param source the result set from which to take rows to be 
			filtered by this operation.
		@param distinct true if distinct SELECT list
		@param isInSortedOrder	true if the source result set is in sorted order
		@param orderItem entry in preparedStatement's savedObjects for order
		@param rowAllocator a reference to a method in the activation
			that generates rows of the right size and shape for the source
		@param rowSize the size of the row that is allocated by rowAllocator.
			size should be the maximum size of the sum of all the datatypes.
			user type are necessarily approximated
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@return the distinct operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getSortResultSet(NoPutResultSet source,
		boolean distinct, 
		boolean isInSortedOrder,
		int orderItem,
		GeneratedMethod rowAllocator, 
		int rowSize,
		int resultSetNumber, 
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		String explainPlan) 
			throws StandardException;

	/**
		A ScalarAggregateResultSet computes non-distinct scalar aggregates.
		It will compute the aggregates when open.

		@param source the result set from which to take rows to be 
			filtered by this operation.
		@param isInSortedOrder	true if the source result set is in sorted order
		@param aggregateItem entry in preparedStatement's savedObjects for aggregates
		@param orderingItem		Ignored to allow same signature as getDistinctScalarAggregateResultSet
		@param rowAllocator a reference to a method in the activation
			that generates rows of the right size and shape for the source
		@param rowSize			Ignored to allow same signature as getDistinctScalarAggregateResultSet
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param singleInputRow	Whether we know we have a single input row or not
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
	 	@param encodedNativeSparkMode Is native spark processing on, off, or should we
	                                      use the system setting (from SConfiguration)?
		@return the scalar aggregation operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getScalarAggregateResultSet(NoPutResultSet source,
		boolean isInSortedOrder,
		int aggregateItem,
		int orderingItem,
		GeneratedMethod rowAllocator, 
		int rowSize,
		int resultSetNumber, 
		boolean singleInputRow,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		String explainPlan,
		int encodedNativeSparkMode)
			throws StandardException;

	/**
		A DistinctScalarAggregateResultSet computes scalar aggregates when 
		at least one of them is a distinct aggregate.
		It will compute the aggregates when open.

		@param source the result set from which to take rows to be 
			filtered by this operation.
		@param isInSortedOrder	true if the source result set is in sorted order
		@param aggregateItem entry in preparedStatement's savedObjects for aggregates
		@param orderingItem entry in preparedStatement's savedObjects for order
		@param rowAllocator a reference to a method in the activation
			that generates rows of the right size and shape for the source
		@param rowSize the size of the row that is allocated by rowAllocator.
			size should be the maximum size of the sum of all the datatypes.
			user type are necessarily approximated
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param singleInputRow	Whether we know we have a single input row or not
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
	 	@param encodedNativeSparkMode Is native spark processing on, off, or should we
	                                      use the system setting (from SConfiguration)?
		@return the scalar aggregation operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getDistinctScalarAggregateResultSet(NoPutResultSet source,
		boolean isInSortedOrder,
		int aggregateItem,
		int orderingItem,
		GeneratedMethod rowAllocator, 
		int rowSize,
		int resultSetNumber, 
		boolean singleInputRow,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		String explainPlan,
		int encodedNativeSparkMode)
			throws StandardException;

	/**
		A GroupedAggregateResultSet computes non-distinct grouped aggregates.
		It will compute the aggregates when open.

		@param source the result set from which to take rows to be 
			filtered by this operation.
		@param isInSortedOrder	true if the source result set is in sorted order
		@param aggregateItem entry in preparedStatement's savedObjects for aggregates
		@param orderingItem		Ignored to allow same signature as getDistinctScalarAggregateResultSet
		@param rowAllocator a reference to a method in the activation
			that generates rows of the right size and shape for the source
		@param rowSize			Ignored to allow same signature as getDistinctScalarAggregateResultSet
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@param isRollup true if this is a GROUP BY ROLLUP()
		@param groupingIdColPosition column position of the groupingId column which is only used for rollup
		@param groupingIdArrayItem entry in preparedStatement's savedObjects for the bit array of groupingId values,
	                               which is only used for rollup
	 	@param encodedNativeSparkMode Is native spark processing on, off, or should we
	                                      use the system setting (from SConfiguration)?
		@return the scalar aggregation operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getGroupedAggregateResultSet(NoPutResultSet source,
		boolean isInSortedOrder,
		int aggregateItem,
		int orderingItem,
		GeneratedMethod rowAllocator, 
		int rowSize,
		int resultSetNumber, 
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		boolean isRollup,
		int groupingIdColPosition,
		int groupingIdArrayItem,
		String explainPlan,
		int encodedNativeSparkMode)
			throws StandardException;

	/**
		A DistinctGroupedAggregateResultSet computes scalar aggregates when 
		at least one of them is a distinct aggregate.
		It will compute the aggregates when open.

		@param source the result set from which to take rows to be 
			filtered by this operation.
		@param isInSortedOrder	true if the source result set is in sorted order
		@param aggregateItem entry in preparedStatement's savedObjects for aggregates
		@param orderingItem entry in preparedStatement's savedObjects for order
		@param rowAllocator a reference to a method in the activation
			that generates rows of the right size and shape for the source
		@param rowSize the size of the row that is allocated by rowAllocator.
			size should be the maximum size of the sum of all the datatypes.
			user type are necessarily approximated
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@param isRollup true if this is a GROUP BY ROLLUP()
		@param groupingIdColPosition column position of the groupingId column which is only used for rollup
		@param groupingIdArrayItem entry in preparedStatement's savedObjects for the bit array of groupingId values,
	                                which is only used for rollup
	 	@param encodedNativeSparkMode Is native spark processing on, off, or should we
	                                      use the system setting (from SConfiguration)?
		@return the scalar aggregation operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getDistinctGroupedAggregateResultSet(NoPutResultSet source,
		boolean isInSortedOrder,
		int aggregateItem,
		int orderingItem,
		GeneratedMethod rowAllocator, 
		int rowSize,
		int resultSetNumber, 
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		boolean isRollup,
		int groupingIdColPosition,
		int groupingIdArrayItem,
		String explainPlan,
		int encodedNativeSparkMode)
			throws StandardException;

	/**
		An any result set iterates over its source,
		returning a row with all columns set to nulls
		if the source returns no rows.

		@param source the result set from which to take rows to be 
			filtered by this operation.
		@param emptyRowFun a reference to a method in the activation
			that is called if the source returns no rows
		@param resultSetNumber		The resultSetNumber for the ResultSet
		@param subqueryNumber		The subquery number for this subquery.
		@param pointOfAttachment	The point of attachment for this subquery.
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@return the any operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getAnyResultSet(NoPutResultSet source,
		GeneratedMethod emptyRowFun, int resultSetNumber,
		int subqueryNumber, int pointOfAttachment,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost) 
		throws StandardException;

	/**
		A once result set iterates over its source,
		raising an error if the source returns > 1 row and
		returning a row with all columns set to nulls
		if the source returns no rows.

		@param source the result set from which to take rows to be 
			filtered by this operation.
		@param emptyRowFun a reference to a method in the activation
			that is called if the source returns no rows
		@param cardinalityCheck The type of cardinality check, if any that
			is required
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param subqueryNumber		The subquery number for this subquery.
		@param pointOfAttachment	The point of attachment for this subquery.
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@return the once operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getOnceResultSet(NoPutResultSet source,
		GeneratedMethod emptyRowFun,
		int cardinalityCheck, int resultSetNumber, 
		int subqueryNumber, int pointOfAttachment,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost) 
		throws StandardException;

	/**
		A row result set forms a result set on a single, known row value.
		It is used to turn constant rows into result sets for use in
		the result set paradigm.
		The row can be constructed when it is requested from the
		result set.

		@param activation the activation for this result set,
			against which the row operation is performed to
			create the result set.
		@param row a reference to a method in the activation
			that creates the expected row.
			<verbatim>
				ExecRow row() throws StandardException;
			</verbatim>
		@param canCacheRow	True if execution can cache the input row
			after it has gotten it.  If the input row is constructed soley
			of constants or parameters, it is ok to cache this row rather
			than recreating it each time it is requested.
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@return the row as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getRowResultSet(Activation activation, GeneratedMethod row, 
							  boolean canCacheRow,
							  int resultSetNumber,
							  double optimizerEstimatedRowCount,
							  double optimizerEstimatedCost)
		throws StandardException;

    NoPutResultSet getRowResultSet(Activation activation, ExecRow row,
                                   boolean canCacheRow,
                                   int resultSetNumber,
                                   double optimizerEstimatedRowCount,
                                   double optimizerEstimatedCost)
            throws StandardException;

    /**
        Splice Addition

        A resultset that forms a result set on a collection of known rows.
        It is used to cache rows from a result set that's been materialized.

        @param activation The activation for this result set
        @param source Source result set
        @param resultSetNumber The resultSetNumber for the result set being materialized
     */

    NoPutResultSet getCachedResultSet(Activation activation, NoPutResultSet source, int resultSetNumber)
        throws StandardException;

	/**
		A VTI result set wraps a user supplied result set.

		@param activation the activation for this result set,
			against which the row operation is performed to
			create the result set.
		@param row a reference to a method in the activation
			that creates the expected row.
			<verbatim>
				ExecRow row() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param constructor		The GeneratedMethod for the user's constructor
		@param javaClassName	The java class name for the VTI
		@param erdNumber		int for referenced column BitSet (so it can be turned back into an object)
		@param version2			Whether or not VTI is a version 2 VTI.
		@param isTarget			Whether or not VTI is a target VTI.
		@param optimizerEstimatedRowCount	Estimated total # of rows by optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@param isDerbyStyleTableFunction    True if this is a Derby-style table function
		@param returnTypeNumber	Which saved object contains the return type (a multi-set) serialized as a byte array
		@param vtiProjectionNumber	Which saved object contains the projection for a RestrictedVTI
		@param vtiRestrictionNumber	Which saved object contains the restriction for a RestrictedVTI
	    @param vtiResultDescriptionNumber Which saved object contains the result description of a VTI
		@return the row as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	public NoPutResultSet getVTIResultSet(Activation activation, GeneratedMethod row,
								   int resultSetNumber,
								   GeneratedMethod constructor,
								   String javaClassName,
								   String pushedQualifiersField,
								   int erdNumber,
								   int ctcNumber,
								   boolean isTarget,
								   int scanIsolationLevel,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   boolean isDerbyStyleTableFunction,
								   int returnTypeNumber,
								   int vtiProjectionNumber,
								   int vtiRestrictionNumber,
								   int vtiResultDescriptionNumber,
								   String explainPlan)
		 throws StandardException;

	/*
	 * This method was purely added to get some stored prepared statements to pass the validation stage of their compilation.
	 * The existing method used a String for pushedQualifiersField.  However, nothing was done with the initial value that
	 * was passed into the constructor.  So this method does the same thing and ignores the pushedQualifiersField which is
	 * an com.splicemachine.db.iapi.store.access.Qualifier[][].
	 */
	NoPutResultSet getVTIResultSet(
			Activation activation,
			GeneratedMethod row,
			int resultSetNumber,
			GeneratedMethod constructor,
			String javaClassName,
			com.splicemachine.db.iapi.store.access.Qualifier[][] pushedQualifiersField,
			int erdNumber,
			int ctcNumber,
			boolean isTarget,
			int scanIsolationLevel,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			boolean isDerbyStyleTableFunction,
			int returnTypeNumber,
			int vtiProjectionNumber,
			int vtiRestrictionNumber,
			int vtiResultDescriptionNumber,
			String explainPlan)
					throws StandardException;
	/**
		A distinct scan result set pushes duplicate elimination into
		the scan.
		<p>

		@param activation the activation for this result set,
			which provides the context for the row allocation operation.
		@param conglomId the conglomerate of the table to be scanned.
		@param scociItem The saved item for the static conglomerate info.
		@param resultRowAllocator a reference to a method in the activation
			that creates a holder for the rows from the scan.
			<verbatim>
				ExecRow rowAllocator() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param hashKeyColumn	The 0-based column # for the hash key.
		@param tableName		The full name of the table
		@param userSuppliedOptimizerOverrides		Overrides specified by the user on the sql
		@param indexName		The name of the index, if one used to access table.
		@param isConstraint		If index, if used, is a backing index for a constraint.
		@param colRefItem		An saved item for a bitSet of columns that
								are referenced in the underlying table.  -1 if
								no item.
		@param lockMode			The lock granularity to use (see
								TransactionController in access)
		@param tableLocked		Whether or not the table is marked as using table locking
								(in sys.systables)
		@param isolationLevel	Isolation level (specified or not) to use on scans
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@return the table scan operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getDistinctScanResultSet(
			                    Activation activation,
								long conglomId,
								int scociItem,			
								GeneratedMethod resultRowAllocator,
								int resultSetNumber,
								int hashKeyColumn,
								String tableName,
								String userSuppliedOptimizerOverrides,
								String indexName,
								boolean isConstraint,
								int colRefItem,
								int lockMode,
								boolean tableLocked,
								int isolationLevel,
								double optimizerEstimatedRowCount,
								double optimizerEstimatedCost,
                                String tableVersion,
								String explainPlan,
								boolean pin,
								int splits,
								String delimited,
								String escaped,
								String lines,
								String storedAs,
								String location,
								int partitionByRefItem,
								GeneratedMethod defaultRowFunc,
								int defaultValueMapItem)
			throws StandardException;

	/**
		A table scan result set forms a result set on a scan
		of a table.
		The rows can be constructed as they are requested from the
		result set.
		<p>
		This form of the table scan operation is simple, and is
		to be used when there are no predicates to be passed down
		to the scan to limit its scope on the target table.

		@param conglomId the conglomerate of the table to be scanned.
		@param scociItem The saved item for the static conglomerate info.
		@param activation the activation for this result set,
			which provides the context for the row allocation operation.
		@param resultRowAllocator a reference to a method in the activation
			that creates a holder for the result row of the scan.  May
			be a partial row.
			<verbatim>
				ExecRow rowAllocator() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param startKeyGetter a reference to a method in the activation
			that gets the start key indexable row for the scan.  Null
			means there is no start key.
			<verbatim>
				ExecIndexRow startKeyGetter() throws StandardException;
			</verbatim>
		@param startSearchOperator The start search operator for opening
			the scan
		@param stopKeyGetter	a reference to a method in the activation
			that gets the stop key indexable row for the scan.  Null means
			there is no stop key.
			<verbatim>
				ExecIndexRow stopKeyGetter() throws StandardException;
			</verbatim>
		@param stopSearchOperator	The stop search operator for opening
			the scan
		@param sameStartStopPosition	Re-use the startKeyGetter for the stopKeyGetter
										(Exact match search.)
		@param qualifiers the array of Qualifiers for the scan.
			Null or an array length of zero means there are no qualifiers.
		@param tableName		The full name of the table
		@param userSuppliedOptimizerOverrides		Overrides specified by the user on the sql
		@param indexName		The name of the index, if one used to access table.
		@param isConstraint		If index, if used, is a backing index for a constraint.
		@param forUpdate		True means open for update
		@param colRefItem		An saved item for a bitSet of columns that
								are referenced in the underlying table.  -1 if
								no item.
		@param lockMode			The lock granularity to use (see
								TransactionController in access)
		@param tableLocked		Whether or not the table is marked as using table locking
								(in sys.systables)
		@param isolationLevel	Isolation level (specified or not) to use on scans
		@param oneRowScan		Whether or not this is a 1 row scan.
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer

		@return the table scan operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getTableScanResultSet(
			                    Activation activation,
								long conglomId,
								int scociItem,
								GeneratedMethod resultRowAllocator,
								int resultSetNumber,
								GeneratedMethod startKeyGetter,
								int startSearchOperator,
								GeneratedMethod stopKeyGetter,
								int stopSearchOperator,
								boolean sameStartStopPosition,
                                boolean rowIdKey,
								String qualifiersField,
								String tableName,
								String userSuppliedOptimizerOverrides,
								String indexName,
								boolean isConstraint,
								boolean forUpdate,
								int colRefItem,
								int indexColItem,
								int lockMode,
								boolean tableLocked,
								int isolationLevel,
								boolean oneRowScan,
								double optimizerEstimatedRowCount,
								double optimizerEstimatedCost,
                                String tableVersion,
                                String explainPlan,
								boolean pin,
								int splits,
								String delimited,
								String escaped,
								String lines,
								String storedAs,
								String location,
								int partitionByRefItem,
								GeneratedMethod defaultRowFunc,
								int defaultValueMapItem
								)
			throws StandardException;

    /**
		A multi-probe result set, used for probing an index with one or more
		target values (probeValues) and returning the matching rows.  This
		type of result set is useful for IN lists as it allows us to avoid
		scannning an entire, potentially very large, index for a mere handful
		of rows (DERBY-47).

		All arguments are the same as for TableScanResultSet, plus the
		following:

		@param getProbeValsFunc function pointer to get list of values with which to probe the underlying
			table. Should not be null.
		@param sortRequired Which type of sort we need for the values
			(ascending, descending, or none).
	 */
	NoPutResultSet getMultiProbeTableScanResultSet(
			                    Activation activation,
								long conglomId,
								int scociItem,
								GeneratedMethod resultRowAllocator,
								int resultSetNumber,
								GeneratedMethod startKeyGetter,
								int startSearchOperator,
								GeneratedMethod stopKeyGetter,
								int stopSearchOperator,
								boolean sameStartStopPosition,
                                boolean rowIdKey,
								String qualifiersField,
								GeneratedMethod getProbeValsFunc,
								int sortRequired,
								int inlistPosition,
								int inlistTypeArrayItem,
								String tableName,
								String userSuppliedOptimizerOverrides,
								String indexName,
								boolean isConstraint,
								boolean forUpdate,
								int colRefItem,
								int indexColItem,
								int lockMode,
								boolean tableLocked,
								int isolationLevel,
								boolean oneRowScan,
								double optimizerEstimatedRowCount,
								double optimizerEstimatedCost,
                                String tableVersion,
                                String explainPlan,
								boolean pin,
								int splits,
								String delimited,
								String escaped,
								String lines,
								String storedAs,
								String location,
								int partitionByRefItem,
								GeneratedMethod defaultRowFunc,
								int defaultValueMapItem
								)
			throws StandardException;
    /**
		An index row to base row result set gets an index row from its source
		and uses the RowLocation in its last column to get the row from the
		base conglomerate.
		<p>

	    @param conglomId	Conglomerate # for the heap.
		@param scoci The saved item for the static conglomerate info.
		@param source	the source result set, which is expected to provide
						rows from an index conglomerate
		@param resultRowAllocator a reference to a method in the activation
			that creates a holder for the rows from the scan.
			<verbatim>
				ExecRow rowAllocator() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param indexName		The name of the index.
		@param heapColRefItem	A saved item for a bitImpl of columns that
								are referenced in the underlying heap.  -1 if
								no item.
		@param allColRefItem A saved item for a bitImpl of columns
								that are referenced in the underlying
								index and heap.  -1 if no item.
		@param heapOnlyColRefItem A saved item for a bitImpl of
								columns that are referenced in the
								underlying heap only.  -1 if no item.

		@param indexColMapItem	A saved item for a ReferencedColumnsDescriptorImpl
								which tell  which columms are coming from the index.
		@param restriction		The restriction, if any, to be applied to the base row
		@param forUpdate		True means to open for update
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer

		@return the index row to base row operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getIndexRowToBaseRowResultSet(
			long conglomId,
			int scoci,
			NoPutResultSet source,
			GeneratedMethod resultRowAllocator,
			int resultSetNumber,
			String indexName,
			int heapColRefItem,
			int allColRefItem,
			int heapOnlyColRefItem,
			int indexColMapItem,
			GeneratedMethod restriction,
			boolean forUpdate,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			String tableVersion,
			String explainPlan,
			GeneratedMethod defaultRowFunc,
			int defaultValueMapItem)
			throws StandardException;


    /**
     A OLAP window on top of a regular result set. It is used to realize
     window functions.

     @param source the result set from which to take rows to be
     filtered by this operation.
     @param isInSortedOrder	true if the source result set is in sorted order
     @param aggregateItem entry in preparedStatement's savedObjects for aggregates
     @param rowAllocator a reference to a method in the activation
     that generates rows of the right size and shape for the source
     @param rowSize			the size of the row that is allocated by rowAllocator.
     size should be the maximum size of the sum of all the datatypes.
     user type are necessarily approximated
     @param resultSetNumber	The resultSetNumber for the ResultSet
     @param optimizerEstimatedRowCount	Estimated total # of rows by
     optimizer
     @param optimizerEstimatedCost		Estimated total cost by optimizer
     @return the scalar aggregation operation as a result set.
     @exception StandardException thrown when unable to create the
     result set
     */
    NoPutResultSet getWindowResultSet(NoPutResultSet source,
                                               boolean isInSortedOrder,
                                               int aggregateItem,
                                               GeneratedMethod rowAllocator,
                                               int rowSize,
                                               int resultSetNumber,
                                               double optimizerEstimatedRowCount,
                                               double optimizerEstimatedCost,
                                               String explainPlan)
        throws StandardException;


	/**
		A nested loop left outer join result set forms a result set on top of
		2 other result sets.
		The rows can be constructed as they are requested from the
		result set.
		<p>
		This form of the nested loop join operation is simple, and is
		to be used when there are no join predicates to be passed down
		to the join to limit its scope on the right ResultSet.

		@param leftResultSet	Outer ResultSet for join.
		@param leftNumCols		Number of columns in the leftResultSet
		@param rightResultSet	Inner ResultSet for join.
		@param rightNumCols		Number of columns in the rightResultSet
		@param joinClause a reference to a method in the activation
			that is applied to the activation's "current row" field
			to determine whether the joinClause is satisfied or not.
			The signature of this method is
			<verbatim>
				Boolean joinClause() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param oneRowRightSide	boolean, whether or not the right side returns
								a single row.  (No need to do 2nd next() if it does.)
		@param notExistsRightSide	boolean, whether or not the right side resides a
									NOT EXISTS base table
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@param userSuppliedOptimizerOverrides		Overrides specified by the user on the sql
		@return the nested loop join operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getNestedLoopJoinResultSet(NoPutResultSet leftResultSet,
											  int leftNumCols,
											  NoPutResultSet rightResultSet,
											  int rightNumCols,
											  GeneratedMethod joinClause,
											  int resultSetNumber,
											  boolean oneRowRightSide,
											  boolean notExistsRightSide,
											  boolean rightFromSSQ,
											  double optimizerEstimatedRowCount,
											  double optimizerEstimatedCost,
											  String userSuppliedOptimizerOverrides,
											  String explainPlan,
											  String sparkExpressionTreeAsString)
			throws StandardException;

	NoPutResultSet getCrossJoinResultSet(NoPutResultSet leftResultSet,
											  int leftNumCols,
											  NoPutResultSet rightResultSet,
											  int rightNumCols,
                                              int leftHashKeyItem,
                                              int rightHashKeyItem,
											  GeneratedMethod joinClause,
											  int resultSetNumber,
											  boolean oneRowRightSide,
											  boolean notExistsRightSide,
											  boolean rightFromSSQ,
											  double optimizerEstimatedRowCount,
											  double optimizerEstimatedCost,
											  String userSuppliedOptimizerOverrides,
											  String explainPlan,
											  String sparkExpressionTreeAsString)
			throws StandardException;

	NoPutResultSet getMergeSortJoinResultSet(NoPutResultSet leftResultSet,
											 int leftNumCols,
											 NoPutResultSet rightResultSet,
											 int rightNumCols,
											 int leftHashKeyItem,
											 int rightHashKeyItem,
											 GeneratedMethod joinClause,
											 int resultSetNumber,
											 boolean oneRowRightSide,
											 boolean notExistsRightSide,
											 boolean rightFromSSQ,
											 double optimizerEstimatedRowCount,
											 double optimizerEstimatedCost,
											 String userSuppliedOptimizerOverrides,
											 String explainPlan,
											 String sparkExpressionTreeAsString)
			throws StandardException;

	NoPutResultSet getHalfMergeSortJoinResultSet(NoPutResultSet leftResultSet,
												 int leftNumCols,
												 NoPutResultSet rightResultSet,
												 int rightNumCols,
												 int leftHashKeyItem,
												 int rightHashKeyItem,
												 GeneratedMethod joinClause,
												 int resultSetNumber,
												 boolean oneRowRightSide,
												 boolean notExistsRightSide,
												 boolean rightFromSSQ,
												 double optimizerEstimatedRowCount,
												 double optimizerEstimatedCost,
												 String userSuppliedOptimizerOverrides,
												 String explainPlan,
												 String sparkExpressionTreeAsString)
			throws StandardException;

    NoPutResultSet getMergeJoinResultSet(NoPutResultSet leftResultSet,
										 int leftNumCols,
										 NoPutResultSet rightResultSet,
										 int rightNumCols,
										 int leftHashKeyItem,
										 int rightHashKeyItem,
										 int rightHashKeyToBaseTableMapItem,
										 int rightHashKeySortOrderItem,
										 GeneratedMethod joinClause,
										 int resultSetNumber,
										 boolean oneRowRightSide,
										 boolean notExistsRightSide,
										 boolean rightFromSSQ,
										 double optimizerEstimatedRowCount,
										 double optimizerEstimatedCost,
										 String userSuppliedOptimizerOverrides,
										 String explainPlan,
										 String sparkExpressionTreeAsString)
					   throws StandardException;

    NoPutResultSet getBroadcastJoinResultSet(NoPutResultSet leftResultSet,
											 int leftNumCols,
											 NoPutResultSet rightResultSet,
											 int rightNumCols,
											 int leftHashKeyItem,
											 int rightHashKeyItem,
											 GeneratedMethod joinClause,
											 int resultSetNumber,
											 boolean oneRowRightSide,
											 boolean notExistsRightSide,
											 boolean rightFromSSQ,
											 double optimizerEstimatedRowCount,
											 double optimizerEstimatedCost,
											 String userSuppliedOptimizerOverrides,
											 String explainPlan,
					                                                 String sparkExpressionTreeAsString)
			           throws StandardException;

	/**
		A nested loop join result set forms a result set on top of
		2 other result sets.
		The rows can be constructed as they are requested from the
		result set.
		<p>
		This form of the nested loop join operation is simple, and is
		to be used when there are no join predicates to be passed down
		to the join to limit its scope on the right ResultSet.

		@param leftResultSet	Outer ResultSet for join.
		@param leftNumCols		Number of columns in the leftResultSet
		@param rightResultSet	Inner ResultSet for join.
		@param rightNumCols		Number of columns in the rightResultSet
		@param joinClause a reference to a method in the activation
			that is applied to the activation's "current row" field
			to determine whether the joinClause is satisfied or not.
			The signature of this method is
			<verbatim>
				Boolean joinClause() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param emptyRowFun a reference to a method in the activation
							that is called if the right child returns no rows
		@param wasRightOuterJoin	Whether or not this was originally a right outer join
		@param oneRowRightSide	boolean, whether or not the right side returns
								a single row.  (No need to do 2nd next() if it does.)
		@param notExistsRightSide	boolean, whether or not the right side resides a
									NOT EXISTS base table
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@param userSuppliedOptimizerOverrides		Overrides specified by the user on the sql
		@return the nested loop join operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getNestedLoopLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
													   int leftNumCols,
													   NoPutResultSet rightResultSet,
													   int rightNumCols,
													   GeneratedMethod joinClause,
													   int resultSetNumber,
													   GeneratedMethod emptyRowFun,
													   boolean wasRightOuterJoin,
													   boolean oneRowRightSide,
													   boolean notExistsRightSide,
													   boolean rightFromSSQ,
													   double optimizerEstimatedRowCount,
													   double optimizerEstimatedCost,
													   String userSuppliedOptimizerOverrides,
													   String explainPlan,
													   String sparkExpressionTreeAsString)
			throws StandardException;

	/**
		A left outer join using a hash join.

		@param leftResultSet	Outer ResultSet for join.
		@param leftNumCols		Number of columns in the leftResultSet
		@param rightResultSet	Inner ResultSet for join.
		@param rightNumCols		Number of columns in the rightResultSet
		@param joinClause a reference to a method in the activation
			that is applied to the activation's "current row" field
			to determine whether the joinClause is satisfied or not.
			The signature of this method is
			<verbatim>
				Boolean joinClause() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param emptyRowFun a reference to a method in the activation
							that is called if the right child returns no rows
		@param wasRightOuterJoin	Whether or not this was originally a right outer join
		@param oneRowRightSide	boolean, whether or not the right side returns
								a single row.  (No need to do 2nd next() if it does.)
		@param notExistsRightSide	boolean, whether or not the right side resides a
									NOT EXISTS base table
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@param userSuppliedOptimizerOverrides		Overrides specified by the user on the sql
		@return the nested loop join operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getHashLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
												 int leftNumCols,
												 NoPutResultSet rightResultSet,
												 int rightNumCols,
												 int leftHashKeyItem,
												 int rightHashKeyItem,
												 GeneratedMethod joinClause,
												 int resultSetNumber,
												 GeneratedMethod emptyRowFun,
												 boolean wasRightOuterJoin,
												 boolean oneRowRightSide,
												 boolean notExistsRightSide,
												 boolean rightFromSSQ,
												 double optimizerEstimatedRowCount,
												 double optimizerEstimatedCost,
												 String userSuppliedOptimizerOverrides,
												 String explainPlan,
												 String sparkExpressionTreeAsString)
			throws StandardException;

	/**
		A ResultSet which materializes the underlying ResultSet tree into a 
		temp table on the 1st open.  All subsequent "scans" of this ResultSet
		will return results from the temp table.

		@param source the result set input to this result set.
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@return the materialization operation as a result set.

	 	@exception StandardException		Thrown on failure
	 */
	NoPutResultSet getMaterializedResultSet(NoPutResultSet source, 
											int resultSetNumber,
											double optimizerEstimatedRowCount,
											double optimizerEstimatedCost) 
		throws StandardException;

	/**
		A ResultSet which provides the insensitive scrolling functionality
		for the underlying result set by materializing the underlying ResultSet 
		tree into a hash table while scrolling forward.

		@param source the result set input to this result set.
		@param activation the activation for this result set,
			which provides the context for normalization.
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param sourceRowWidth	The # of columns in the source row.
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@return the materialization operation as a result set.

	 	@exception StandardException		Thrown on failure
	 */
	NoPutResultSet getScrollInsensitiveResultSet(NoPutResultSet source,
	                                        Activation activation, 
											int resultSetNumber,
											int sourceRowWidth,
											boolean scrollable,
											double optimizerEstimatedRowCount,
											double optimizerEstimatedCost,
											String explainPlan) 
		throws StandardException;
	/**
	 A left outer join using a sort merge join.

	@param leftResultSet	Outer ResultSet for join.
	@param leftNumCols		Number of columns in the leftResultSet
	@param rightResultSet	Inner ResultSet for join.
	@param rightNumCols		Number of columns in the rightResultSet
	@param joinClause a reference to a method in the activation
		that is applied to the activation's "current row" field
		to determine whether the joinClause is staisfied or not.
		The signature of this method is
		<verbatim>
			Boolean joinClause() throws StandardException;
		</verbatim>
	@param resultSetNumber	The resultSetNumber for the ResultSet
	@param emptyRowFun a reference to a method in the activation
		that is called if the right child returns no rows
	@param wasRightOuterJoin	Whether or not this was originally a right outer join
	@param oneRowRightSide	boolean, whether or not the right side returns
		a single row. (No need to do 2nd next() if it does.)
	@param notExistsRightSide	boolean, whether or not the right side resides a
		NOT EXISTS base table
	@param optimizerEstimatedRowCount	Estimated total # of rows by
		optimizer
	@param optimizerEstimatedCost	Estimated total cost by optimizer
	@param userSuppliedOptimizerOverrides	Overrides specified by the user on the sql
	@return the nested loop join operation as a result set.
	@exception StandardException thrown when unable to create the 
		result set
	*/
	NoPutResultSet getMergeSortLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
													  int leftNumCols,
													  NoPutResultSet rightResultSet,
													  int rightNumCols,
													  int leftHashKeyItem,
													  int rightHashKeyItem,
													  GeneratedMethod joinClause,
													  int resultSetNUmber,
													  GeneratedMethod emptyRowFun,
													  boolean wasRightOuterJoin,
													  boolean oneRowRightSide,
													  boolean noExistsRightSide,
													  boolean rightFromSSQ,
													  double optimizerEstimatedRowCount,
													  double optimizerEstimatedCost,
													  String userSuppliedOptimizerOverrides,
													  String explainPlan,
													  String sparkExpressionTreeAsString)
			throws StandardException;

	NoPutResultSet getHalfMergeSortLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
														  int leftNumCols,
														  NoPutResultSet rightResultSet,
														  int rightNumCols,
														  int leftHashKeyItem,
														  int rightHashKeyItem,
														  GeneratedMethod joinClause,
														  int resultSetNUmber,
														  GeneratedMethod emptyRowFun,
														  boolean wasRightOuterJoin,
														  boolean oneRowRightSide,
														  boolean noExistsRightSide,
														  boolean rightFromSSQ,
														  double optimizerEstimatedRowCount,
														  double optimizerEstimatedCost,
														  String userSuppliedOptimizerOverrides,
														  String explainPlan,
														  String sparkExpressionTreeAsString)
			throws StandardException;

	NoPutResultSet getMergeLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
												  int leftNumCols,
												  NoPutResultSet rightResultSet,
												  int rightNumCols,
												  int leftHashKeyItem,
												  int rightHashKeyItem,
												  int rightHashKeyToBaseTableMapItem,
												  int rightHashKeySortOrderItem,
												  GeneratedMethod joinClause,
												  int resultSetNUmber,
												  GeneratedMethod emptyRowFun,
												  boolean wasRightOuterJoin,
												  boolean oneRowRightSide,
												  boolean noExistsRightSide,
												  boolean rightFromSSQ,
												  double optimizerEstimatedRowCount,
												  double optimizerEstimatedCost,
												  String userSuppliedOptimizerOverrides,
												  String explainPlan,
												  String sparkExpressionTreeAsString)
	throws StandardException;

	NoPutResultSet getBroadcastLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
													  int leftNumCols,
													  NoPutResultSet rightResultSet,
													  int rightNumCols,
													  int leftHashKeyItem,
													  int rightHashKeyItem,
													  GeneratedMethod joinClause,
													  int resultSetNUmber,
													  GeneratedMethod emptyRowFun,
													  boolean wasRightOuterJoin,
													  boolean oneRowRightSide,
													  boolean noExistsRightSide,
													  boolean rightFromSSQ,
													  double optimizerEstimatedRowCount,
													  double optimizerEstimatedCost,
													  String userSuppliedOptimizerOverrides,
													  String explainPlan,
													  String sparkExpressionTreeAsString)
	throws StandardException;

	/**
          Support old versions of getXXXJoinResultSet, so SHOW SCHEMAS and other statements
	  which store these serialized objects on disk won't break upon upgrade.
	 */

	NoPutResultSet getNestedLoopJoinResultSet(NoPutResultSet leftResultSet,
											  int leftNumCols,
											  NoPutResultSet rightResultSet,
											  int rightNumCols,
											  GeneratedMethod joinClause,
											  int resultSetNumber,
											  boolean oneRowRightSide,
											  boolean notExistsRightSide,
											  boolean rightFromSSQ,
											  double optimizerEstimatedRowCount,
											  double optimizerEstimatedCost,
											  String userSuppliedOptimizerOverrides,
											  String explainPlan)
			throws StandardException;

	NoPutResultSet getCrossJoinResultSet(NoPutResultSet leftResultSet,
											  int leftNumCols,
											  NoPutResultSet rightResultSet,
											  int rightNumCols,
                                              int leftHashKeyItem,
                                              int rightHashKeyItem,
											  GeneratedMethod joinClause,
											  int resultSetNumber,
											  boolean oneRowRightSide,
											  boolean notExistsRightSide,
											  boolean rightFromSSQ,
											  double optimizerEstimatedRowCount,
											  double optimizerEstimatedCost,
											  String userSuppliedOptimizerOverrides,
											  String explainPlan)
			throws StandardException;

	NoPutResultSet getMergeSortJoinResultSet(NoPutResultSet leftResultSet,
											 int leftNumCols,
											 NoPutResultSet rightResultSet,
											 int rightNumCols,
											 int leftHashKeyItem,
											 int rightHashKeyItem,
											 GeneratedMethod joinClause,
											 int resultSetNumber,
											 boolean oneRowRightSide,
											 boolean notExistsRightSide,
											 boolean rightFromSSQ,
											 double optimizerEstimatedRowCount,
											 double optimizerEstimatedCost,
											 String userSuppliedOptimizerOverrides,
											 String explainPlan)
			throws StandardException;


    NoPutResultSet getMergeJoinResultSet(NoPutResultSet leftResultSet,
										 int leftNumCols,
										 NoPutResultSet rightResultSet,
										 int rightNumCols,
										 int leftHashKeyItem,
										 int rightHashKeyItem,
										 int rightHashKeyToBaseTableMapItem,
										 int rightHashKeySortOrderItem,
										 GeneratedMethod joinClause,
										 int resultSetNumber,
										 boolean oneRowRightSide,
										 boolean notExistsRightSide,
										 boolean rightFromSSQ,
										 double optimizerEstimatedRowCount,
										 double optimizerEstimatedCost,
										 String userSuppliedOptimizerOverrides,
										 String explainPlan)
					   throws StandardException;

    NoPutResultSet getBroadcastJoinResultSet(NoPutResultSet leftResultSet,
											 int leftNumCols,
											 NoPutResultSet rightResultSet,
											 int rightNumCols,
											 int leftHashKeyItem,
											 int rightHashKeyItem,
											 GeneratedMethod joinClause,
											 int resultSetNumber,
											 boolean oneRowRightSide,
											 boolean notExistsRightSide,
											 boolean rightFromSSQ,
											 double optimizerEstimatedRowCount,
											 double optimizerEstimatedCost,
											 String userSuppliedOptimizerOverrides,
											 String explainPlan)
			           throws StandardException;


	NoPutResultSet getNestedLoopLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
													   int leftNumCols,
													   NoPutResultSet rightResultSet,
													   int rightNumCols,
													   GeneratedMethod joinClause,
													   int resultSetNumber,
													   GeneratedMethod emptyRowFun,
													   boolean wasRightOuterJoin,
													   boolean oneRowRightSide,
													   boolean notExistsRightSide,
													   boolean rightFromSSQ,
													   double optimizerEstimatedRowCount,
													   double optimizerEstimatedCost,
													   String userSuppliedOptimizerOverrides,
													   String explainPlan)
			throws StandardException;

	NoPutResultSet getMergeSortLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
													  int leftNumCols,
													  NoPutResultSet rightResultSet,
													  int rightNumCols,
													  int leftHashKeyItem,
													  int rightHashKeyItem,
													  GeneratedMethod joinClause,
													  int resultSetNUmber,
													  GeneratedMethod emptyRowFun,
													  boolean wasRightOuterJoin,
													  boolean oneRowRightSide,
													  boolean noExistsRightSide,
													  boolean rightFromSSQ,
													  double optimizerEstimatedRowCount,
													  double optimizerEstimatedCost,
													  String userSuppliedOptimizerOverrides,
													  String explainPlan)
			throws StandardException;

	NoPutResultSet getMergeLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
												  int leftNumCols,
												  NoPutResultSet rightResultSet,
												  int rightNumCols,
												  int leftHashKeyItem,
												  int rightHashKeyItem,
												  int rightHashKeyToBaseTableMapItem,
												  int rightHashKeySortOrderItem,
												  GeneratedMethod joinClause,
												  int resultSetNUmber,
												  GeneratedMethod emptyRowFun,
												  boolean wasRightOuterJoin,
												  boolean oneRowRightSide,
												  boolean noExistsRightSide,
												  boolean rightFromSSQ,
												  double optimizerEstimatedRowCount,
												  double optimizerEstimatedCost,
												  String userSuppliedOptimizerOverrides,
												  String explainPlan)
			throws StandardException;

	NoPutResultSet getBroadcastLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
													  int leftNumCols,
													  NoPutResultSet rightResultSet,
													  int rightNumCols,
													  int leftHashKeyItem,
													  int rightHashKeyItem,
													  GeneratedMethod joinClause,
													  int resultSetNUmber,
													  GeneratedMethod emptyRowFun,
													  boolean wasRightOuterJoin,
													  boolean oneRowRightSide,
													  boolean noExistsRightSide,
													  boolean rightFromSSQ,
													  double optimizerEstimatedRowCount,
													  double optimizerEstimatedCost,
													  String userSuppliedOptimizerOverrides,
													  String explainPlan)
	throws StandardException;

	NoPutResultSet getMergeSortFullOuterJoinResultSet(
			NoPutResultSet leftResultSet, int leftNumCols,
			NoPutResultSet rightResultSet, int rightNumCols,
			int leftHashKeyItem, int rightHashKeyItem,
			GeneratedMethod joinClause, int resultSetNumber,
			GeneratedMethod leftEmptyRowFun, GeneratedMethod rightEmptyRowFun,
			boolean wasRightOuterJoin,
			boolean oneRowRightSide, boolean notExistsRightSide,
			boolean rightFromSSQ,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides,
			String explainPlan) throws StandardException;

	NoPutResultSet getMergeSortFullOuterJoinResultSet(
			NoPutResultSet leftResultSet, int leftNumCols,
			NoPutResultSet rightResultSet, int rightNumCols,
			int leftHashKeyItem, int rightHashKeyItem,
			GeneratedMethod joinClause, int resultSetNumber,
			GeneratedMethod leftEmptyRowFun, GeneratedMethod rightEmptyRowFun,
			boolean wasRightOuterJoin,
			boolean oneRowRightSide, boolean notExistsRightSide,
			boolean rightFromSSQ,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides,
			String explainPlan,
			String sparkExpressionTreeAsString) throws StandardException;

	NoPutResultSet getBroadcastFullOuterJoinResultSet(NoPutResultSet leftResultSet,
													  int leftNumCols,
													  NoPutResultSet rightResultSet,
													  int rightNumCols,
													  int leftHashKeyItem,
													  int rightHashKeyItem,
													  GeneratedMethod joinClause,
													  int resultSetNUmber,
													  GeneratedMethod leftEmptyRowFun,
													  GeneratedMethod rightEmptyRowFun,
													  boolean wasRightOuterJoin,
													  boolean oneRowRightSide,
													  boolean noExistsRightSide,
													  boolean rightFromSSQ,
													  double optimizerEstimatedRowCount,
													  double optimizerEstimatedCost,
													  String userSuppliedOptimizerOverrides,
													  String explainPlan,
													  String sparkExpressionTreeAsString)
			throws StandardException;

	NoPutResultSet getBroadcastFullOuterJoinResultSet(NoPutResultSet leftResultSet,
													  int leftNumCols,
													  NoPutResultSet rightResultSet,
													  int rightNumCols,
													  int leftHashKeyItem,
													  int rightHashKeyItem,
													  GeneratedMethod joinClause,
													  int resultSetNUmber,
													  GeneratedMethod leftEmptyRowFun,
													  GeneratedMethod rightEmptyRowFun,
													  boolean wasRightOuterJoin,
													  boolean oneRowRightSide,
													  boolean noExistsRightSide,
													  boolean rightFromSSQ,
													  double optimizerEstimatedRowCount,
													  double optimizerEstimatedCost,
													  String userSuppliedOptimizerOverrides,
													  String explainPlan)
			throws StandardException;
	/**
		REMIND: needs more description...

		@param source the result set input to this result set.
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param erdNumber	int for ResultDescription 
							(so it can be turned back into an object)
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@return the normalization operation as a result set.

	 	@exception StandardException		Thrown on failure
	 */
	NoPutResultSet getNormalizeResultSet(NoPutResultSet source, 
										 int resultSetNumber,
										 int erdNumber,
										 double optimizerEstimatedRowCount,
										 double optimizerEstimatedCost,
										 boolean forUpdate,
										 String explainPlan) 
		throws StandardException;

	/**
		A current of result set forms a result set on the
		current row of an open cursor.
		It is used to perform positioned operations such as
		positioned update and delete, using the result set paradigm.

		@param cursorName the name of the cursor providing the row.
		@param resultSetNumber	The resultSetNumber for the ResultSet
	 */
	NoPutResultSet getCurrentOfResultSet(String cursorName, Activation activation, 
									int resultSetNumber);

	/**
	 * The Union interface is used to evaluate the union (all) of two ResultSets.
	 * (Any duplicate elimination is performed above this ResultSet.)
	 *
	 * Forms a ResultSet returning the union of the rows in two source
	 * ResultSets.  The column types in source1 and source2 are assumed to be
	 * the same.
	 *
	 * @param source1	The first ResultSet whose rows go into the union
	 * @param source2	The second ResultSet whose rows go into the
	 *			union
	 *	@param resultSetNumber	The resultSetNumber for the ResultSet
	 *	@param optimizerEstimatedRowCount	Estimated total # of rows by
	 *										optimizer
	 *	@param optimizerEstimatedCost		Estimated total cost by optimizer
	 *
	 * @return	A ResultSet from which the caller can get the union
	 *		of the two source ResultSets.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	NoPutResultSet	getUnionResultSet(NoPutResultSet source1,
					NoPutResultSet source2,
					int resultSetNumber,
					double optimizerEstimatedRowCount,
					double optimizerEstimatedCost,
					String explainPlan)
					throws StandardException;


    /**
     * The SetOpResultSet is used to implement an INTERSECT or EXCEPT operation.
     * It selects rows from two ordered input result sets.
     *
     * @param leftSource The result set that implements the left input
     * @param rightSource The result set that implements the right input
     * @param activation the activation for this result set
     * @param resultSetNumber
     * @param optimizerEstimatedRowCount
     * @param optimizerEstimatedCost
     * @param opType IntersectOrExceptNode.INTERSECT_OP or EXCEPT_OP
     * @param all true if the operation is an INTERSECT ALL or an EXCEPT ALL,
     *            false if the operation is an INTERSECT DISCTINCT or an EXCEPT DISCTINCT
     * @param intermediateOrderByColumnsSavedObject The saved object index for the array of order by columns for the
     *        ordering of the left and right sources. That is, both the left and right sources have an order by
     *        clause of the form ORDER BY intermediateOrderByColumns[0],intermediateOrderByColumns[1],...
     * @param intermediateOrderByDirectionSavedObject The saved object index for the array of source
     *        order by directions. That is, the ordering of the i'th order by column in the input is ascending
     *        if intermediateOrderByDirection[i] is 1, descending if intermediateOrderByDirection[i] is -1.
	 *
	 * @return	A ResultSet from which the caller can get the INTERSECT or EXCEPT
	 *
	 * @exception StandardException		Thrown on failure
	 */
    NoPutResultSet getSetOpResultSet( NoPutResultSet leftSource,
                                      NoPutResultSet rightSource,
                                      Activation activation, 
                                      int resultSetNumber,
                                      long optimizerEstimatedRowCount,
                                      double optimizerEstimatedCost,
                                      int opType,
                                      boolean all,
                                      int intermediateOrderByColumnsSavedObject,
                                      int intermediateOrderByDirectionSavedObject,
                                      int intermediateOrderByNullsLowSavedObject)
        throws StandardException;
                                                     
                                                     
	//
	// Misc operations
	//



	/**
	 * A last index key result set returns the last row from
	 * the index in question.  It is used as an ajunct to max().
	 *
	 * @param activation 		the activation for this result set,
	 *		which provides the context for the row allocation operation.
	 * @param resultSetNumber	The resultSetNumber for the ResultSet
	 * @param resultRowAllocator a reference to a method in the activation
	 * 						that creates a holder for the result row of the scan.  May
	 *						be a partial row.  <verbatim>
	 *		ExecRow rowAllocator() throws StandardException; </verbatim>
	 * @param conglomId 		the conglomerate of the table to be scanned.
	 * @param tableName			The full name of the table
	 * @param userSuppliedOptimizerOverrides		Overrides specified by the user on the sql
	 * @param indexName			The name of the index, if one used to access table.
	 * @param colRefItem		An saved item for a bitSet of columns that
	 *							are referenced in the underlying table.  -1 if
	 *							no item.
	 * @param lockMode			The lock granularity to use (see
	 *							TransactionController in access)
	 * @param tableLocked		Whether or not the table is marked as using table locking
	 *							(in sys.systables)
	 * @param isolationLevel	Isolation level (specified or not) to use on scans
	 * @param optimizerEstimatedRowCount	Estimated total # of rows by
	 * 										optimizer
	 * @param optimizerEstimatedCost		Estimated total cost by optimizer
	 *
	 * @return the scan operation as a result set.
 	 *
	 * @exception StandardException thrown when unable to create the
	 * 				result set
	 */
	NoPutResultSet getLastIndexKeyResultSet
	(
		Activation 			activation,
		int 				resultSetNumber,
		GeneratedMethod 	resultRowAllocator,
		long 				conglomId,
		String 				tableName,
		String 				userSuppliedOptimizerOverrides,
		String 				indexName,
		int 				colRefItem,
		int 				lockMode,
		boolean				tableLocked,
		int					isolationLevel,
		double				optimizerEstimatedRowCount,
		double 				optimizerEstimatedCost,
        String              tableVersion,
		String              explainPlan
	) throws StandardException;


	/**
		A Dependent table scan result set forms a result set on a scan
		of a dependent table for the rows that got materilized 
		on the scan of its parent table and if the row being deleted
		on parent table has a reference in the dependent table.

		@param activation the activation for this result set,
			which provides the context for the row allocation operation.
		@param conglomId the conglomerate of the table to be scanned.
		@param scociItem The saved item for the static conglomerate info.
		@param resultRowAllocator a reference to a method in the activation
			that creates a holder for the result row of the scan.  May
			be a partial row.
			<verbatim>
				ExecRow rowAllocator() throws StandardException;
			</verbatim>
		@param resultSetNumber	The resultSetNumber for the ResultSet
		@param startKeyGetter a reference to a method in the activation
			that gets the start key indexable row for the scan.  Null
			means there is no start key.
			<verbatim>
				ExecIndexRow startKeyGetter() throws StandardException;
			</verbatim>
		@param startSearchOperator The start search operator for opening
			the scan
		@param stopKeyGetter	a reference to a method in the activation
			that gets the stop key indexable row for the scan.  Null means
			there is no stop key.
			<verbatim>
				ExecIndexRow stopKeyGetter() throws StandardException;
			</verbatim>
		@param stopSearchOperator	The stop search operator for opening
			the scan
		@param sameStartStopPosition	Re-use the startKeyGetter for the stopKeyGetter
										(Exact match search.)
		@param qualifiers the array of Qualifiers for the scan.
			Null or an array length of zero means there are no qualifiers.
		@param tableName		The full name of the table
		@param userSuppliedOptimizerOverrides		Overrides specified by the user on the sql
		@param indexName		The name of the index, if one used to access table.
		@param isConstraint		If index, if used, is a backing index for a constraint.
		@param forUpdate		True means open for update
		@param colRefItem		An saved item for a bitSet of columns that
								are referenced in the underlying table.  -1 if
								no item.
		@param lockMode			The lock granularity to use (see
								TransactionController in access)
		@param tableLocked		Whether or not the table is marked as using table locking
								(in sys.systables)
		@param isolationLevel	Isolation level (specified or not) to use on scans
		@param oneRowScan		Whether or not this is a 1 row scan.
		@param optimizerEstimatedRowCount	Estimated total # of rows by
											optimizer
		@param optimizerEstimatedCost		Estimated total cost by optimizer
		@param parentResultSetId  Id to access the materlized temporary result
                            	  set from the refence stored in the activation.
		@param fkIndexConglomId foreign key index conglomerate id.
		@param fkColArrayItem  saved column array object  that matches the foreign key index
		                       columns  and the resultset from the parent table.
		@param  rltItem row location template

		@return the table scan operation as a result set.
		@exception StandardException thrown when unable to create the
			result set
	 */
	NoPutResultSet getRaDependentTableScanResultSet(
			Activation activation,
			long conglomId,
			int scociItem,
			GeneratedMethod resultRowAllocator,
			int resultSetNumber,
			GeneratedMethod startKeyGetter,
			int startSearchOperator,
			GeneratedMethod stopKeyGetter,
			int stopSearchOperator,
			boolean sameStartStopPosition,
			boolean rowIdKey,
			String qualifiersField,
			String tableName,
			String userSuppliedOptimizerOverrides,
			String indexName,
			boolean isConstraint,
			boolean forUpdate,
			int colRefItem,
			int indexColItem,
			int lockMode,
			boolean tableLocked,
			int isolationLevel,
			boolean oneRowScan,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			String parentResultSetId,
			long fkIndexConglomId,
			int fkColArrayItem,
			int rltItem)
		throws StandardException;

    /**
	 * This result sets implements the filtering needed by <result offset
	 * clause> and <fetch first clause>. It is only ever generated if at least
	 * one of the two clauses is present.
	 *
	 * @param source          The source result set being filtered
	 * @param activation      The activation for this result set,
	 *		                  which provides the context for the row
	 *                        allocation operation
	 * @param resultSetNumber The resultSetNumber for the ResultSet
	 * @param offsetMethod   The OFFSET parameter was specified
	 * @param fetchFirstMethod The FETCH FIRST/NEXT parameter was specified
	 * @param hasJDBClimitClause True if the offset/fetchFirst clauses were added by JDBC LIMExeIT escape syntax
	 * @param optimizerEstimatedRowCount
	 *                        Estimated total # of rows by optimizer
	 * @param optimizerEstimatedCost
	 *                        Estimated total cost by optimizer
	 * @exception StandardException Standard error policy
	 */

	NoPutResultSet getRowCountResultSet(
			NoPutResultSet source,
			Activation activation,
			int resultSetNumber,
			GeneratedMethod offsetMethod,
			GeneratedMethod fetchFirstMethod,
			boolean hasJDBClimitClause,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			String explainPlan) throws StandardException;

    NoPutResultSet getExplainResultSet(ResultSet source, Activation activation, int resultSetNumber) throws StandardException;

    NoPutResultSet getExplainResultSet(NoPutResultSet source, Activation activation, int resultSetNumber) throws StandardException;

    /**
     * Export
     */
	NoPutResultSet getExportResultSet(NoPutResultSet source,
									  Activation activation,
									  int resultSetNumber,
									  String exportPath,
									  String compression,
									  int replicationCount,
									  String encoding,
									  String fieldSeparator,
									  String quoteChar,
									  int srcResultDescriptionSavedObjectNum) throws StandardException;


	/**
	 * Binary Export
	 */
	NoPutResultSet getBinaryExportResultSet(NoPutResultSet source,
									  Activation activation,
									  int resultSetNumber,
									  String exportPath,
									  String compression,
									  String format,
									  int srcResultDescriptionSavedObjectNum) throws StandardException;
    /**
     * Batch Once
     */
	NoPutResultSet getBatchOnceResultSet(NoPutResultSet source,
										 Activation activation,
										 int resultSetNumber,
										 NoPutResultSet subqueryResultSet,
										 String updateResultSetFieldName,
										 int sourceCorrelatedColumnItem,
										 int subqueryCorrelatedColumnItem) throws StandardException;

	/**
	 * Recursive query
	 */
	NoPutResultSet getSelfReferenceResultSet(Activation activation,
											 GeneratedMethod rowAllocator,
											 int resultSetNumber,
											 double optimizerEstimatedRowCount,
											 double optimizerEstimatedCost,
											 String explainPlan) throws StandardException;

	NoPutResultSet getRecursiveUnionResultSet(NoPutResultSet leftResultSet,
											  NoPutResultSet rightResultSet,
											  int resultSetNumber,
											  double optimizerEstimatedRowCount,
											  double optimizerEstimatedCost,
											  String explainPlan,
											  int iterationLimit) throws StandardException;

	NoPutResultSet getSignalResultSet(Activation activation,
	                                  String sqlState,
	                                  GeneratedMethod errorTextGenerator) throws StandardException;

        NoPutResultSet getSetResultSet(Activation activation,
                                       String getColumnDVDsMethodNames,
                                       GeneratedMethod getNewDVDsMethod) throws StandardException;

}
